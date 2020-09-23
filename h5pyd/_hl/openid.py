import os
import sys
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime

# Azure
import adal
from adal.adal_error import AdalError

# Google
try:
    from google_auth_oauthlib.flow import InstalledAppFlow as GoogleInstalledAppFlow
    from google.auth.transport.requests import Request as GoogleRequest
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from google.oauth2 import id_token as GoogleIDToken
except ModuleNotFoundError:
    print("Unable to import google auth packages")

from .config import Config

class OpenIDHandler(ABC):

    def __init__(self, endpoint, use_token_cache=True):
        """Initialize the token."""

        # Location of the token cache.
        self._token_cache_file = os.path.expanduser('~/.hstokencfg')
        self._endpoint = endpoint

        # The _token attribute should be a dict with at least the following keys:
        #
        # accessToken - The OpenID token to send.
        # refreshToken - The refresh token (optional).
        # expiresOn - The unix timestamp when the token expires (optional).

        if not use_token_cache or not os.path.isfile(self._token_cache_file):
            self._token = None

        else:
            with open(self._token_cache_file, 'r') as token_file:
                self._token = json.load(token_file).get(endpoint, None)

    @abstractmethod
    def acquire(self):
        """Acquire a new token from the provider."""
        pass

    @abstractmethod
    def refresh(self):
        """Refresh an existing token with the provider."""
        pass

    @property
    def expired(self):
        """Return if the token is expired."""
        t = self._token
        return t is not None and 'expiresOn' in t and time.time() >= t['expiresOn']

    @property
    def token(self):
        """Return the token if valid, otherwise get a new one."""

        if self.expired:
            self.refresh()
            self.write_token_cache()

        if self._token is None:
            self.acquire()
            self.write_token_cache()

        return self._token['accessToken']

    def write_token_cache(self):
        """Write the token to a file cache."""

        cache_exists = os.path.isfile(self._token_cache_file)

        # Create a new cache file.
        if not cache_exists and self._token is not None:
            with open(self._token_cache_file, 'w') as token_file:
                json.dump({self._endpoint: self._token}, token_file)

        # Update an exisiting cache file.
        elif cache_exists:
            with open(self._token_cache_file, 'r+') as token_file:
                cache = json.loads(token_file.read())

                # Store valid tokens.
                if self._token is not None:
                    cache[self._endpoint] = self._token

                # Delete invalid tokens.
                elif self._endpoint in cache:
                    del cache[self._endpoint]

                token_file.seek(0)
                token_file.truncate(0)
                json.dump(cache, token_file)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class AzureOpenID(OpenIDHandler):

    AUTHORITY_URI = 'https://login.microsoftonline.com'  # login endpoint for AD auth

    def __init__(self, endpoint, config=None):
        """Store configuration."""

        # Configuration manager
        hs_config = Config()

        # Config is a dictionary.
        if isinstance(config, dict):
            self.config = config

        # Maybe client_secrets are in environment variables?
        else:

            self.config = {
                'AD_APP_ID': hs_config.get("hs_ad_app_id", None),
                'AD_TENANT_ID': hs_config.get("hs_ad_tenant_id", None),
                'AD_RESOURCE_ID': hs_config.get("hs_ad_resource_id", None),
                'AD_CLIENT_SECRET': hs_config.get("hs_ad_client_secret", None)
            }

        if 'AD_CLIENT_SECRET' in self.config and self.config['AD_CLIENT_SECRET']:
            use_token_cache = False
        else:
            use_token_cache = True

        super().__init__(endpoint, use_token_cache=use_token_cache)

    def write_token_cache(self):
        if 'AD_CLIENT_SECRET' in self.config and self.config['AD_CLIENT_SECRET']:
            pass # don't use token cache for unattended authentication
        else:
            super().write_token_cache()

    def acquire(self):
        """Acquire a new Azure token."""

        app_id = self.config["AD_APP_ID"]
        resource_id = self.config["AD_RESOURCE_ID"]
        tenant_id = self.config["AD_TENANT_ID"]
        client_secret = self.config.get("AD_CLIENT_SECRET", None)
        authority_uri = self.AUTHORITY_URI + '/' + tenant_id

        # Try to get a token using different oauth flows.
        context = adal.AuthenticationContext(authority_uri, api_version=None)

        try:
            if client_secret is not None:
                code = context.acquire_token_with_client_credentials(resource_id, app_id, client_secret)
            else:
                code = context.acquire_user_code(resource_id, app_id)

        except AdalError:
            eprint("unable to process AD token")
            self._token = None
            self.write_token_cache()
            raise

        if "message" in code:
            eprint(code["message"])
            mgmt_token = context.acquire_token_with_device_code(resource_id, code, app_id)

        elif "accessToken" in code:
            mgmt_token = code

        else:
            eprint("Could not authenticate with AD")

        # Only store some fields.
        self._token = {
            'accessToken': mgmt_token['accessToken'],
            'refreshToken': mgmt_token.get('refreshToken', None),
            'tenantId': mgmt_token.get('tenantId', tenant_id),
            'clientId': mgmt_token.get('_clientId', app_id),
            'resource': mgmt_token.get('resource', resource_id)
        }

        # Parse time to timestamp.
        if 'expiresOn' in mgmt_token:
            expire_dt = datetime.strptime(mgmt_token['expiresOn'], '%Y-%m-%d %H:%M:%S.%f')
            self._token['expiresOn'] = expire_dt.timestamp()

    def refresh(self):
        """Try to renew an Azure token."""

        try:

            # This will work for device code flow, but not with client
            # credentials. If we have the secret, we can just request a new
            # token anyways.

            authority_uri = self.AUTHORITY_URI + '/' + self._token['tenantId']
            context = adal.AuthenticationContext(authority_uri, api_version=None)
            mgmt_token = context.acquire_token_with_refresh_token(self._token['refreshToken'],
                                                                  self._token['clientId'],
                                                                  self._token['resource'],
                                                                  None)

            # New token does not have all the metadata.
            self._token['accessToken'] = mgmt_token['accessToken']
            self._token['refreshToken'] = mgmt_token['refreshToken']

            # Parse time to timestamp.
            if 'expiresOn' in mgmt_token:
                expire_dt = datetime.strptime(mgmt_token['expiresOn'], '%Y-%m-%d %H:%M:%S.%f')
                self._token['expiresOn'] = expire_dt.timestamp()

        except:
            self._token = None


class GoogleOpenID(OpenIDHandler):

    def __init__(self, endpoint, config=None, scopes=None):
        """Store configuration."""

        # Configuration manager
        hs_config = Config()

        if scopes is None:
            scopes = hs_config.get('hs_google_scopes', 'openid').split()
        self.scopes = scopes

        # Config is a client_secrets dictionary.
        if isinstance(config, dict):
            self.config = config

        # Config points to a client_secrets.json file.
        elif isinstance(config, str) and os.path.isfile(config):
            with open(config, 'r') as f:
                self.config = json.loads(f.read())

        # Maybe client_secrets are in environment variables?
        else:
            self.config = {
                'installed': {
                    'project_id': hs_config.get('hs_google_project_id', None),
                    'client_id': hs_config.get('hs_google_client_id', None),
                    'client_secret': hs_config.get('hs_google_client_secret', None),
                    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                    'token_uri': 'https://oauth2.googleapis.com/token',
                    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
                    'redirect_uris': ['urn:ietf:wg:oauth:2.0:oob', 'http://localhost']
                }
            }

        super().__init__(endpoint)

    def _parse(self, creds):
        """Parse credentials."""

        # NOTE: In Google OpenID, if a client is set up for InstalledAppFlow
        # then the client_secret is not actually treated as a secret. Acquire
        # will ALWAYS prompt for user input before granting a token.

        token = {
            'accessToken': creds.id_token,
            'refreshToken': creds.refresh_token,
            'tokenUri': creds.token_uri,
            'clientId': creds.client_id,
            'clientSecret': creds.client_secret,
            'scopes': creds.scopes
        }

        # The expiry field that is in creds is for the OAuth token, not the
        # OpenID token. We need to validate the OpenID tokenn to get the exp.
        idinfo = GoogleIDToken.verify_oauth2_token(creds.id_token, GoogleRequest())
        if 'exp' in idinfo:
            token['expiresOn'] = idinfo['exp']

        return token

    def acquire(self):
        """Acquire a new Google token."""

        flow = GoogleInstalledAppFlow.from_client_config(self.config,
                                                         scopes=self.scopes)
        creds = flow.run_console()
        self._token = self._parse(creds)

    def refresh(self):
        """Try to renew a token."""

        try:

            token = self._token
            creds = GoogleCredentials(token=None,
                                      refresh_token=token['refreshToken'],
                                      scopes=token['scopes'],
                                      token_uri=token['tokenUri'],
                                      client_id=token['clientId'],
                                      client_secret=token['clientSecret'])

            creds.refresh(GoogleRequest())
            self._token = self._parse(creds)

        except:
            self._token = None
