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
from google_auth_oauthlib.flow import InstalledAppFlow as GoogleInstalledAppFlow
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials as GoogleCredentials
from google.oauth2 import id_token as GoogleIDToken

from .config import Config

class OpenIDHandler(ABC):

    def __init__(self, id):
        """Initialize the token."""

        # Location of the token cache.
        self._token_cache_file = os.path.expanduser('~/.hstokencfg')
        self._id = id

        # The _token attribute should be a dict with the following keys:
        #
        # accessToken - The OpenID token to send.
        # refreshToken - The refresh token (optional).
        # expiresOn - The unix timestamp when the token expires (optional).
        self._token = self.read_token_cache()
        if self._token is None:
            self._token = self.acquire()

    @abstractmethod
    def acquire(self):
        pass

    @abstractmethod
    def refresh(self):
        pass

    @property
    def token(self):
        """Return the token if valid, otherwise get a new one."""

        if self._token is not None:
            if 'expiresOn' in self._token and time.time() >= self._token['expiresOn']:
                self._token = self.refresh()

        if self._token is None:
            self._token = self.acquire()

        return self._token['accessToken']


    def read_token_cache(self):
        """Read the cached token from a file."""

        if not os.path.isfile(self._token_cache_file):
            return None

        with open(self._token_cache_file, 'r') as token_file:
            return json.load(token_file).get(self._id, None)


    def write_token_cache(self):
        """Write the token to a file cache."""

        # Create a new cache file.
        if not os.path.isfile(self._token_cache_file) and self._token is not None:
            with open(self._token_cache_file, 'w') as token_file:
                json.dump({self._id: self._token}, token_file)

        # Update an exisiting cache file.
        else:
            with open(self._token_cache_file, 'r+') as token_file:
                cache = json.loads(token_file.read())

                # Store valid tokens.
                if self._token is not None:
                    cache[self._id] = self._token

                # Delete invalid tokens.
                elif self._id in cache:
                    del cache[self._id]

                token_file.seek(0)
                token_file.truncate(0)
                json.dump(cache, token_file)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class AzureOpenID(OpenIDHandler):

    AUTHORITY_URI = 'https://login.microsoftonline.com'  # login endpoint for AD auth
    TOKEN_CACHE_PREFIX = 'hsazcfg'

    def __init__(self, config=None):
        """Store configuration."""

        # Configuration manager
        hs_config = Config()

        # Config is a dictionary.
        if isinstance(config, dict):
            self.config = config

        # Maybe client_secrets are in environment variables?
        else:

            self.config = {
                'AD_APP_ID': hs_config["hs_ad_app_id"],
                'AD_TENANT_ID': hs_config["hs_ad_tenant_id"],
                'AD_RESOURCE_ID': hs_config["hs_ad_resource_id"],
                'AD_CLIENT_SECRET': hs_config.get("hs_ad_client_secret", None)
            }

        super().__init__(self.config['AD_APP_ID'])

    def acquire(self):
        """Acquire a new Azure token."""

        app_id = self.config["AD_APP_ID"]
        resource_id = self.config["AD_RESOURCE_ID"]
        client_secret = self.config["AD_CLIENT_SECRET"]
        authority_uri = self.AUTHORITY_URI + '/' + self.config["AD_TENANT_ID"]

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

        if 'expiresOn' in mgmt_token:
            mgmt_token['expiresOn'] = datetime.strptime(mgmt_token['expiresOn'],
                                                        '%Y-%m-%d %H:%M:%S.%f')

        self._token = mgmt_token
        self.write_token_cache()

    def refresh(self):
        """Try to renew an Azure token."""

        token = self._token
        if 'refreshToken' not in token:
            return None

        try:

            authority_uri = self.AUTHORITY_URI + '/' + self.config["AD_TENANT_ID"]
            context = adal.AuthenticationContext(authority_uri, api_version=None)
            token = context.acquire_token_with_refresh_token(token['refreshToken'],
                                                             self.config['AD_APP_ID'],
                                                             self.config['AD_RESOURCE_ID'],
                                                             self.config['AD_CLIENT_SECRET'])

            if 'expiresOn' in token:
                token['expiresOn'] = datetime.strptime(token['expiresOn'],
                                                       '%Y-%m-%d %H:%M:%S.%f')
            self._token = token

        except:
            self._token = None

        self.write_token_cache()


class GoogleOpenID(OpenIDHandler):

    def __init__(self, config=None, scopes=None):
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
                    'project_id': hs_config['hs_google_project_id'],
                    'client_id': hs_config['hs_google_client_id'],
                    'client_secret': hs_config['hs_google_client_secret'],
                    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                    'token_uri': 'https://oauth2.googleapis.com/token',
                    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
                    'redirect_uris': ['urn:ietf:wg:oauth:2.0:oob', 'http://localhost']
                }
            }

        super().__init__(self.config['installed']['client_id'])

    def _parse(self, creds):
        """Parse credentials and store if valid."""

        token = {
            'accessToken': creds.id_token,
            'refreshToken': creds.refresh_token,
        }

        # The expiry field that is in creds is for the OAuth token, not the
        # OpenID token. We need to validate the OpenID tokenn to get the exp.
        idinfo = GoogleIDToken.verify_oauth2_token(creds.id_token, GoogleRequest())
        if 'exp' in idinfo:
            token['expiresOn'] = idinfo['exp']

        return token

    def acquire(self):
        """Acquire a new Google OAuth token."""

        flow = GoogleInstalledAppFlow.from_client_config(self.config,
                                                         scopes=self.scopes)
        creds = flow.run_console()
        self._token = self._parse(creds)
        self.write_token_cache()

    def refresh(self):
        """Try to renew a token."""

        try:

            config = self.config['installed']
            creds = GoogleCredentials(token=None,
                                      refresh_token=self._token['refreshToken'],
                                      scopes=self.scopes,
                                      token_uri=config['token_uri'],
                                      client_id=config['client_id'],
                                      client_secret=config['client_secret'])

            creds.refresh(GoogleRequest())
            self._token = self._parse(creds)

        except:
            self._token = None

        self.write_token_cache()
