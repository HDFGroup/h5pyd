##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 REST Server) Service, Libraries and        #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

from __future__ import absolute_import

import os
import sys
import multiprocessing

import base64
import requests
import requests_unixsocket
from requests import ConnectionError
from requests.adapters import HTTPAdapter, Retry
import json
import logging

from . import openid
from .objdb import ObjDB
from .. import config
from . import requests_lambda


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


DEFAULT_TIMEOUT = (
    10,
    1000,
)  # #20  # 180  # seconds - allow time for hsds service to bounce

"""
def verifyCert(self):
    # default to validate CERT for https requests, unless
    # the H5PYD_VERIFY_CERT environment variable is set and True
    #
    # TBD: set default to True once the signing authority of data.hdfgroup.org is
    # recognized
    if "H5PYD_VERIFY_CERT" in os.environ:
        verify_cert = os.environ["H5PYD_VERIFY_CERT"].upper()
        if verify_cert.startswith('F'):
            return False
    return True
"""


def getAzureApiKey():
    """construct API key for Active Directory if configured"""
    # TBD: GoogleID?

    api_key = None

    # if Azure AD ids are set, pass them to HttpConn via api_key dict
    cfg = config.get_config()  # pulls in state from a .hscfg file (if found).

    ad_app_id = None  # Azure AD HSDS Server id
    if "HS_AD_APP_ID" in os.environ:
        ad_app_id = os.environ["HS_AD_APP_ID"]
    elif "hs_ad_app_id" in cfg:
        ad_app_id = cfg["hs_ad_app_id"]
    ad_tenant_id = None  # Azure AD tenant id
    if "HS_AD_TENANT_ID" in os.environ:
        ad_tenant_id = os.environ["HS_AD_TENANT_ID"]
    elif "hs_ad_tenant_id" in cfg:
        ad_tenant_id = cfg["hs_ad_tenant_id"]

    ad_resource_id = None  # Azure AD resource id
    if "HS_AD_RESOURCE_ID" in os.environ:
        ad_resource_id = os.environ["HS_AD_RESOURCE_ID"]
    elif "hs_ad_resource_id" in cfg:
        ad_resource_id = cfg["hs_ad_resource_id"]

    ad_client_secret = None  # Azure client secret
    if "HS_AD_CLIENT_SECRET" in os.environ:
        ad_client_secret = os.environ["HS_AD_CLIENT_SECRET"]
    elif "hs_ad_client_secret" in cfg:
        ad_client_secret = cfg["hs_ad_client_secret"]

    if ad_app_id and ad_tenant_id and ad_resource_id:
        # contruct dict to pass to HttpConn
        api_key = {
            "AD_APP_ID": ad_app_id,
            "AD_TENANT_ID": ad_tenant_id,
            "AD_RESOURCE_ID": ad_resource_id,
            "openid_provider": "azure",
        }
        # optional config
        if ad_client_secret:
            api_key["AD_CLIENT_SECRET"] = ad_client_secret
    return api_key  # None if AAD not configured


def getKeycloakApiKey():
    # check for keycloak next
    cfg = config.get_config()  # pulls in state from a .hscfg file (if found).
    api_key = None
    # check to see if we are configured for keycloak authentication
    if "HS_KEYCLOAK_URI" in os.environ:
        keycloak_uri = os.environ["HS_KEYCLOAK_URI"]
    elif "hs_keycloak_uri" in cfg:
        keycloak_uri = cfg["hs_keycloak_uri"]
    else:
        keycloak_uri = None
    if "HS_KEYCLOAK_CLIENT_ID" in os.environ:
        keycloak_client_id = os.environ["HS_KEYCLOAK_CLIENT_ID"]
    elif "hs_keycloak_client_id" in cfg:
        keycloak_client_id = cfg["hs_keycloak_client_id"]
    else:
        keycloak_client_id = None
    if "HS_KEYCLOAK_REALM" in os.environ:
        keycloak_realm = cfg["HS_KEYCLOAK_REALM"]
    elif "hs_keycloak_realm" in cfg:
        keycloak_realm = cfg["hs_keycloak_realm"]
    else:
        keycloak_realm = None

    if keycloak_uri and keycloak_client_id and keycloak_uri:
        api_key = {
            "keycloak_uri": keycloak_uri,
            "keycloak_client_id": keycloak_client_id,
            "keycloak_realm": keycloak_realm,
            "openid_provider": "keycloak",
        }
    return api_key


class HttpResponse:
    """ wrapper for http request responses """
    def __init__(self, rsp, logger=None):
        self._rsp = rsp
        self._logger = logger
        if logger is None:
            self.log = logging
        else:
            self.log = logging.getLogger(logger)
        self._text = None

    @property
    def status_code(self):
        """ return response status code """
        return self._rsp.status_code

    @property
    def reason(self):
        """ return response reason """
        return self._rsp.reason

    @property
    def content_type(self):
        """ return content type """
        rsp = self._rsp
        if 'Content-Type' in rsp.headers:
            content_type = rsp.headers['Content-Type']
        else:
            content_type = ""
        return content_type

    @property
    def content_length(self):
        """ Return length of response if available """
        if 'Content-Length' in self._rsp.headers:
            content_length = self._rsp.headers['Content-Length']
        else:
            content_length = None
        return content_length

    @property
    def is_binary(self):
        """ return True if the response indicates binary data """

        if self.content_type == "application/octet-stream":
            return True
        else:
            return False

    @property
    def is_json(self):
        """ return true if response indicates json """

        if self.content_type.startswith("application/json"):
            return True
        else:
            return False

    @property
    def text(self):
        """ getresponse content as bytes """

        if not self._text:
            rsp = self._rsp
            if not self.is_binary:
                # hex encoded response?
                # this is returned by API Gateway for lambda responses
                self._text = bytes.fromhex(rsp.text)
            else:
                if self.content_length:
                    self.log.debug(f"got binary response, {self.content_length} bytes")
                else:
                    self.log.debug("got binary response, content_length unknown")

                HTTP_CHUNK_SIZE = 4096
                http_chunks = []
                downloaded_bytes = 0
                for http_chunk in rsp.iter_content(chunk_size=HTTP_CHUNK_SIZE):
                    if http_chunk:  # filter out keep alive chunks
                        self.log.debug(f"got http_chunk - {len(http_chunk)} bytes")
                        downloaded_bytes += len(http_chunk)
                        http_chunks.append(http_chunk)
                if len(http_chunks) == 0:
                    raise IOError("no data returned")
                if len(http_chunks) == 1:
                    # can return first and only chunk as response
                    self._text = http_chunks[0]
                else:
                    msg = f"retrieved {len(http_chunks)} http_chunks "
                    msg += f" {downloaded_bytes} total bytes"
                    self.log.info(msg)
                    self._text = bytearray(downloaded_bytes)
                    index = 0
                    for http_chunk in http_chunks:
                        self._text[index:(index + len(http_chunk))] = http_chunk
                        index += len(http_chunk)

        return self._text

    def json(self):
        """ Return json from response"""

        rsp = self._rsp

        if not self.is_json:
            raise IOError("response is not json")

        rsp_json = json.loads(rsp.text)
        self.log.debug(f"rsp_json - {len(rsp.text)} bytes")
        return rsp_json


class HttpConn:
    """
    Some utility methods based on equivalents in base class.
    TBD: Should refactor these to a common base class
    """

    def __init__(
        self,
        domain_name,
        endpoint=None,
        username=None,
        password=None,
        bucket=None,
        api_key=None,
        mode="a",
        use_session=True,
        use_cache=True,
        logger=None,
        retries=3,
        timeout=DEFAULT_TIMEOUT,
        objdb=None,
        **kwds,
    ):
        self._domain = domain_name
        self._mode = mode
        self._domain_json = None
        self._use_session = use_session
        self._retries = retries
        self._timeout = timeout
        self._hsds = None
        self._lambda = None
        self._api_key = api_key
        self._s = None  # Sessions
        self._server_info = None

        self._logger = logger
        if logger is None:
            self.log = logging
        else:
            self.log = logging.getLogger(logger)
        msg = f"HttpConn.init(domain: {domain_name} use_session: {use_session} "
        msg += f"use_cache: {use_cache} retries: {retries}"
        self.log.debug(msg)

        if self._timeout != DEFAULT_TIMEOUT:
            self.log.info(f"HttpConn.init - timeout = {self._timeout}")
        if endpoint is None:
            if "HS_ENDPOINT" in os.environ:
                endpoint = os.environ["HS_ENDPOINT"]

        if not endpoint:
            msg = "no endpoint set"
            raise ValueError(msg)

        lambda_prefix = requests_lambda.LAMBDA_REQ_PREFIX

        if endpoint.startswith(lambda_prefix):
            # save lambda function name
            self._lambda = endpoint[len(lambda_prefix):]

        elif endpoint.startswith("local"):
            # create a local hsds server
            # set the number of nodes
            # if the endpoint is of the form: "local[n]", use n as the number of nodes
            # else set the number of nodes equal to number of cores
            bracket_start = endpoint.find("[")
            bracket_end = endpoint.find("]")
            dn_count = None
            if bracket_start > 0 and bracket_end > 0:
                try:
                    dn_count = int(endpoint[bracket_start + 1: bracket_end])
                except ValueError:
                    # if value is '*' or something just drop down to default
                    # setup based on cpu count
                    pass
            if not dn_count:
                dn_count = multiprocessing.cpu_count()
                dn_count = -(
                    -dn_count // 2
                )  # get the ceiling of count / 2 (don't include hyperthreading cores)
            if dn_count < 1:
                dn_count = 1

            try:
                from hsds.hsds_app import HsdsApp
            except ImportError:
                raise IOError("unable to import HSDS package")

            # path created by the python tempdir is too long for use with sockets
            # just use /tmp for now
            tmp_dir = "/tmp/hs"
            if not os.path.isdir(tmp_dir):
                os.mkdir(tmp_dir)
            log_dir = os.path.join(tmp_dir, "hs.log")
            hsds = HsdsApp(
                username=username,
                password=password,
                dn_count=dn_count,
                logfile=log_dir,
                socket_dir=tmp_dir,
            )
            hsds.run()
            self._hsds = hsds
            # replace 'local' with the socket path
            endpoint = hsds.endpoint
            self.log.debug(f"got hsds endpoint: {endpoint} for 'local' connection")

        self._endpoint = endpoint

        if username is None:
            if "HS_USERNAME" in os.environ:
                username = os.environ["HS_USERNAME"]
        if isinstance(username, str) and (not username or username.upper() == "NONE"):
            username = None
        self._username = username

        if password is None:
            if "HS_PASSWORD" in os.environ:
                password = os.environ["HS_PASSWORD"]
        if isinstance(password, str) and (not password or password.upper() == "NONE"):
            password = None
        self._password = password

        if bucket is None:
            if "HS_BUCKET" in os.environ:
                bucket = os.environ["HS_BUCKET"]
            if isinstance(bucket, str) and (not bucket or bucket.upper() == "NONE"):
                bucket = None
        self._bucket = bucket

        if api_key is None and "HS_API_KEY" in os.environ:
            api_key = os.environ["HS_API_KEY"]
        if isinstance(api_key, str) and (not api_key or api_key.upper() == "NONE"):
            api_key = None
        if not api_key:
            api_key = getAzureApiKey()
        if not api_key:
            api_key = getKeycloakApiKey()

        # Convert api_key to OpenIDHandler
        if isinstance(api_key, dict):
            # Maintain Azure-defualt backwards compatibility, but allow
            # both environment variable and kwarg override.
            provider = api_key.get("openid_provider", "azure")
            if provider == "azure":
                self.log.debug("creating OpenIDHandler for Azure")
                self._api_key = openid.AzureOpenID(endpoint, api_key)
            elif provider == "google":
                self.log.debug("creating OpenIDHandler for Google")

                config = api_key.get("client_secret", None)
                scopes = api_key.get("scopes", None)
                self._api_key = openid.GoogleOpenID(
                    endpoint, config=config, scopes=scopes
                )
            elif provider == "keycloak":
                self.log.debug("creating OpenIDHandler for Keycloak")

                # for Keycloak, pass in username and password
                self._api_key = openid.KeycloakOpenID(
                    endpoint, config=api_key, username=username, password=password
                )
            else:
                self.log.error(f"Unknown openid provider: {provider}")

        self._objdb = ObjDB(self, use_cache=use_cache)

    def __del__(self):
        if self._hsds:
            self.log.debug("hsds stop")
            self._hsds.stop()
            self._hsds = None
        if self._s:
            self.log.debug("close session")
            self._s.close()
            self._s = None

    def getHeaders(self, username=None, password=None, headers=None):

        if headers is None:
            headers = {}

        # This should be the default - but explicitly set anyway
        if "Accept-Encoding" not in headers:
            headers['Accept-Encoding'] = "deflate, gzip"

        elif "Authorization" in headers:
            return headers  # already have auth key
        if username is None:
            username = self._username
        if password is None:
            password = self._password

        if self._api_key:
            self.log.debug("using api key")
            # use OpenId handler to get a bearer token
            token = ""

            # Get a token, possibly refreshing if needed.
            if isinstance(self._api_key, openid.OpenIDHandler):
                token = self._api_key.token

            # Token was provided as a string.
            elif isinstance(self._api_key, str):
                token = self._api_key

            if token:
                auth_string = b"Bearer " + token.encode("ascii")
                headers["Authorization"] = auth_string
        elif username is not None and password is not None:
            self.log.debug(f"use basic auth with username: {username}")
            auth_string = username + ":" + password
            auth_string = auth_string.encode("utf-8")
            auth_string = base64.b64encode(auth_string)
            auth_string = b"Basic " + auth_string
            headers["Authorization"] = auth_string
        else:
            self.log.debug("no auth header")
            # no auth header
            pass

        return headers

    def serverInfo(self):
        if self._server_info:
            return self._server_info

        if self._endpoint is None:
            raise IOError("object not initialized")

        # make an about request
        rsp = self.GET("/about")
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        server_info = rsp.json()
        if server_info:
            self._server_info = server_info
        return server_info

    def server_version(self):
        server_info = self.serverInfo()
        if "hsds_version" in server_info:
            server_version = server_info["hsds_version"]
        else:
            # no standard way to get version for other implements...
            server_version = None
        return server_version

    def verifyCert(self):
        # default to validate CERT for https requests, unless
        # the H5PYD_VERIFY_CERT environment variable is set and True
        #
        # TBD: set default to True once the signing authority of data.hdfgroup.org is
        # recognized
        if "H5PYD_VERIFY_CERT" in os.environ:
            verify_cert = os.environ["H5PYD_VERIFY_CERT"].upper()
            if verify_cert.startswith("F"):
                return False
        return True

    @property
    def objdb(self):
        return self._objdb

    def GET(self, req, format="json", params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        # check that domain is defined (except for some specific requests)
        if req not in ("/domains", "/about", "/info", "/") and self._domain is None:
            raise IOError(f"no domain defined: req: {req}")

        rsp = None

        headers = self.getHeaders(headers=headers)

        if params is None:
            params = {}
        if "domain" not in params:
            params["domain"] = self._domain
        if "bucket" not in params and self._bucket:
            params["bucket"] = self._bucket
        if self._api_key and not isinstance(self._api_key, dict):
            params["api_key"] = self._api_key
        domain = params["domain"]
        self.log.debug(f"GET: {req} [{domain}] bucket: {self._bucket}")

        if format == "binary":
            headers["accept"] = "application/octet-stream"

        self.log.info(f"GET: {self._endpoint + req} [{params['domain']}] timeout: {self._timeout}")

        for k in params:
            if k != "domain":
                v = params[k]
                self.log.debug(f"GET params {k}:{v}")

        try:
            if self._hsds:
                self._hsds.run()

            s = self.session
            if self._lambda:
                stream = False
            else:
                stream = True

            rsp = s.get(
                self._endpoint + req,
                params=params,
                headers=headers,
                stream=stream,
                timeout=self._timeout,
                verify=self.verifyCert(),
            )
            self.log.info(f"status: {rsp.status_code}")
            if self._hsds:
                self._hsds.run()
        except ConnectionError as ce:
            self.log.error(f"connection error: {ce}")
            raise IOError("Connection Error")
        except Exception as e:
            self.log.error(f"got {type(e)} exception: {e}")
            raise IOError("Unexpected exception")

        return HttpResponse(rsp)

    def PUT(self, req, body=None, format="json", params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")

        if params:
            self.log.info(f"PUT params: {params}")
        else:
            params = {}

        if "domain" not in params:
            params["domain"] = self._domain
        if "bucket" not in params and self._bucket:
            params["bucket"] = self._bucket
        if self._api_key:
            params["api_key"] = self._api_key

        # verify the file was open for modification
        if self._mode == "r":
            raise IOError("Unable to create group (No write intent on file)")

        # try to do a PUT to the domain

        headers = self.getHeaders(headers=headers)

        if format == "binary":
            headers["Content-Type"] = "application/octet-stream"
            # binary write
            data = body
        else:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body)

        self.log.info(f"PUT: {req} format: {format} [{len(data)} bytes]")

        try:
            if self._hsds:
                self._hsds.run()
            s = self.session
            rsp = s.put(
                self._endpoint + req,
                data=data,
                headers=headers,
                params=params,
                verify=self.verifyCert(),
            )
            self.log.info(f"status: {rsp.status_code}")
            if self._hsds:
                self._hsds.run()
        except ConnectionError as ce:
            self.log.error(f"connection error: {ce}")
            raise IOError("Connection Error")

        if rsp.status_code == 201 and req == "/":
            self.log.info("clearing domain_json cache")
            self._domain_json = None
        self.log.info(f"PUT returning: {rsp}")
        return HttpResponse(rsp)

    def POST(self, req, body=None, format="json", params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")

        if params is None:
            params = {}
        if "domain" not in params:
            params["domain"] = self._domain
        if "bucket" not in params and self._bucket:
            params["bucket"] = self._bucket
        if self._api_key:
            params["api_key"] = self._api_key

        # verify we have write intent (unless this is a dataset point selection)
        if req.startswith("/datasets/") and req.endswith("/value"):
            point_sel = True
        else:
            point_sel = False
        if self._mode == "r" and not point_sel:
            raise IOError("Unable perform request (No write intent on file)")

        # try to do a POST to the domain

        headers = self.getHeaders(headers=headers)

        if isinstance(body, bytes):
            headers["Content-Type"] = "application/octet-stream"
            data = body
        else:
            # assume json
            try:
                data = json.dumps(body)
            except TypeError:
                msg = f"Unable to convert {body} to json"
                self.log.error(msg)
                raise IOError("JSON encoding error")
        if format == "binary":
            # recieve data as binary
            headers["accept"] = "application/octet-stream"

        self.log.info("POST: " + req)

        try:
            s = self.session
            rsp = s.post(
                self._endpoint + req,
                data=data,
                headers=headers,
                params=params,
                verify=self.verifyCert(),
            )
        except ConnectionError as ce:
            self.log.warning(f"connection error: {ce}")
            raise IOError(str(ce))

        if rsp.status_code not in (200, 201):
            self.log.error(f"POST error: {rsp.status_code}")

        return HttpResponse(rsp)

    def DELETE(self, req, params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")

        if req not in ("/domains", "/") and self._domain is None:
            raise IOError("no domain defined")
        if params is None:
            params = {}
        if "domain" not in params:
            params["domain"] = self._domain
        if "bucket" not in params and self._bucket:
            params["bucket"] = self._bucket
        if self._api_key:
            params["api_key"] = self._api_key

        # verify we have write intent
        if self._mode == "r":
            raise IOError("Unable perform request (No write intent on file)")

        # try to do a DELETE of the resource

        headers = self.getHeaders(headers=headers)

        self.log.info("DEL: " + req)
        try:
            s = self.session
            rsp = s.delete(
                self._endpoint + req,
                headers=headers,
                params=params,
                verify=self.verifyCert(),
            )
            self.log.info(f"status: {rsp.status_code}")
        except ConnectionError as ce:
            self.log.error(f"connection error: {ce}")
            raise IOError("Connection Error")

        if rsp.status_code == 200 and req == "/":
            self.log.info("clearing domain_json cache")
            self._domain_json = None

        return HttpResponse(rsp)

    @property
    def session(self):
        # create a session object to re-use http connection when possible
        s = requests
        retries = self._retries
        backoff_factor = 1
        status_forcelist = (500, 502, 503, 504)
        lambda_prefix = requests_lambda.LAMBDA_REQ_PREFIX

        if self._use_session:
            if self._s is None:
                if self._endpoint.startswith("http+unix://"):
                    self.log.debug(f"create unixsocket session: {self._endpoint}")
                    s = requests_unixsocket.Session()
                elif self._endpoint.startswith(lambda_prefix):
                    s = requests_lambda.Session()
                else:
                    # regular request session
                    s = requests.Session()

                retry = Retry(
                    total=retries,
                    read=retries,
                    connect=retries,
                    backoff_factor=backoff_factor,
                    status_forcelist=status_forcelist,
                )

                s.mount(
                    "http://",
                    HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16),
                )
                s.mount(
                    "https://",
                    HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16),
                )
                self._s = s
            else:
                s = self._s
        return s

    def close(self):
        if self._s:
            self._s.close()
            self._s = None
        if self._hsds:
            self._hsds.stop()
            self._hsds = None

    @property
    def domain(self):
        return self._domain

    @property
    def username(self):
        return self._username

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def password(self):
        return self._password

    @property
    def mode(self):
        return self._mode

    @property
    def domain_json(self):
        if self._domain_json is None:
            rsp = self.GET("/")
            if rsp.status_code != 200:
                raise IOError(rsp.reason)
            # assume JSON
            self._domain_json = rsp.json()
        return self._domain_json

    @property
    def root_uuid(self):
        domain_json = self.domain_json
        if "root" not in domain_json:
            raise IOError("Unexpected response")
        root_uuid = domain_json["root"]
        return root_uuid

    @property
    def compressors(self):
        compressors = []
        if "compressors" in self.domain_json:
            compressors = self.domain_json["compressors"]
        if not compressors:
            compressors = [
                "gzip",
            ]
        return compressors

    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        domain_json = self.domain_json
        if "lastModified" not in domain_json:
            raise IOError("Unexpected response")
        last_modified = domain_json["lastModified"]
        return last_modified

    @property
    def created(self):
        """Creation time of the domain"""
        domain_json = self.domain_json
        if "created" not in domain_json:
            raise IOError("Unexpected response")
        created = domain_json["created"]
        return created

    @property
    def owner(self):
        """username of creator of domain"""
        domain_json = self.domain_json
        username = None
        if "owner" in domain_json:
            # currently this is only available for HSDS
            username = domain_json["owner"]
        return username

    @property
    def logging(self):
        """return name of logging handler"""
        return self.log
