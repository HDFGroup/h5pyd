##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

from __future__ import absolute_import

import os
import base64
import requests
from requests import ConnectionError
import json
import logging

MAX_CACHE_ITEM_SIZE=10000  # max size of an item to put in the cache

class CacheResponse(object):
    """ Wrap a json response in a Requests.Response looking class.
        Note: we don't want to keep a proper requests obj in the cache since it 
        would contain refernces to other objects
    """
    def __init__(self, rsp):
        # just save off what we need
        self._text = rsp.text
        self._status_code = rsp.status_code
        self._headers = rsp.headers

    @property
    def text(self):
        return self._text

    @property
    def status_code(self):
        return self._status_code

    @property
    def headers(self):
        return self._headers

 
class HttpConn:
    """
    Some utility methods based on equivalents in base class.
    TBD: Should refactor these to a common base class
    """
    def __init__(self, domain_name, endpoint=None, username=None, password=None, 
            mode='a', use_session=True, use_cache=False, logger=None, **kwds):
        self._domain = domain_name
        self._mode = mode
        self._domain_json = None
        self._use_session = use_session
        if use_cache:
            self._cache = {}
        else:
            self._cache = None
        self._logger = logger
        if logger is None:
            self.log = logging
        else:
            self.log = logging.getLogger(logger)
        self.log = logging
        if endpoint is None:
            if "H5SERV_ENDPOINT" in os.environ:
                self._endpoint = os.environ["H5SERV_ENDPOINT"]
            else:
                self._endpoint = "http://127.0.0.1:5000"
        else:
            self._endpoint = endpoint

        if username is None and "H5SERV_USERNAME" in os.environ:
            self._username = os.environ["H5SERV_USERNAME"]
        else:
            self._username = username
        if password is None and "H5SERV_PASSWORD" in os.environ:
            self._password = os.environ["H5SERV_PASSWORD"]
        else:
            self._password = password
        self._s = None  # Sessions 
        

    def getHeaders(self, domain=None, username=None, password=None, headers=None):
        if headers is None:
            headers = {}
        if domain is None:
            domain = self._domain
        if username is None:
            username = self._username
        if password is None:
            password = self._password
        headers['host'] = domain
        if username is not None and password is not None:
            auth_string = username + ':' + password
            auth_string = auth_string.encode('utf-8')
            auth_string = base64.b64encode(auth_string)
            auth_string = b"Basic " + auth_string
            headers['Authorization'] = auth_string
        return headers

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

    def GET(self, req, format="json", params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        #if self._domain is None:
        #    raise IOError("no domain defined")
        rsp = None

        if not headers:
            headers = self.getHeaders() 
         
        if format == "binary":
            headers['accept'] = 'application/octet-stream'

        if self._cache is not None and format == "json" and headers["host"] == self._domain:
            if req in self._cache:
                rsp = self._cache[req]
                return rsp
        
        self.log.info("GET: {} [{}]".format(self._endpoint + req, headers["host"]))
        if self._username and self._password:
            auth = (self._username, self._password)
        else:
            auth = None
        try:
            s = self.session
            rsp = s.get(self._endpoint + req, params=params, headers=headers, auth=auth, verify=self.verifyCert())
            self.log.info("status: {}".format(rsp.status_code))
        except ConnectionError as ce:
            self.log.error("connection error: {}".format(ce))
            raise IOError("Connection Error")
        if rsp.status_code == 200 and self._cache is not None:
            rsp_headers = rsp.headers
            content_length = 0
            try:
                content_length = int(rsp_headers['Content-Length'])
            except ValueError:
                content_length = MAX_CACHE_ITEM_SIZE + 1

            if rsp_headers['Content-Type'] == 'application/json' and content_length < MAX_CACHE_ITEM_SIZE:
            
                # add to our _cache
                cache_rsp = CacheResponse(rsp)
                self._cache[req] = cache_rsp
        return rsp

    def PUT(self, req, body=None, format="json", params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")
        if self._cache is not None:   
            # domain deletin, invalidate everything in cache
            self._cache = {}  
        if params:
            self.log.info("PUT params: {}".format(params))

        req = self._endpoint + req
        
        # try to do a PUT to the domain
         
        if not headers:
            headers = self.getHeaders() 
        
        if format=="binary":
            headers['Content-Type'] = "application/octet-stream"
            # binary write
            data = body    
        else:
            data = json.dumps(body)
        self.log.info("PUT: {} format: {} [{} bytes]".format(req, format, len(data)))
        if self._username and self._password:
            auth = (self._username, self._password)
        else:
            auth = None
        try:
            s = self.session
            rsp = s.put(req, data=data, headers=headers, params=params, auth=auth, verify=self.verifyCert())
            self.log.info("status: {}".format(rsp.status_code))
        except ConnectionError as ce:
            self.log.error("connection error: {}".format(ce))
            raise IOError("Connection Error")
 
        return rsp

    def POST(self, req, body=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")
        if self._cache is not None:  
            self._cache = {}
            
        # try to do a POST to the domain
        req = self._endpoint + req

        data = json.dumps(body)

        if not headers:
            headers = self.getHeaders() 

        self.log.info("PST: " + req)

        if self._username and self._password:
            auth = (self._username, self._password)
        else:
            auth = None
        try: 
            s = self.session
            rsp = s.post(req, data=data, headers=headers, auth=auth, verify=self.verifyCert())
        except ConnectionError as ce:
            self.log.warn("connection error: ", ce)
            raise IOError(str(ce))

        return rsp

    def DELETE(self, req, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")
        if self._cache is not None:
            self._cache = {}
             
        # try to do a DELETE of the resource
        req = self._endpoint + req

        if not headers:
            headers = self.getHeaders() 

        self.log.info("DEL: " + req)
        if self._username and self._password:
            auth = (self._username, self._password)
        else:
            auth = None
        try:
            s = self.session
            rsp = s.delete(req, headers=headers, auth=auth, verify=self.verifyCert())
            self.log.info("status: {}".format(rsp.status_code))
        except ConnectionError as ce:
            self.log.error("connection error: {}".format(ce))
            raise IOError("Connection Error")
 
        return rsp
    
    @property
    def session(self):
        # create a session object to re-use http connection when possible
        # TBD: Add retry here - see: https://laike9m.com/blog/requests-secret-pool_connections-and-pool_maxsize,89/
        s = requests
        if self._use_session:
            if self._s is None:
                self._s = requests.Session()
            s = self._s
        return s

    def close(self):
        if self._s:
            self._s.close()
            self._s = None

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
            rsp = self.GET('/')
            if rsp.status_code != 200:
                raise IOError(rsp.reason)
            # assume JSON
            self._domain_json = json.loads(rsp.text)
        return self._domain_json

    @property
    def root_uuid(self):
        domain_json = self.domain_json
        if "root" not in domain_json:
            raise IOError("Unexpected response")
        root_uuid = domain_json["root"] 
        return root_uuid

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
        """ username of creator of domain"""
        domain_json = self.domain_json
        username = None
        if 'owner' in domain_json:
            # currently this is only available for HSDS 
            username = domain_json["owner"]
        return username

    @property
    def logging(self):
        """ return name of logging handler"""
        return self.log
