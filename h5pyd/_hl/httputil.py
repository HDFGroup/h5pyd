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

 
class HttpUtil:
    """
    Some utility methods based on equivalents in base class.
    TBD: Should refactor these to a common base class
    """
    def __init__(self, domain_name, endpoint=None, 
        username=None, password=None, **kwds):
        self._domain = domain_name
        self.log = logging.getLogger("h5pyd")
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

    def GET(self, req, format="json", headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")
        
        req = self._endpoint + req
        rsp = None

        if not headers:
            headers = self.getHeaders() 
         
        if format == "binary":
            headers['accept'] = 'application/octet-stream'
        
        self.log.info("GET: {} [{}]".format(req, headers["host"]))
        try:
            rsp = requests.get(req, headers=headers, verify=self.verifyCert())
            self.log.info("status: {}".format(rsp.status_code))
        except ConnectionError as ce:
            self.log.error("connection error: {}".format(ce))
            raise IOError("Connection Error")
         
        
        return rsp

    def PUT(self, req, body=None, format="json", params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")

        req = self._endpoint + req
        # try to do a PUT to the domain
         
        if not headers:
            headers = self.getHeaders() 

        self.log.info("PUT: " + req)
        if format=="binary":
            headers['Content-Type'] = "application/octet-stream"
            # binary write
            data = body
        else:
            data = json.dumps(body)
        # self.log.info("BODY: " + str(data))
        rsp = requests.put(req, data=data, headers=headers,
                           params=params, verify=self.verifyCert())
        return rsp

    def POST(self, req, body=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")

        # try to do a POST to the domain
        req = self._endpoint + req

        data = json.dumps(body)

        if not headers:
            headers = self.getHeaders() 

        self.log.info("PST: " + req)

        try: 
            rsp = requests.post(req, data=data, headers=headers, verify=self.verifyCert())
        except ConnectionError as ce:
            print("connection error: ", ce)
            raise IOError(str(ce))

        return rsp

    def DELETE(self, req, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")

        # try to do a DELETE of the resource
        req = self._endpoint + req

        if not headers:
            headers = self.getHeaders() 

        self.log.info("DEL: " + req)
        rsp = requests.delete(req, headers=headers, verify=self.verifyCert())
        return rsp

