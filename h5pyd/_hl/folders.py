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
import os.path as op
import six
import base64
import requests
import json
import logging
from .base import parse_lastmodified

 
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

        if not headers:
            headers = self.getHeaders() 
         
        if format == "binary":
            headers['accept'] = 'application/octet-stream'
        self.log.info("GET: {} [{}]".format(req, headers["host"]))
 
        rsp = requests.get(req, headers=headers, verify=self.verifyCert())
        return rsp

    def PUT(self, req, body=None, params=None, headers=None):
        if self._endpoint is None:
            raise IOError("object not initialized")
        if self._domain is None:
            raise IOError("no domain defined")

        # try to do a PUT to the domain
        req = self._endpoint + req  
        
        if not headers:
            headers = self.getHeaders() 

        self.log.info("PUT: " + req)
        if 'Content-Type' in headers and headers['Content-Type'] == "application/octet-stream":
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
         
        rsp = requests.post(req, data=data, headers=headers, verify=self.verifyCert())
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

class Folder():

    """
        Represents a folder of domains
    """

    @property
    def domain(self):
        return self._domain + '/'

    @property
    def parent(self):
        parent = op.dirname(self._domain)
        if not parent:
            return None
        return parent + '/'


    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        return self._modified

    @property
    def created(self):
        """creation time of the domain as a datetime object."""
        return self._created

    @property
    def owner(self):
        """Username of the owner of the folder """
        return self._owner

    def __init__(self, domain_name, mode=None, endpoint=None, 
        username=None, password=None, **kwds):
        """Create a new file object.


        domain_name
            URI of the domain name to access. E.g.: /org/hdfgroup/folder/
        
        endpoint
            Server endpoint.   Defaults to "http://localhost:5000"
        """

        if len(domain_name) < 2: 
            raise ValueError("Invalid folder name")

        if domain_name[-1] != '/':
            raise ValueError("Folder name must end with '/'")

        self._domain = domain_name[:-1]
        self._http = HttpUtil(self._domain, endpoint=endpoint, username=username, password=password)

        domain_json = None

        # try to do a GET from the domain
        req = "/"
                        
        rsp = self._http.GET(req)

        if rsp.status_code != 200:
            # file must exist
            raise IOError(rsp.reason)
        domain_json = json.loads(rsp.text)
         
        self._name = domain_name
        self._created = domain_json['created']
        self._modified = domain_json['lastModified']
        if "owner" in domain_json:
            self._owner = domain_json["owner"]
        else:
            self._owner = None


    def getACL(self, username):
        req = '/acls/' + username
        rsp = self._http.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.reason)
        rsp_json = json.loads(rsp.text)
        acl_json = rsp_json["acl"]
        return acl_json

    def getACLs(self):
        req = '/acls'
        rsp = self._http.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.reason)
        rsp_json = json.loads(rsp.text)
        acls_json = rsp_json["acls"] 
        return acls_json

    def putACL(self, acl):
        if "userName" not in acl:
            raise IOError("ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            perm[k] = acl[k]
         
        req = '/acls/' + acl['userName']
        rsp = self._http.PUT(req, body=perm)
        if rsp.status_code != 201:
            raise IOError(rsp.reason)

    # TBD: Replace with implementation that can handle large collections
    def _getSubdomains(self):
        req = '/domains'
        rsp = self._http.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.reason)
        rsp_json = json.loads(rsp.text)
        if "domains" not in rsp_json:
            raise IOError("Unexpected Error")
        domains = rsp_json["domains"]
        return domains


    def close(self):
        """ Clears reference to remote resource.
        """
        self._domain = None

    def __getitem__(self, name):
        """ Get a domain  """
        domains = self._getSubdomains()
        for domain in domains:
            if domain["name"] == name:
                return domain
        return None

    def __delitem__(self, name):
        """ Delete domain. """
        domain = self._domain + '/' + name
        headers = self._http.getHeaders(domain=domain)
        req = '/'
        self.DELETE('/', headers=headers)
        #self.id.unlink(self._e(name))

    def __len__(self):
        """ Number of subdomains of this folder """
        domains = self._getSubdomains()
        return len(domains)
         

    def __iter__(self):
        """ Iterate over subdomain names """
        domains = self._getSubdomains()
        for domain in domains:
            yield domain['name']
         

    def __contains__(self, name):
        """ Test if a member name exists """
        domains = self._getSubdomains()
        found = False
        for domain in domains:
            if domain['name'] == name:
                found = True
                break
        
        return found

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __repr__(self):
        
        r = six.u(self._domain + '/')
             
        if six.PY3:
            return r
        return r.encode('utf8')
