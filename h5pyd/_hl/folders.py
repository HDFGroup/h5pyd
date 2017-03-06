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

import os.path as op
import six
#from requests import ConnectionError
import json
import logging
from .httputil import HttpUtil
 
 
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
        """Create a new Folders object.


        domain_name
            URI of the domain name to access. E.g.: /org/hdfgroup/folder/
        
        endpoint
            Server endpoint.   Defaults to "http://localhost:5000"
        """

        self.log = logging.getLogger("h5pyd")

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
            if rsp.status_code < 500:
                self.log.warn("status_code: {}".format(rsp.status_code))
            else:
                self.log.error("status_code: {}".format(rsp.status_code))
            raise IOError(rsp.status_code, rsp.reason)
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
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = json.loads(rsp.text)
        acls_json = rsp_json["acls"] 
        return acls_json

    def putACL(self, acl):
        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            perm[k] = acl[k]
         
        req = '/acls/' + acl['userName']
        rsp = self._http.PUT(req, body=perm)
        if rsp.status_code != 201:
            raise IOError(rsp.status_code, rsp.reason)

    # TBD: Replace with implementation that can handle large collections
    def _getSubdomains(self):
        req = '/domains'
        rsp = self._http.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = json.loads(rsp.text)
        if "domains" not in rsp_json:
            raise IOError(500, "Unexpected Error")
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
        self._http.DELETE(req, headers=headers)
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
