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
from .httpconn import HttpConn
from .config import Config
 
 
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

    @property
    def is_folder(self):
        """ is this a proper folder (i.e. domain without root group), 
            or a domain """
        if self._obj_class == "folder":
            return True
        else:
            return False
        

    def __init__(self, domain_name, mode=None, endpoint=None, 
        username=None, password=None, logger=None, **kwds):
        """Create a new Folders object.


        domain_name
            URI of the domain name to access. E.g.: /org/hdfgroup/folder/
        
        endpoint
            Server endpoint.   Defaults to "http://localhost:5000"
        """

        self.log = logging.getLogger("h5pyd")

        if len(domain_name) == 0: 
            raise ValueError("Invalid folder name")
        
        if domain_name[0] != '/':
            raise ValueError("Folder name must start with '/'")

        if domain_name[-1] != '/':
            raise ValueError("Folder name must end with '/'")

        if mode and mode not in ('r', 'r+', 'w', 'w-', 'x', 'a'):
            raise ValueError("Invalid mode; must be one of r, r+, w, w-, x, a")

        if mode is None:
            mode = 'r'

        cfg = None
        if endpoint is None or username is None or password is None:
            # unless we'r given all the connect info, create a config object that
            # pulls in state from a .hscfg file (if found).
            cfg = Config()
 
        if endpoint is None and "hs_endpoint" in cfg:
            endpoint = cfg["hs_endpoint"]

        if username is None and "hs_username" in cfg:
            username = cfg["hs_username"]
              
        if password is None and "hs_password" in cfg:
            password = cfg["hs_password"]

        if len(domain_name) <= 1:
            self._domain = None
        else:
            self._domain = domain_name[:-1]
        self._subdomains = None
        self._http_conn = HttpConn(self._domain, endpoint=endpoint, username=username, password=password, mode=mode, logger=logger)
        self.log = self._http_conn.logging

        domain_json = None

        # try to do a GET from the domain
        if domain_name == '/':
            if mode != 'r':
                raise IOError(400, "mode must be 'r' for top-level domain")
            req = "/domains"
        else:
            req = '/'
                        
        rsp = self._http_conn.GET(req)

        if rsp.status_code in (404, 410) and mode in ('w', 'w-', 'x'):
            # create folder
            body = {"folder": True}
            rsp = self._http_conn.PUT(req, body=body)  
            if rsp.status_code != 201:
                self._http_conn.close() 
                raise IOError(rsp.status_code, rsp.reason)
        elif rsp.status_code != 200:
            # folder must exist
            if rsp.status_code < 500:
                self.log.warning("status_code: {}".format(rsp.status_code))
            else:
                self.log.error("status_code: {}".format(rsp.status_code))
            raise IOError(rsp.status_code, rsp.reason)
        domain_json = json.loads(rsp.text)
        if "class" in domain_json:
            if domain_json["class"] != "folder":
                self.log.warning("Not a folder domain")
            self._obj_class = domain_json["class"]
        elif "root" in domain_json:
            # open with Folder but actually has a root group
            self._obj_class = "domain" 
        else:
            self._obj_class = "folder" 
        self._name = domain_name
        if "created" in domain_json:
            self._created = domain_json['created']
        else:
            self._created = None
        if "lastModified" in domain_json:
            self._modified = domain_json['lastModified']
        else:
            self._modified = None
        if "owner" in domain_json:
            self._owner = domain_json["owner"]
        else:
            self._owner = None

    def getACL(self, username):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        req = '/acls/' + username
        rsp = self._http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.reason)
        rsp_json = json.loads(rsp.text)
        acl_json = rsp_json["acl"]
        return acl_json

    def getACLs(self):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        req = '/acls'
        rsp = self._http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = json.loads(rsp.text)
        acls_json = rsp_json["acls"] 
        return acls_json

    def putACL(self, acl):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._http_conn.mode == 'r':
            raise IOError(400, "folder is open as read-onnly")
        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            if k not in acl:
                raise IOError(404, "Missing ACL field: {}".format(k))
            perm[k] = acl[k]
         
        req = '/acls/' + acl['userName']
        rsp = self._http_conn.PUT(req, body=perm)
        if rsp.status_code != 201:
            raise IOError(rsp.status_code, rsp.reason)


    def _getSubdomains(self):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        req = '/domains'
        if self._domain is None:
            params = {"domain": '/'}
        else:
            params = {"domain": self._domain + '/'}
        rsp = self._http_conn.GET(req, params=params)
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
        self._http_conn.close()
        self._http_conn = None

    def __getitem__(self, name):
        """ Get a domain  """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._subdomains is None:
            self._subdomains = self._getSubdomains()
        domains = self._subdomains
        for domain in domains:
            if op.basename(domain["name"]) == name:
                return domain
        return None

    def __delitem__(self, name):
        """ Delete domain. """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._http_conn.mode == 'r':
            raise IOError(400, "folder is open as read-onnly")
        domain = self._domain + '/' + name
        headers = self._http_conn.getHeaders(domain=domain)
        req = '/'
        self._http_conn.DELETE(req, headers=headers)
        self._subdomains = None # reset the cache list
        #self.id.unlink(self._e(name))

    def __len__(self):
        """ Number of subdomains of this folder """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        domains = self._getSubdomains()
        return len(domains)
         

    def __iter__(self):
        """ Iterate over subdomain names """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        domains = self._getSubdomains()
        for domain in domains:
            yield op.basename(domain['name'])
         

    def __contains__(self, name):
        """ Test if a member name exists """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        domains = self._getSubdomains()
        found = False
        for domain in domains:
            if op.basename(domain['name']) == name:
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
