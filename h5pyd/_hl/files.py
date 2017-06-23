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
import six
import time
import json

from .objectid import GroupID
from .group import Group
from .. import version
from .httpconn import HttpConn
from .config import Config

VERBOSE_REFRESH_TIME=1.0  # 1 second


class File(Group):

    """
        Represents an HDF5 file.
    """

    @property
    def attrs(self):
        """ Attributes attached to this object """
        # hdf5 complains that a file identifier is an invalid location for an
        # attribute. Instead of self, pass the root group to AttributeManager:
        from . import attrs
        return attrs.AttributeManager(self['/'])

    @property
    def filename(self):
        """File name on disk"""
        return self.id.http_conn.domain

    @property
    def driver(self):
        return "rest_driver"

    @property
    def mode(self):
        """ Python mode used to open file """
        return self.id.http_conn.mode

    @property
    def fid(self):
        """File ID (backwards compatibility) """
        return self.id.domain

    @property
    def libver(self):
        """File format version bounds (2-tuple: low, high)"""
        # bounds = self.id.get_access_plist().get_libver_bounds()
        # return tuple(libver_dict_r[x] for x in bounds)
        return ("0.0.1",)

    @property
    def userblock_size(self):
        """ User block size (in bytes) """
        return 0

    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        return self.id.http_conn.modified

    @property
    def created(self):
        """Creation time of the domain"""
        return self.id.http_conn.created

    @property
    def owner(self):
        """Username of the owner of the domain"""
        return self.id.http_conn.owner

    def __init__(self, domain, mode=None, endpoint=None, username=None, password=None, 
        use_session=True, use_cache=False, logger=None, **kwds):
        """Create a new file object.

        See the h5py user guide for a detailed explanation of the options.

        domain
            URI of the domain name to access. E.g.: tall.data.hdfgroup.org.
        mode
            Access mode: 'r', 'r+', 'w', or 'a'
        endpoint
            Server endpoint.   Defaults to "http://localhost:5000"
        """

        
        groupid = None
        # if we're passed a GroupId as domain, jsut initialize the file object
        # with that.  This will be faster and enable the File object to share the same http connection.
        if mode is None and endpoint is None and username is None \
            and password is None and isinstance(domain, GroupID):
            groupid = domain
        else:
            
            if mode and mode not in ('r', 'r+', 'w', 'w-', 'x', 'a'):
                raise ValueError(
                    "Invalid mode; must be one of r, r+, w, w-, x, a")

            if mode is None:
                mode = 'a'

            cfg = None
            if endpoint is None or username is None or password is None:
                # unless we'r given all the connect info, create a config object that
                # pulls in state from a .hscfg file (if found).
                cfg = Config()
 
            if endpoint is None:
                if "H5SERV_ENDPOINT" in os.environ:
                    endpoint = os.environ["H5SERV_ENDPOINT"]
                elif "hs_endpoint" in cfg:
                    endpoint = cfg["hs_endpoint"]
                else:
                    endpoint = "http://127.0.0.1:5000"

            if username is None:
                if "H5SERV_USERNAME" in os.environ:
                    username = os.environ["H5SERV_USERNAME"]
                elif "hs_username" in cfg:
                    username = cfg["hs_username"]
                
            if password is None:
                if "H5SERV_PASSWORD" in os.environ:
                    password = os.environ["H5SERV_PASSWORD"]
                elif "hs_password" in cfg:
                    password = cfg["hs_password"]
  
            http_conn =  HttpConn(domain, endpoint=endpoint, 
                    username=username, password=password, mode=mode, 
                    use_session=use_session, use_cache=use_cache, logger=logger)
        
            root_json = None

            # try to do a GET from the domain
            req = "/"      
                        
            rsp = http_conn.GET(req)  

            if rsp.status_code == 200:
                root_json = json.loads(rsp.text)
            if rsp.status_code != 200 and mode in ('r', 'r+'):
                # file must exist
                http_conn.close() 
                raise IOError(rsp.status_code, rsp.reason)
            if rsp.status_code == 200 and mode in ('w-', 'x'):
                # Fail if exists
                http_conn.close() 
                raise IOError(409, "domain already exists")
            if rsp.status_code == 200 and mode == 'w':
                # delete existing domain
                rsp = http_conn.DELETE(req)
                if rsp.status_code != 200:
                    # failed to delete
                    http_conn.close() 
                    raise IOError(rsp.status_code, rsp.reason)
                root_json = None
            if root_json and 'root' not in root_json:
                http_conn.close()
                raise IOError(404, "Location is a folder, not a file")
            if root_json is None:
                # create the domain
                if mode not in ('w', 'a', 'x'):
                    http_conn.close() 
                    raise IOError(404, "File not found")
                rsp = http_conn.PUT(req)  
                if rsp.status_code != 201:
                    http_conn.close() 
                    raise IOError(rsp.status_code, rsp.reason)
                 
                root_json = json.loads(rsp.text)
            
            if 'root' not in root_json:
                http_conn.close() 
                raise IOError(404, "Unexpected error")
            root_uuid = root_json['root']

            if mode == 'a':
                # for append, verify we have 'update' permission on the domain
                # try first with getting the acl for the current user, then as default
                for name in (username, "default"):
                    if not username:
                        continue
                    req = "/acls/" + name
                    rsp = http_conn.GET(req)  
                    if rsp.status_code == 200:
                        rspJson = json.loads(rsp.text)
                        domain_acl = rspJson["acl"]
                        if not domain_acl["update"]:
                            http_conn.close() 
                            raise IOError(403, "Forbidden")
                        else:
                            break  # don't check with "default" user in this case

            if mode in ('w', 'w-', 'x', 'a'):
                http_conn._mode = 'r+'

            # get the group json for the root group
            req = "/groups/" + root_uuid

            rsp = http_conn.GET(req)

            if rsp.status_code != 200:
                http_conn.close() 
                raise IOError(rsp.status_code, "Unexpected Error")
            group_json = json.loads(rsp.text)

            groupid = GroupID(None, group_json, http_conn=http_conn)
        # end else
        self._name = '/'
        self._id = groupid
        self._verboseInfo = None  # aditional state we'll get when requested
        self._verboseUpdated = None # when the verbose data was fetched
        

        Group.__init__(self, self._id)

    def _getVerboseInfo(self):
        now = time.time()
        if self._verboseUpdated is None or now - self._verboseUpdated > VERBOSE_REFRESH_TIME:
            # resynch the verbose data
            req = '/?verbose=1'
            rsp_json = self.GET(req)
            props = {}
            for k in ("num_chunks", "num_datatypes", "num_groups", "num_datasets", "allocated_bytes"):
                if k in rsp_json:
                    props[k] = rsp_json[k]
            self._verboseInfo = props
            self._verboseUpdated = now
        return self._verboseInfo

    @property
    def num_chunks(self):
        props = self._getVerboseInfo()
        num_chunks = 0
        if "num_chunks" in props:
            num_chunks = props["num_chunks"]
        return num_chunks

    @property
    def num_datatypes(self):
        props = self._getVerboseInfo()
        num_datatypes = 0
        if "num_datatypes" in props:
            num_datatypes = props["num_datatypes"]
        return num_datatypes

    @property
    def num_groups(self):
        props = self._getVerboseInfo()
        num_groups = 0
        if "num_groups" in props:
            num_groups = props["num_groups"]
        return num_groups

    @property
    def num_datasets(self):
        props = self._getVerboseInfo()
        num_datasets = 0
        if "num_datasets" in props:
            num_datasets = props["num_datasets"]
        return num_datasets

    @property
    def allocated_bytes(self):
        props = self._getVerboseInfo()
        allocated_bytes = 0
        if "allocated_bytes" in props:
            allocated_bytes = props["allocated_bytes"]
        return allocated_bytes


    # override base implemention of ACL methods to use the domain rather than update root group
    def getACL(self, username):
        req = '/acls/' + username
        rsp_json = self.GET(req)
        acl_json = rsp_json["acl"]
        return acl_json

    def getACLs(self):
        req = '/acls'
        rsp_json = self.GET(req)
        acls_json = rsp_json["acls"] 
        return acls_json

    def putACL(self, acl):
        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            if k not in acl:
                raise IOError(404, "Missing ACL field: {}".format(k))
            perm[k] = acl[k]
         
        req = '/acls/' + acl['userName']
        self.PUT(req, body=perm)

    def close(self):
        """ Clears reference to remote resource.
        """
        # this will close the socket of the http_conn singleton
        self._id._http_conn.close()   
        self._id.close()

    def flush(self):
        """ For h5py compatibility, doesn't currently do anything in h5pyd.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.id:
            self.close()

    def __repr__(self):
        if not self.id:
            r = six.u('<Closed HDF5 file>')
        else:
            # Filename has to be forced to Unicode if it comes back bytes
            # Mode is always a "native" string
            filename = self.filename
            if isinstance(filename, bytes):  # Can't decode fname
                filename = filename.decode('utf8', 'replace')
            r = (six.u('<HDF5 file "%s" (mode %s)>') %
                 (os.path.basename(filename), self.mode))

        if six.PY3:
            return r
        return r.encode('utf8')
