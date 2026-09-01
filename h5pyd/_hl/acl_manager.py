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

import logging

# the permission keys an HSDS ACL is made up of - PUT /acls/<username>
# expects exactly these, so they're validated before the request goes out
# rather than letting the server 400 on a partial ACL
ACL_KEYS = ("create", "read", "update", "delete", "readACL", "updateACL")


class ACLManager:
    """ Read and write domain ACLs over the HDF REST API.

    Both Folder and HsdsPlugin expose getACL/getACLs/putACL and used to
    carry their own copy of this logic; this holds the one copy. Neither
    owns the connection, so rather than taking an HttpConn the manager
    takes a zero-argument callable returning the live one - the caller
    decides what "not open" means for it and raises accordingly, and a
    connection reopened underneath the manager is still picked up.
    """

    def __init__(self, get_http_conn, log=None):
        self._get_http_conn = get_http_conn
        if log is None:
            self.log = logging.getLogger(__name__)
        else:
            self.log = log

    def getACL(self, username):
        """ Return the ACL for the given username. """
        http_conn = self._get_http_conn()
        req = "/acls/" + username
        self.log.debug(f"getACL: {req}")
        rsp = http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = rsp.json()
        if "acl" not in rsp_json:
            raise IOError(500, "Unexpected Error")
        return rsp_json["acl"]

    def getACLs(self):
        """ Return all the ACLs for the domain. """
        http_conn = self._get_http_conn()
        req = "/acls"
        self.log.debug(f"getACLs: {req}")
        rsp = http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = rsp.json()
        if "acls" not in rsp_json:
            raise IOError(500, "Unexpected Error")
        return rsp_json["acls"]

    def putACL(self, acl):
        """ Create or update the ACL for the user named in the given acl. """
        http_conn = self._get_http_conn()
        if http_conn.mode == "r":
            raise IOError(400, "domain is open as read-only")
        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ACL_KEYS:
            if k not in acl:
                raise IOError(404, f"Missing ACL field: {k}")
            perm[k] = acl[k]

        req = "/acls/" + acl["userName"]
        self.log.debug(f"putACL: {req}")
        rsp = http_conn.PUT(req, body=perm)
        if rsp.status_code != 201:
            raise IOError(rsp.status_code, rsp.reason)
