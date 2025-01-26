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

"""
    Implements high-level operations for ACLs.

    Provides the  ACLManager class, available on file and folder objects
    as <obj>.acls.
"""

from __future__ import absolute_import

ACL_KEYS = ("create", "read", "update", "delete", "readACL", "updateACL")


class ACL():
    def __init__(self, perm):
        self._acl = perm

    @property
    def create(self):
        return True if self._acl['create'] else False

    @create.setter
    def create(self, v):
        self._acl['create'] = bool(v)

    @property
    def read(self):
        return True if self._acl['read'] else False

    @read.setter
    def read(self, v):
        self._acl['read'] = bool(v)

    @property
    def update(self):
        return True if self._acl['update'] else False

    @update.setter
    def update(self, v):
        self._acl['update'] = bool(v)

    @property
    def delete(self):
        return True if self._acl['delete'] else False

    @delete.setter
    def delete(self, v):
        self._acl['delete'] = bool(v)

    @property
    def readACL(self):
        return True if self._acl['readACL'] else False

    @readACL.setter
    def readACL(self, v):
        self._acl['readACL'] = bool(v)

    @property
    def updateACL(self):
        return True if self._acl['updateACL'] else False

    @updateACL.setter
    def updateACL(self, v):
        self._acl['updateACL'] = bool(v)

    def copy(self):
        acl_copy = ACL(self._acl.copy())
        return acl_copy

    def __repr__(self):
        # repr for full priv ACL: <ACL(crudep)>
        # repr for read-only ACL: <ACL(-r----)>
        perms = []
        perms.append('c') if self.create else perms.append('-')
        perms.append('r') if self.read else perms.append('-')
        perms.append('u') if self.update else perms.append('-')
        perms.append('d') if self.delete else perms.append('-')
        perms.append('e') if self.readACL else perms.append('-')
        perms.append('p') if self.updateACL else perms.append('-')
        return f"<ACL({''.join(perms)})>"


class ACLManager():

    """
        Allows dictionary-style access to an Domain or Folder's ACLs.

        Like Group objects, acls provide a minimal dictionary-
        style interface, key'd by username.  Each acl consist of a set
        of permissions flags for 'create', 'read', 'update', 'delete',
        'readACL', and 'updateACL'.

        To modify an existing ACL, fetch it, set the desired permission
        flags and then set the acl.

        To create a new ACL, get an ACL instance with acls.create_ACL method,
        modify as desired, then set the acl using the desired username.
    """

    def __init__(self, parent):
        """ Private constructor.
        """
        if hasattr(parent, "id"):
            self._parent_type = "Domain"
            self._http_conn = parent.id.http_conn
        else:
            # assume Folder
            self._parent_type = "Folder"
            self._http_conn = parent._http_conn
        self._acls = None

    def refresh(self):
        """ Fetch the current set of ACLs from the server """
        req = "/acls"
        rsp = self._http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, "Unable to get ACLs")
        rsp_json = rsp.json()
        acls = rsp_json["acls"]
        # convert to a dict
        self._acls = {}

        for acl_json in acls:
            user_name = acl_json['userName']
            acl = {}
            for k in ACL_KEYS:
                if k in acl_json:
                    acl_bool = bool(acl_json[k])
                    acl[k] = acl_bool
                else:
                    acl[k] = False
            self._acls[user_name] = acl

    def create_acl(self, c=False, r=False, u=False, d=False, e=False, p=False):
        """ return an ACL with the given flag settings"""
        perm = {"create": c, "read": r, "update": u, "delete": d, "readACL": e, "updateACL": p}
        acl = ACL(perm)
        return acl

    def __getitem__(self, name):
        """ Get the ACL for the given username.
        """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if self._acls is None:
            self.refresh()

        if name not in self._acls:
            raise IOError(404, "Not Found")

        return ACL(self._acls[name])

    def __setitem__(self, name, acl):
        """ Set an ACL, overwriting any existing ACL.
        """

        if not isinstance(acl, ACL):
            raise TypeError("expected ACL instance")

        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if not name or len(name.split()) != 1:
            raise ValueError("name  not valid")

        if self._acls is None:
            self.refresh()

        req = "/acls/" + name
        body = acl._acl
        rsp = self._http_conn.PUT(req, body=body)
        if rsp.status_code not in (200, 201):
            raise IOError(rsp.status_code, "PUT ACL failed")
        self.refresh()

    def __delitem__(self, name):
        """ Delete an ACL (which must already exist). """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if not name or len(name.split()) != 1:
            raise ValueError("name  not valid")

        if self._acls is None:
            self.refresh()

        if name not in self._acls:
            raise IOError(404, "Not Found")

        req = "/acls/" + name

        # TBD: this action is not yet supported in HSDS, so expect an error...
        rsp = self._http_conn.DELETE(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, "DELETE ACL failed")
        self.refresh()

    def __len__(self):
        """ Number of ACLs attached to the domain or folder. """
        if self._acls is None:
            self.refresh()
        return len(self._acls)

    def __contains__(self, name):
        """ Determine if an ACL exists, by name. """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if not name or len(name.split()) != 1:
            raise ValueError("name  not valid")

        if self._acls is None:
            self.refresh()

        return True if name in self._acls else False

    def __repr__(self):
        return f"<ACLs of {self._parent_type}>"

    def _get_acl_names(self):
        if self._acls is None:
            self.refresh()
        return self._acls.keys()

    def __iter__(self):
        """ Iterate over the names of the ACLs. """
        # convert to a list of dicts
        names = self._get_acl_names()
        for name in names:
            yield name

    def __reversed__(self):
        """ Iterate over the names of ACLs in reverse order. """
        names = self._get_acl_names()
        for name in reversed(names):
            yield name
        # done
