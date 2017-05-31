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

import posixpath
import os
import json
import base64
import logging
import logging.handlers
from collections import (
    Mapping, MutableMapping, KeysView, ValuesView, ItemsView
)
import six
from .objectid import GroupID
from .h5type import Reference


class FakeLock():
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, a, b, c):
        pass

_phil = FakeLock()

# Python alias for access from other modules
phil = _phil


def with_phil(func):
    """ Locking decorator """
    """
    For h5yp source code compatiblity - jlr
    """

    import functools

    def wrapper(*args, **kwds):
        with _phil:
            return func(*args, **kwds)

    functools.update_wrapper(wrapper, func, ('__name__', '__doc__'))
    return wrapper


def guess_dtype(data):
    """ Attempt to guess an appropriate dtype for the object, returning None
    if nothing is appropriate (or if it should be left up the the array
    constructor to figure out)
    """
    """
    # todo
    with phil:
        if isinstance(data, h5r.RegionReference):
            return h5t.special_dtype(ref=h5r.RegionReference)
        if isinstance(data, h5r.Reference):
            return h5t.special_dtype(ref=h5r.Reference)
        if type(data) == bytes:
            return h5t.special_dtype(vlen=bytes)
        if type(data) == six.text_type:
           return h5t.special_dtype(vlen=six.text_type)
    """
    # print("guess_dtype")
    return None


def getHeaders(domain, username=None, password=None, headers=None):
        if headers is None:
            headers = {}
        headers['host'] = domain
        
        if username is not None and password is not None:
            auth_string = username + ':' + password
            auth_string = auth_string.encode('utf-8')
            auth_string = base64.b64encode(auth_string)
            auth_string = b"Basic " + auth_string
            headers['Authorization'] = auth_string
        return headers



class LinkCreationPropertyList(object):
    """
        Represents a LinkCreationPropertyList
    """
    @with_phil
    def __init__(self, char_encoding=None):
        if char_encoding:
            if char_encoding not in ("CSET_ASCII", "CSET_UTF8"):
                raise ValueError("Unknown encoding")
            self._char_encoding = char_encoding
        else:
            self._char_encoding = "CSET_ASCII"

    @with_phil
    def __repr__(self):
        return "<HDF5 LinkCreationPropertyList>"

    @property
    def char_encoding(self):
        return self._char_encoding



class LinkAccessPropertyList(object):
    """
        Represents a LinkAccessPropertyList
    """

    @with_phil
    def __repr__(self):
        return "<HDF5 LinkAccessPropertyList>"

def default_lcpl():
    """ Default link creation property list """
    lcpl = LinkCreationPropertyList()
    return lcpl

def default_lapl():
    """ Default link access property list """
    lapl = LinkAccessPropertyList()
    return lapl

dlapl = default_lapl()
dlcpl = default_lcpl()



class CommonStateObject(object):

    """
        Mixin class that allows sharing information between objects which
        reside in the same HDF5 file.  Requires that the host class have
        a ".id" attribute which returns a low-level ObjectID subclass.

        Also implements Unicode operations.
    """
    

    @property
    def _lapl(self):
        """ Fetch the link access property list appropriate for this object
        """
        return dlapl

    @property
    def _lcpl(self):
        """ Fetch the link creation property list appropriate for this object
        """
        return dlcpl

    def _e(self, name, lcpl=None):
        """ Encode a name according to the current file settings.

        Returns name, or 2-tuple (name, lcpl) if lcpl is True

        - Binary strings are always passed as-is, h5t.CSET_ASCII
        - Unicode strings are encoded utf8, h5t.CSET_UTF8

        If name is None, returns either None or (None, None) appropriately.
        """
        def get_lcpl(coding):
            lcpl = self._lcpl.copy()
            lcpl.set_char_encoding(coding)
            return lcpl

        if name is None:
            return (None, None) if lcpl else None

        if isinstance(name, bytes):
            coding = "CSET_ASCII"
        else:
            try:
                name = name.encode('ascii')
                coding = "CSET_ASCII"
            except UnicodeEncodeError:
                name = name.encode('utf8')
                coding = "CSET_UTF8"

        if lcpl:
            return name, get_lcpl(coding)
        return name

    def _decode(self, item, encoding="ascii"):
        """decode any byte items to python 3 strings
        """
        ret_val = None
        if type(item) is bytes:
            ret_val = item.decode(encoding)
        elif type(item) is list:
            ret_val = []
            for x in item:
                ret_val.append(self._decode(x, encoding))
        elif type(item) is tuple:
            ret_val = []
            for x in item:
                ret_val.append(self._decode(x, encoding))
            ret_val = tuple(ret_val)
        elif type(item) is dict:
            ret_val = {}
            for k in dict:
                ret_val[k] = self._decode(item[k], encoding)
        else:
            ret_val = item
        return ret_val

    def _d(self, name):
        """ Decode a name according to the current file settings.

        - Try to decode utf8
        - Failing that, return the byte string

        If name is None, returns None.
        """
        if name is None:
            return None

        try:
            return name.decode('utf8')
        except UnicodeDecodeError:
            pass
        return name


class _RegionProxy(object):

    """
        Proxy object which handles region references.

        To create a new region reference (datasets only), use slicing syntax:

            >>> newref = obj.regionref[0:10:2]

        To determine the target dataset shape from an existing reference:

            >>> shape = obj.regionref.shape(existingref)

        where <obj> may be any object in the file. To determine the shape of
        the selection in use on the target dataset:

            >>> selection_shape = obj.regionref.selection(existingref)
    """

    def __init__(self, obj):
        self.id = obj.id
        self._name = None

    def __getitem__(self, args):
        pass
        # bases classes will override
         

    def shape(self, ref):
        pass
        

    def selection(self, ref):
        """ Get the shape of the target dataspace selection referred to by *ref*
        """
        pass
         

class ACL(object):

    @property
    def username(self):
        return self._username

    @property
    def create(self):
        return self._create

    @property
    def delete(self):
        return self._delete

    @property
    def read(self):
        return self._read

    @property
    def update(self):
        return self._update


    @property
    def readACL(self):
        return self._readACL

    @property
    def updateACL(self):
        return self._updateACL

     

    """
        Proxy object which handles ACLs (access control list)

    """

    def __init__(self):
        self._username = None
        self._create = True
        self._delete = True
        self._read = True
        self._update = True
        self._readACL = True
        self._updateACL = True


class HLObject(CommonStateObject):

    """
        Base class for high-level interface objects.
        self._name = name
            self._id = root_json['root']
            self._mode = mode
            self._created = root_json['created']
            self._modified = root_json['lastModified']
            self._endpoint = endpoint

    """

    @property
    def file(self):
        """ Return a File instance associated with this object """
        from .files import File
        http_conn = self._id.http_conn
        root_uuid = http_conn.root_uuid
        # construct a group json, so we don't need to do a request
        group_json = {}
        group_json["root"] = root_uuid
        group_json["id"] = root_uuid
        group_json["domain"] = http_conn.domain
        group_json["created"] = http_conn.created
        group_json["lastModified"] = http_conn.modified
    
        groupid = GroupID(None, group_json, http_conn=http_conn)

        return File(groupid)

    @property
    def name(self):
        """ Return the full name of this object.  None if anonymous. """
        return self._name
        # return self._d(h5i.get_name(self.id))

    @property
    def parent(self):
        """Return the parent group of this object.

        This is always equivalent to obj.file[posixpath.dirname(obj.name)].
        ValueError if this object is anonymous.
        """
        if self.name is None:
            raise ValueError("Parent of an anonymous object is undefined")
        return self.file[posixpath.dirname(self.name)]

    @property
    def id(self):
        """ Low-level identifier appropriate for this object """
        return self._id

    @property
    def ref(self):
        """ An (opaque) HDF5 reference to this object """
        return Reference(self)
        # return h5r.create(self.id, b'.', h5r.OBJECT)

    @property
    def regionref(self):
        """Create a region reference (Datasets only).

        The syntax is regionref[<slices>]. For example, dset.regionref[...]
        creates a region reference in which the whole dataset is selected.

        Can also be used to determine the shape of the referenced dataset
        (via .shape property), or the shape of the selection (via the
        .selection property).
        """
        return "todo"
        # return _RegionProxy(self)

    @property
    def attrs(self):
        """ Attributes attached to this object """
        from . import attrs
        return attrs.AttributeManager(self)

    @property
    def modified(self):
        """Last modified time as a datetime object"""
        return self.id._modified

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
      

    def GET(self, req, params=None, format="json"):
        if self.id.http_conn is None:
            raise IOError("object not initialized")
         
        rsp = self.id._http_conn.GET(req, params=params, format=format)
        if rsp.status_code != 200:
            self.log.info("Got response: {}".format(rsp.status_code))
            raise IOError(rsp.status_code, rsp.reason)
        if rsp.headers['Content-Type'] == "application/octet-stream":
            self.log.info("returning binary content, length: " +
                          rsp.headers['Content-Length'])
            return rsp.content
        else:
            # assume JSON
            rsp_json = json.loads(rsp.text)
            return rsp_json

    def PUT(self, req, body=None, params=None, format="json"):
        if self.id.http_conn is None:
            raise IOError("object not initialized")
        
        # try to do a PUT to the domain
        rsp = self._id._http_conn.PUT(req, body=body, params=params, format=format)
         
        if rsp.status_code not in (200, 201):
            if rsp.status_code == 409:
                raise RuntimeError(rsp.reason)
            else:
                raise IOError(rsp.reason)

        if rsp.text:
            rsp_json = json.loads(rsp.text)
            return rsp_json

    def POST(self, req, body=None):
        if self.id.http_conn is None:
            raise IOError("object not initialized")
         
        # try to do a POST to the domain
         
        self.log.info("PST: {} [{}]".format(req, self.id.domain))
         
        rsp = self.id._http_conn.POST(req, body=body)
        if rsp.status_code == 409:
            raise ValueError("name already exists")
        if rsp.status_code not in (200, 201):
            raise IOError(rsp.reason)

        rsp_json = json.loads(rsp.text)
        return rsp_json

    def DELETE(self, req):
        if self.id.http_conn is None:
            raise IOError("object not initialized")
         
        # try to do a DELETE of the resource
        
        self.log.info("DEL: {} [{}]".format(req, self.id.domain))
        rsp = self.id._http_conn.DELETE(req)  
        # self.log.info("RSP: " + str(rsp.status_code) + ':' + rsp.text)
        if rsp.status_code != 200:
            raise IOError(rsp.reason)

    def __init__(self, oid, file=None):
        """ Setup this object, given its low-level identifier """
        self._id = oid
        self.log = self._id.http_conn.logging
        self.req_prefix  = None # derived class should set this to the URI of the object
        self._file = file
        
        if not self.log.handlers:
            # setup logging
            log_path = os.getcwd()
            if not os.access(log_path, os.W_OK):
                log_path = "/tmp"
            log_file = os.path.join(log_path, "h5pyd.log")
            self.log.setLevel(logging.INFO)
            fh = logging.FileHandler(log_file)
            self.log.addHandler(fh)
        else:
            pass

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if hasattr(other, 'id'):
            return self.id == other.id
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __bool__(self):
        with phil:
            return bool(self.id)
    __nonzero__ = __bool__  # Python 2.7 compat

    def getACL(self, username):
        req = self._req_prefix + '/acls/' + username
        rsp_json = self.GET(req)
        acl_json = rsp_json["acl"]
        return acl_json

    def getACLs(self):
        req = self._req_prefix + '/acls'
        rsp_json = self.GET(req)
        acls_json = rsp_json["acls"] 
        return acls_json

    def putACL(self, acl):
        if "userName" not in acl:
            raise IOError("ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            perm[k] = acl[k]
         
        req = self._req_prefix + '/acls/' + acl['userName']
        self.PUT(req, body=perm)
   

# --- Dictionary-style interface ----------------------------------------------

# To implement the dictionary-style interface from groups and attributes,
# we inherit from the appropriate abstract base classes in collections.
#
# All locking is taken care of by the subclasses.
# We have to override ValuesView and ItemsView here because Group and
# AttributeManager can only test for key names.


class ValuesViewHDF5(ValuesView):

    """
        Wraps e.g. a Group or AttributeManager to provide a value view.

        Note that __contains__ will have poor performance as it has
        to scan all the links or attributes.
    """

    def __contains__(self, value):
        with phil:
            for key in self._mapping:
                if value == self._mapping.get(key):
                    return True
            return False

    def __iter__(self):
        with phil:
            for key in self._mapping:
                yield self._mapping.get(key)


class ItemsViewHDF5(ItemsView):

    """
        Wraps e.g. a Group or AttributeManager to provide an items view.
    """

    def __contains__(self, item):
        with phil:
            key, val = item
            if key in self._mapping:
                return val == self._mapping.get(key)
            return False

    def __iter__(self):
        with phil:
            for key in self._mapping:
                yield (key, self._mapping.get(key))


class MappingHDF5(Mapping):

    """
        Wraps a Group, AttributeManager or DimensionManager object to provide
        an immutable mapping interface.

        We don't inherit directly from MutableMapping because certain
        subclasses, for example DimensionManager, are read-only.
    """

    if six.PY3:
        def keys(self):
            """ Get a view object on member names """
            return KeysView(self)

        def values(self):
            """ Get a view object on member objects """
            return ValuesViewHDF5(self)

        def items(self):
            """ Get a view object on member items """
            return ItemsViewHDF5(self)

    else:
        def keys(self):
            """ Get a list containing member names """
            with phil:
                return list(self)

        def values(self):
            """ Get a list containing member objects """
            with phil:
                return [self.get(x) for x in self]

        def itervalues(self):
            """ Get an iterator over member objects """
            for x in self:
                yield self.get(x)

        def items(self):
            """ Get a list of tuples containing (name, object) pairs """
            with phil:
                return [(x, self.get(x)) for x in self]

        def iteritems(self):
            """ Get an iterator over (name, object) pairs """
            for x in self:
                yield (x, self.get(x))


class MutableMappingHDF5(MappingHDF5, MutableMapping):

    """
        Wraps a Group or AttributeManager object to provide a mutable
        mapping interface, in contrast to the read-only mapping of
        MappingHDF5.
    """

    pass
