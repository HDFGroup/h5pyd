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
import sys
import numpy as np
import logging
from collections.abc import (
    Mapping, MutableMapping, KeysView, ValuesView, ItemsView
)
from datetime import datetime

from h5json.hdf5dtype import Reference
from h5json.h5writer import H5NullWriter
from .objectid import GroupID

numpy_integer_types = (np.int8, np.uint8, np.int16, np.int16, np.int32, np.uint32, np.int64, np.uint64)
numpy_float_types = (np.float16, np.float32, np.float64)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class LinkCreationPropertyList(object):
    """
        Represents a LinkCreationPropertyList
    """
    def __init__(self, char_encoding=None):
        if char_encoding:
            if char_encoding not in ("CSET_ASCII", "CSET_UTF8"):
                raise ValueError("Unknown encoding")
            self._char_encoding = char_encoding
        else:
            self._char_encoding = "CSET_ASCII"

    def __repr__(self):
        return "<HDF5 LinkCreationPropertyList>"

    @property
    def char_encoding(self):
        return self._char_encoding


class LinkAccessPropertyList(object):
    """
        Represents a LinkAccessPropertyList
    """

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

    @property
    def file(self):
        """ Return a File instance associated with this object """
        from .files import File
        db = self._id.db
        root_id = db.root_id
        group_json = db.getObjectById(root_id)

        groupid = GroupID(None, root_id, obj_json=group_json, db=db)

        return File(groupid)

    @property
    def name(self):
        """ Return the full name of this object.  None if anonymous. """

        obj_name = None
        try:
            obj_name = self._name
        except AttributeError:
            # name hasn't been assigned yet
            obj_json = self.id.obj_json
            if "alias" in obj_json:
                alias = obj_json["alias"]

                if len(alias) > 0:
                    obj_name = alias[0]
                    self._name = obj_name

        return obj_name

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
        return Reference(self.id.uuid)
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

        timestamp = self.id.modified
        if timestamp:
            dt = datetime.fromtimestamp(timestamp)
        else:
            dt = None

        return dt

    @property
    def created(self):
        """create time as a datetime object"""

        timestamp = self.id.created
        if timestamp:
            dt = datetime.fromtimestamp(timestamp)
        else:
            dt = None

        return dt

    @property
    def track_order(self):
        return self._track_order

    @property
    def read_only(self):
        if isinstance(self.id.db.writer, H5NullWriter):
            return True
        else:
            return False

    @property
    def creation_properties(self):
        db = self.id.db
        obj_json = db.getObjectById(self.id.uuid)
        if "creationProperties" in obj_json:
            return obj_json["creationProperties"]
        return {}

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

    def refresh(self):
        """ get the latest obj_json data from server """

        # get the latest version of the object
        self.id.db.getObjectById(self.id.uuid, refresh=True)

    def flush(self):
        """ persist any recent changes to the object """

        # TBD: this actually flushes all objects in the file,
        #    update hdf5-json hdf5db to take an optional id arg?
        self.id.db.flush()

    def __init__(self, oid, file=None, track_order=None):
        """ Setup this object, given its low-level identifier """
        self._id = oid
        self.log = self._id.db.log
        self.req_prefix = None  # derived class should set this to the URI of the object
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

        if track_order is None:
            # set order based on creation props
            obj_json = self.id.obj_json
            if "creationProperties" in obj_json:
                cpl = obj_json["creationProperties"]
            else:
                cpl = {}
            if "CreateOrder" in cpl:
                createOrder = cpl["CreateOrder"]
                if not createOrder or createOrder == "0":
                    self._track_order = False
                else:
                    self._track_order = True
            else:
                self._track_order = False
        else:
            self._track_order = track_order

    def __hash__(self):
        return hash(self.id.id)

    def __eq__(self, other):
        if hasattr(other, 'id'):
            return self.id == other.id
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __bool__(self):
        return bool(self.id)


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
        for key in self._mapping:
            if value == self._mapping.get(key):
                return True
        return False

    def __iter__(self):
        for key in self._mapping:
            yield self._mapping.get(key)


class ItemsViewHDF5(ItemsView):

    """
        Wraps e.g. a Group or AttributeManager to provide an items view.
    """

    def __contains__(self, item):
        key, val = item
        if key in self._mapping:
            return val == self._mapping.get(key)
        return False

    def __iter__(self):
        for key in self._mapping:
            yield (key, self._mapping.get(key))


class MappingHDF5(Mapping):

    """
        Wraps a Group, AttributeManager or DimensionManager object to provide
        an immutable mapping interface.

        We don't inherit directly from MutableMapping because certain
        subclasses, for example DimensionManager, are read-only.
    """

    def keys(self):
        """ Get a view object on member names """
        return KeysView(self)

    def values(self):
        """ Get a view object on member objects """
        return ValuesViewHDF5(self)

    def items(self):
        """ Get a view object on member items """
        return ItemsViewHDF5(self)


class MutableMappingHDF5(MappingHDF5, MutableMapping):

    """
        Wraps a Group or AttributeManager object to provide a mutable
        mapping interface, in contrast to the read-only mapping of
        MappingHDF5.
    """

    pass


class Empty(object):

    """
        Proxy object to represent empty/null dataspaces (a.k.a H5S_NULL).
        This can have an associated dtype, but has no shape or data. This is not
        the same as an array with shape (0,).
    """

    shape = None
    size = None

    def __init__(self, dtype):
        self.dtype = np.dtype(dtype)

    def __eq__(self, other):
        if isinstance(other, Empty) and self.dtype == other.dtype:
            return True
        return False

    def __repr__(self):
        return "Empty(dtype={0!r})".format(self.dtype)
