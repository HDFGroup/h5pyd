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
import logging.handlers
from collections.abc import (
    Mapping, MutableMapping, KeysView, ValuesView, ItemsView
)
from .objectid import GroupID, ObjectID
from .h5type import Reference, check_dtype, special_dtype

numpy_integer_types = (np.int8, np.uint8, np.int16, np.int16, np.int32, np.uint32, np.int64, np.uint64)
numpy_float_types = (np.float16, np.float32, np.float64)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


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


def find_item_type(data):
    """Find the item type of a simple object or collection of objects.

    E.g. [[['a']]] -> str

    The focus is on collections where all items have the same type; we'll return
    None if that's not the case.

    The aim is to treat numpy arrays of Python objects like normal Python
    collections, while treating arrays with specific dtypes differently.
    We're also only interested in array-like collections - lists and tuples,
    possibly nested - not things like sets or dicts.
    """
    if isinstance(data, np.ndarray):
        if (
            data.dtype.kind == 'O' and not check_dtype(vlen=data.dtype)
        ):
            item_types = {type(e) for e in data.flat}
        else:
            return None
    elif isinstance(data, (list, tuple)):
        item_types = {find_item_type(e) for e in data}
    else:
        return type(data)

    if len(item_types) != 1:
        return None
    return item_types.pop()


def guess_dtype(data):
    """ Attempt to guess an appropriate dtype for the object, returning None
    if nothing is appropriate (or if it should be left up the the array
    constructor to figure out)
    """

    # todo - handle RegionReference, Reference
    item_type = find_item_type(data)
    if item_type is bytes:
        return special_dtype(vlen=bytes)
    if item_type is str:
        return special_dtype(vlen=str)

    return None


def is_float16_dtype(dt):
    if dt is None:
        return False

    dt = np.dtype(dt)  # normalize strings -> np.dtype objects
    return dt.kind == 'f' and dt.itemsize == 2


def array_for_new_object(data, specified_dtype=None):
    """Prepare an array from data used to create a new dataset or attribute"""

    # We mostly let HDF5 convert data as necessary when it's written.
    # But if we are going to a float16 datatype, pre-convert in python
    # to workaround a bug in the conversion.
    # https://github.com/h5py/h5py/issues/819
    if is_float16_dtype(specified_dtype):
        as_dtype = specified_dtype
    elif not isinstance(data, np.ndarray) and (specified_dtype is not None):
        # If we need to convert e.g. a list to an array, don't leave numpy
        # to guess a dtype we already know.
        as_dtype = specified_dtype
    else:
        as_dtype = guess_dtype(data)

    data = np.asarray(data, order="C", dtype=as_dtype)

    # In most cases, this does nothing. But if data was already an array,
    # and as_dtype is a tagged h5py dtype (e.g. for an object array of strings),
    # asarray() doesn't replace its dtype object. This gives it the tagged dtype:
    if as_dtype is not None:
        data = data.view(dtype=as_dtype)

    return data


def _decode(item, encoding="ascii"):
    """
    decode any byte items to python 3 strings
    """
    ret_val = None
    if type(item) is bytes:
        ret_val = item.decode(encoding)
    elif type(item) is list:
        ret_val = []
        for x in item:
            ret_val.append(_decode(x, encoding))
    elif type(item) is tuple:
        ret_val = []
        for x in item:
            ret_val.append(_decode(x, encoding))
        ret_val = tuple(ret_val)
    elif type(item) is dict:
        ret_val = {}
        for k in dict:
            ret_val[k] = _decode(item[k], encoding)
    elif type(item) is np.ndarray:
        x = item.tolist()
        ret_val = []
        for x in item:
            ret_val.append(_decode(x, encoding))
    elif type(item) in numpy_integer_types:
        ret_val = int(item)
    elif type(item) in numpy_float_types:
        ret_val = float(item)
    else:
        ret_val = item
    return ret_val


# TBD: this was cut & pasted from attrs.py
def toTuple(rank, data):
    """
    Convert a list to a tuple, recursively.
    Example. [[1,2],[3,4]] -> ((1,2),(3,4))
    """
    if type(data) in (list, tuple):
        if rank > 0:
            return list(toTuple(rank - 1, x) for x in data)
        else:
            return tuple(toTuple(rank - 1, x) for x in data)
    else:
        return data


def getNumElements(dims):
    """
    Helper - get num elements defined by a shape
    """
    num_elements = 0
    if isinstance(dims, int):
        num_elements = dims
    elif isinstance(dims, (list, tuple)):
        num_elements = 1
        for dim in dims:
            num_elements *= dim
    else:
        raise ValueError("Unexpected argument")
    return num_elements


def copyToArray(arr, rank, index, data, vlen_base=None):
    """
    Copy JSON array into given numpy array
    """
    nlen = arr.shape[rank]
    if len(data) != nlen:
        msg = f"Array len of {nlen} at index: {index} doesn't match data length: {len(data)}"
        raise ValueError(msg)
    for i in range(nlen):
        index[rank] = i
        if rank < len(arr.shape) - 1:
            # recursive call
            copyToArray(arr, rank + 1, index, data[i], vlen_base=vlen_base)
        else:
            if vlen_base:
                if vlen_base == str:
                    e = str(data[i])
                else:
                    e = np.array(data[i], dtype=vlen_base)
                    if len(e.shape) > 1:
                        # squeeze dimensions, but don't convert a 1-d to 0-d
                        e = e.squeeze()
                arr[tuple(index)] = e
            else:
                arr[tuple(index)] = data[i]
    index[rank] = 0


def jsonToArray(data_shape, data_dtype, data_json):
    """Return numpy array from the given json array."""

    # need some special conversion for compound types --
    # each element must be a tuple, but the JSON decoder
    # gives us a list instead.

    # Special case: complex numbers
    is_complex = data_dtype.names is not None and (
        data_dtype.names == ('r', 'i')) and (
        all(dt.kind == 'f' for dt, off in data_dtype.fields.values())) and (
        data_dtype.fields['r'][0] == data_dtype.fields['i'][0])

    if (is_complex):
        itemsize = data_dtype.itemsize
        if itemsize == 16:
            cmplx_dtype = np.dtype(np.complex128)
        elif itemsize == 8:
            cmplx_dtype = np.dtype(np.complex64)
        arr = np.empty(shape=data_shape, dtype=cmplx_dtype)
        if data_shape == ():
            tmp = np.array(tuple(data_json), dtype=data_dtype)
            arr.real = tmp['r']
            arr.imag = tmp['i']
        else:
            data = np.array(data_json)
            tmp = np.empty(shape=data_shape, dtype=data_dtype)
            for i, n in enumerate(data_dtype.names):
                tmp[n] = data[:, i]
            arr.real = tmp['r']
            arr.imag = tmp['i']
        return arr

    if len(data_dtype) > 1 and not isinstance(data_json, (list, tuple)):
        raise TypeError("expected list data for compound data type")

    vlen_base = check_dtype(vlen=data_dtype)
    if vlen_base:
        # for vlen types, convert each element to a ndarray
        arr = np.zeros(data_shape, dtype=data_dtype)
        index = []
        for i in range(len(data_shape)):
            index.append(0)
        if data_shape == ():
            arr[()] = data_json
        else:
            copyToArray(arr, 0, index, data_json, vlen_base=vlen_base)
    else:
        npoints = int(np.prod(data_shape))
        if type(data_json) in (list, tuple):
            np_shape_rank = len(data_shape)
            converted_data = []
            if npoints == 1 and len(data_json) == len(data_dtype):
                converted_data.append(toTuple(0, data_json))
            else:
                converted_data = toTuple(np_shape_rank, data_json)
            data_json = converted_data

        arr = np.array(data_json, dtype=data_dtype)
        # raise an exception of the array shape doesn't match the selection shape
        # allow if the array is a scalar and the selection shape is one element,
        # numpy is ok with this
        if arr.size != npoints:
            msg = "Input data doesn't match selection number of elements"
            msg += f" Expected {npoints}, but received: {arr.size}"
            raise ValueError(msg)
        if arr.shape != data_shape:
            arr = arr.reshape(data_shape)  # reshape to match selection

    return arr


def isVlen(dt):
    """
    Return True if the type contains variable length elements
    """
    is_vlen = False
    if len(dt) > 1:
        names = dt.names
        for name in names:
            if isVlen(dt[name]):
                is_vlen = True
                break
    else:
        if dt.metadata and "vlen" in dt.metadata:
            is_vlen = True
    return is_vlen


def getElementSize(e, dt):
    """
    Get number of byte needed for given element as a bytestream
    """

    if len(dt) > 1:
        count = 0
        for name in dt.names:
            field_dt = dt[name]
            field_val = e[name]
            count += getElementSize(field_val, field_dt)
    elif not dt.metadata or "vlen" not in dt.metadata:
        count = dt.itemsize  # fixed size element
    else:
        # variable length element
        vlen = dt.metadata["vlen"]

        if isinstance(e, bytes):
            count = len(e) + 4
        elif isinstance(e, str):
            count = len(e.encode('utf-8')) + 4
        elif isinstance(e, np.ndarray):
            nElements = int(np.prod(e.shape))
            if e.dtype.kind != 'O':
                count = e.dtype.itemsize * nElements
            else:
                count = nElements * vlen.itemsize
            count += 4  # byte count
        elif isinstance(e, list) or isinstance(e, tuple):
            if not e:
                # empty list, just add byte count
                count = 4
            else:
                count = len(e) * vlen.itemsize + 4  # +4 for byte count
        else:
            # uninitialized element
            if e and not np.isnan(e):
                raise ValueError(f"Unexpected value: {e}")
            else:
                count = 4  # non-initialized element

    return count


def getByteArraySize(arr):
    """
    Get number of bytes needed to store given numpy array as a bytestream
    """
    if not isVlen(arr.dtype) and arr.dtype.kind != 'O':
        # not vlen just return itemsize * number of elements
        return arr.itemsize * np.prod(arr.shape)
    nElements = int(np.prod(arr.shape))
    # reshape to 1d for easier iteration
    arr1d = arr.reshape((nElements,))
    dt = arr1d.dtype
    count = 0
    for e in arr1d:
        count += getElementSize(e, dt)

    return count


def copyBuffer(src, des, offset):
    """
    Copy to buffer at given offset
    """
    for i in range(len(src)):
        des[i + offset] = src[i]

    return offset + len(src)


def copyElement(e, dt, buffer, offset, vlen=None):
    """
    Copy element to bytearray
    """
    if vlen is None and dt.metadata and "vlen" in dt.metadata:
        vlen = dt.metadata["vlen"]
    if len(dt) > 1:
        for name in dt.names:
            field_dt = dt[name]
            field_val = e[name]
            offset = copyElement(field_val, field_dt, buffer, offset)
    elif not vlen:
        # print("e no vlen: {} type: {}".format(e, type(e)))
        e_buf = e.tobytes()
        if len(e_buf) < dt.itemsize:
            # extend the buffer for fixed size strings
            e_buf_ex = bytearray(dt.itemsize)
            for i in range(len(e_buf)):
                e_buf_ex[i] = e_buf[i]
            e_buf = bytes(e_buf_ex)
        offset = copyBuffer(e_buf, buffer, offset)
    else:
        # variable length element
        if isinstance(e, bytes):
            count = np.int32(len(e))
            offset = copyBuffer(count.tobytes(), buffer, offset)
            offset = copyBuffer(e, buffer, offset)
        elif isinstance(e, str):
            if vlen == str:
                encoding = "utf-8"
            else:
                encoding = "ascii"
            text = e.encode(encoding)
            count = np.int32(len(text))
            offset = copyBuffer(count.tobytes(), buffer, offset)
            offset = copyBuffer(text, buffer, offset)

        elif isinstance(e, np.ndarray):
            nElements = int(np.prod(e.shape))
            if e.dtype.kind != 'O':
                count = np.int32(e.dtype.itemsize * nElements)
                offset = copyBuffer(count.tobytes(), buffer, offset)
                offset = copyBuffer(e.tobytes(), buffer, offset)
            else:
                arr1d = e.reshape((nElements,))
                count = np.int32(nElements * vlen.itemsize)
                offset = copyBuffer(count.tobytes(), buffer, offset)
                arr = np.asarray(arr1d, dtype=vlen)
                offset = copyBuffer(arr.tobytes(), buffer, offset)

        elif isinstance(e, list) or isinstance(e, tuple):
            count = np.int32(len(e) * vlen.itemsize)
            offset = copyBuffer(count.tobytes(), buffer, offset)
            if isinstance(e, np.ndarray):
                arr = e
            else:
                arr = np.asarray(e, dtype=vlen)
            offset = copyBuffer(arr.tobytes(), buffer, offset)

        else:
            # uninitialized variable length element
            if e and not np.isnan(e):
                raise ValueError(f"Unexpected value: {e}")
            else:
                # write 4-byte integer 0 to buffer
                offset = copyBuffer(b'\x00\x00\x00\x00', buffer, offset)
        # print("buffer: {}".format(buffer))
    return offset


def getElementCount(buffer, offset):
    """
    Get the count value from persisted vlen array
    """
    count_bytes = bytes(buffer[offset:(offset + 4)])

    try:
        arr = np.frombuffer(count_bytes, dtype="<i4")
        count = int(arr[0])
    except TypeError as e:
        msg = f"Unexpected error reading count value for variable length elemennt: {e}"
        raise TypeError(msg)
    if count < 0:
        # shouldn't be negative
        raise ValueError("Unexpected count value for variable length element")
    if count > 1024 * 1024 * 1024:
        # expect variable length element to be between 0 and 1mb
        raise ValueError("Variable length element size expected to be less than 1MB")
    return count


def readElement(buffer, offset, arr, index, dt):
    """
    Read element from bytearrray
    """
    # print(f"readElement, offset: {offset}, index: {index} dt: {dt}")

    if len(dt) > 1:
        e = arr[index]
        for name in dt.names:
            field_dt = dt[name]
            offset = readElement(buffer, offset, e, name, field_dt)
    elif not dt.metadata or "vlen" not in dt.metadata:
        count = dt.itemsize
        e_buffer = buffer[offset:(offset + count)]
        offset += count
        try:
            e = np.frombuffer(bytes(e_buffer), dtype=dt)
            arr[index] = e[0]
        except ValueError:
            eprint(f"ERROR: ValueError setting {e_buffer} and dtype: {dt}")
            raise
    else:
        # variable length element
        vlen = dt.metadata["vlen"]
        e = arr[index]

        if isinstance(e, np.ndarray):
            nelements = int(np.prod(dt.shape))
            e.reshape((nelements,))
            for i in range(nelements):
                offset = readElement(buffer, offset, e, i, dt)
            e.reshape(dt.shape)
        else:
            count = getElementCount(buffer, offset)
            offset += 4
            if count < 0:
                raise ValueError("Unexpected variable length data format")
            e_buffer = buffer[offset:(offset + count)]
            offset += count

            if vlen in (bytes, str):
                arr[index] = bytes(e_buffer)
            else:
                try:
                    e = np.frombuffer(bytes(e_buffer), dtype=vlen)
                except ValueError:
                    eprint("ValueError -- e_buffer:", e_buffer, "dtype:", vlen)
                    raise
                arr[index] = e

    return offset


def arrayToBytes(arr, vlen=None):
    """
    Return byte representation of numpy array
    """
    if not isVlen(arr.dtype) and vlen is None:
        # can just return normal numpy bytestream
        return arr.tobytes()

    nElements = int(np.prod(arr.shape))
    arr1d = arr.reshape((nElements,))
    nSize = getByteArraySize(arr1d)
    buffer = bytearray(nSize)
    offset = 0

    for e in arr1d:
        offset = copyElement(e, arr1d.dtype, buffer, offset, vlen=vlen)
    return buffer


def bytesToArray(data, dt, shape):
    """
    Create numpy array based on byte representation
    """
    nelements = getNumElements(shape)

    if not isVlen(dt):
        # regular numpy from string
        arr = np.frombuffer(data, dtype=dt)
    else:
        arr = np.zeros((nelements,), dtype=dt)
        offset = 0
        for index in range(nelements):
            offset = readElement(data, offset, arr, index, dt)

    if shape is not None:
        if shape == () and dt.shape:
            # special case for scalar array with array sub-type
            arr = arr.reshape(dt.shape)
        else:
            arr = arr.reshape(shape)
    return arr


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
        http_conn = self._id.http_conn
        root_uuid = http_conn.root_uuid
        groupid = GroupID(root_uuid, http_conn=http_conn)

        return File(groupid)

    @property
    def name(self):
        """ Return the full name of this object.  None if anonymous. """
        return self._name

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
        return attrs.AttributeManager(self.id, track_order=self.track_order)

    @property
    def modified(self):
        """Last modified time as a datetime object"""
        return self.id.modified

    @property
    def track_order(self):
        track_order = self._track_order
        if track_order is None and self.id.cpl.get('CreateOrder'):
            track_order = True
        return track_order

    def __init__(self, oid, track_order=None):
        """ Setup this object, given its low-level identifier """
        if not isinstance(oid, ObjectID):
            raise TypeError(f"unexpected type for HLObject.__init__: {type(oid)}")
        self._id = oid
        self.log = self._id.http_conn.logging
        if self.id.uuid == self.id.http_conn.root_uuid:
            # set the name as the root group
            self._name = "/"
        else:
            # allow super-class to set the name based on how this
            # object was instantiated
            self._name = None

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
