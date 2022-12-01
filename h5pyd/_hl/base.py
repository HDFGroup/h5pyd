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
import numpy as np
import logging
import logging.handlers
from collections.abc import (
    Mapping, MutableMapping, KeysView, ValuesView, ItemsView
)
from .objectid import GroupID
from .h5type import Reference, check_dtype, special_dtype

numpy_integer_types = (np.int8, np.uint8, np.int16, np.int16, np.int32, np.uint32, np.int64, np.uint64)
numpy_float_types = (np.float16, np.float32, np.float64)

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
            data.dtype.kind == 'O'
            and not check_dtype(vlen=data.dtype)
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
        """decode any byte items to python 3 strings
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


"""
Convert a list to a tuple, recursively.
Example. [[1,2],[3,4]] -> ((1,2),(3,4))
"""
# TBD: this was cut & pasted from attrs.py
def toTuple(rank, data):
    if type(data) in (list, tuple):
        if rank > 0:
            return list(toTuple(rank-1, x) for x in data)
        else:
            return tuple(toTuple(rank-1, x) for x in data)
    else:
        return data

"""
Helper - get num elements defined by a shape
"""
def getNumElements(dims):
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

"""
Copy JSON array into given numpy array
"""
def copyToArray(arr, rank, index, data, vlen_base=None):

    nlen = arr.shape[rank]
    if len(data) != nlen:
        raise ValueError("Array len of {} at index: {} doesn't match data length: {}".format(nlen, index, len(data)))
    for i in range(nlen):
        index[rank] = i
        if rank < len(arr.shape) - 1:
            # recursive call
            copyToArray(arr, rank+1, index, data[i], vlen_base=vlen_base)
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
    if (data_dtype.names is not None and
            data_dtype.names == ('r', 'i') and
            all(dt.kind == 'f' for dt, off in data_dtype.fields.values()) and
            data_dtype.fields['r'][0] == data_dtype.fields['i'][0]):
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
            msg += " Expected {}, but received: {}".format(npoints, arr.size)
            raise ValueError(msg)
        if arr.shape != data_shape:
            arr = arr.reshape(data_shape)  # reshape to match selection

    return arr

"""
Return True if the type contains variable length elements
"""
def isVlen(dt):
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

"""
Get number of byte needed for given element as a bytestream
"""
def getElementSize(e, dt):
    # print(f"getElementSize - e: {e}  dt: {dt} itemsize: {dt.itemsize}")
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
        if isinstance(e, int):
            if e == 0:
                count = 4  # non-initialized element
            else:
                raise ValueError(f"Unexpected value: {e}")
        elif isinstance(e, bytes):
            count = len(e) + 4
        elif isinstance(e, str):
            count = len(e.encode('utf-8')) + 4
        elif isinstance(e, np.ndarray):
            nElements = int(np.prod(e.shape))
            if e.dtype.kind != 'O':
                count = e.dtype.itemsize * nElements
            else:
                count = nElements * vlen.itemsize # tbd - special case for strings?
            count += 4  # byte count
        elif isinstance(e, list) or isinstance(e, tuple):
            if not e:
                # empty list, just add byte count
                count = 4
            else:
                count = len(e) * vlen.itemsize + 4  # +4 for byte count
        else:

            raise TypeError(f"unexpected type: {type(e)}")
    return count


"""
Get number of bytes needed to store given numpy array as a bytestream
"""
def getByteArraySize(arr):
    if not isVlen(arr.dtype) and arr.dtype.kind != 'O':
        print("not isVlen")
        return arr.itemsize * np.prod(arr.shape)
    nElements = int(np.prod(arr.shape))
    # reshape to 1d for easier iteration
    arr1d = arr.reshape((nElements,))
    dt = arr1d.dtype
    count = 0
    for e in arr1d:
        count += getElementSize(e, dt)
    return count

"""
Copy to buffer at given offset
"""
def copyBuffer(src, des, offset):
    for i in range(len(src)):
        des[i+offset] = src[i]

    return offset + len(src)

"""
Copy element to bytearray
"""
def copyElement(e, dt, buffer, offset, vlen=None):
    if vlen is None and dt.metadata and "vlen" in dt.metadata:
        vlen = dt.metadata["vlen"]
    if len(dt) > 1:
        for name in dt.names:
            field_dt = dt[name]
            field_val = e[name]
            offset = copyElement(field_val, field_dt, buffer, offset)
    elif not vlen:
        #print("e no vlen: {} type: {}".format(e, type(e)))
        e_buf = e.tobytes()
        if len(e_buf) < dt.itemsize:
            # extend the buffer for fixed size strings
            #print("extending buffer to {}".format(dt.itemsize))
            e_buf_ex = bytearray(dt.itemsize)
            for i in range(len(e_buf)):
                e_buf_ex[i] = e_buf[i]
            e_buf = bytes(e_buf_ex)
        offset = copyBuffer(e_buf, buffer, offset)
    else:
        # variable length element
        if isinstance(e, int):
            if e == 0:
                # write 4-byte integer 0 to buffer
                offset = copyBuffer(b'\x00\x00\x00\x00', buffer, offset)
            else:
                raise ValueError("Unexpected value: {}".format(e))
        elif isinstance(e, bytes):
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

                #for item in arr1d:
                #    offset = copyElement(item, dt, buffer, offset)

        elif isinstance(e, list) or isinstance(e, tuple):
            count = np.int32(len(e) * vlen.itemsize)
            offset = copyBuffer(count.tobytes(), buffer, offset)
            if isinstance(e, np.ndarray):
                arr = e
            else:
                arr = np.asarray(e, dtype=vlen)
            offset = copyBuffer(arr.tobytes(), buffer, offset)

        else:
            raise TypeError("unexpected type: {}".format(type(e)))
        #print("buffer: {}".format(buffer))
    return offset

"""
Get the count value from persisted vlen array
"""
def getElementCount(buffer, offset):
    count_bytes = bytes(buffer[offset:(offset+4)])

    try:
        count = int(np.frombuffer(count_bytes, dtype="<i4"))
    except TypeError as e:
        msg = "Unexpected error reading count value for variable length elemennt: {}".format(e)
        raise TypeError(msg)
    if count < 0:
        # shouldn't be negative
        raise ValueError("Unexpected count value for variable length element")
    if count > 1024*1024*1024:
        # expect variable length element to be between 0 and 1mb
        raise ValueError("Variable length element size expected to be less than 1MB")
    return count


"""
Read element from bytearrray
"""
def readElement(buffer, offset, arr, index, dt):
    # print(f"readElement, offset: {offset}, index: {index} dt: {dt}")

    if len(dt) > 1:
        e = arr[index]
        for name in dt.names:
            field_dt = dt[name]
            offset = readElement(buffer, offset, e, name, field_dt)
    elif not dt.metadata or "vlen" not in dt.metadata:
        count = dt.itemsize
        e_buffer = buffer[offset:(offset+count)]
        offset += count
        try:
            e = np.frombuffer(bytes(e_buffer), dtype=dt)
            arr[index] = e[0]
        except ValueError:
            print(f"ERROR: ValueError setting {e_buffer} and dtype: {dt}")
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
            e_buffer = buffer[offset:(offset+count)]
            offset += count

            if vlen in (bytes, str):
                arr[index] = bytes(e_buffer)
            else:
                try:
                    e = np.frombuffer(bytes(e_buffer), dtype=vlen)
                except ValueError:
                    print("ValueError -- e_buffer:", e_buffer, "dtype:", vlen)
                    raise
                arr[index] = e        

    return offset


"""
Return byte representation of numpy array
"""
def arrayToBytes(arr, vlen=None):
    if not isVlen(arr.dtype) and vlen is None:
        # can just return normal numpy bytestream
        return arr.tobytes()

    nSize = getByteArraySize(arr)
    buffer = bytearray(nSize)
    offset = 0
    nElements = int(np.prod(arr.shape))
    arr1d = arr.reshape((nElements,))
    for e in arr1d:
        offset = copyElement(e, arr1d.dtype, buffer, offset, vlen=vlen)
    return buffer

"""
Create numpy array based on byte representation
"""
def bytesToArray(data, dt, shape):
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
        # construct a group json, so we don't need to do a request
        group_json = {}
        group_json["root"] = root_uuid
        group_json["id"] = root_uuid
        group_json["domain"] = http_conn.domain
        group_json["created"] = http_conn.created
        group_json["lastModified"] = http_conn.modified

        groupid = GroupID(None, group_json, http_conn=http_conn)

        return File(groupid)

    def _getNameFromObjDb(self):
        objdb = self._id._http_conn.getObjDb()

        if not objdb:
            return None

        root_uuid = self._id.http_conn.root_uuid
        objid = self._id.uuid
        self.log.debug("_getNameFromObjDb: find name for: {}".format(objid))
        objids = set()
        objids.add(objid)
        h5path = ""
        while not h5path.startswith("/"):
            found_link = False
            for id in objdb:
                if id == objid:
                    self.log.debug("_getNameFromObjDb - skipping id {} - obj cannot link to itself".format(id))
                    continue
                self.log.debug("_getNameFromObjDb - searching id: {}".format(id))
                if not id.startswith("g-"):
                    continue  # not a group, so no links
                if id in objids:
                    continue  # we've been here already
                obj = objdb[id]
                links = obj["links"]
                for title in links:
                    self.log.debug("_getNameFromObjDb - looking at linK: {}".format(title))
                    link = links[title]
                    if link["class"] != 'H5L_TYPE_HARD':
                        self.log.debug("_getNameFromObjDb - skipping link type: {}".format(link['class']))
                        continue
                    if link["id"] == objid:
                        # found a link to our target
                        found_link = True
                        if not h5path:
                            h5path = title
                        else:
                            h5path = title + '/' + h5path
                        self.log.debug("_getNameFromObjDb - update h5path: {}".format(h5path))
                        objids.add(id)
                        if id == root_uuid:
                            h5path = '/' + h5path  # we got to root
                            self.log.debug("_getNameFromObjDb - found root")
                        else:
                            objid = id
                            self.log.debug("_getNameFromObjDb - now looking for link to: {}".format(objid))
                        break
            if not found_link:
                self.log.info("_getNameFromObjDb - could not find link")
                break
        if h5path.startswith("/"):
            # found path to obj
            self.log.debug("_getNameFromObjDb - returning: {}".format(h5path))
            return h5path
        else:
            self.log.debug("_getNameFromObjDb - could not find path")
            return None


    @property
    def name(self):
        """ Return the full name of this object.  None if anonymous. """
        try:
            obj_name = self._name
        except AttributeError:
            # name hasn't been assigned yet
            obj_name = self._getNameFromObjDb() # pull from the objdb if present
            if obj_name:
                self._name = obj_name  # save this
            if not obj_name:
                # query the server for the name
                self.log.debug("querying server for name to: {}".format(self._id.id))
                req = None
                if self._id.id.startswith("g-"):
                    req = "/groups/" + self._id.id
                elif self._id.id.startswith("d-"):
                    req = "/datasets/" + self._id.id
                elif self._id.id.startswith("t-"):
                    req = "/datatypes/" + self._id
                if req:
                    params=params = {"getalias": 1}
                    self.log.info("sending get alias request for id: {}".format(self._id.id))
                    obj_json = self.GET(req, params, use_cache=False)
                    if "alias" in obj_json:
                        alias = obj_json["alias"]
                        if len(alias) > 0:
                            obj_name = alias[0]
                            self._name = obj_name


        return obj_name
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

    def GET(self, req, params=None, use_cache=True, format="json"):
        if self.id.http_conn is None:
            raise IOError("object not initialized")
        # This should be the default - but explictly set anyway
        headers = {"Accept-Encoding": "deflate, gzip"}
        
        rsp = self.id._http_conn.GET(req, params=params, headers=headers, format=format, use_cache=use_cache)
        if rsp.status_code != 200:
            self.log.info(f"Got response: {rsp.status_code}")
            raise IOError(rsp.status_code, rsp.reason)
        if 'Content-Type' in rsp.headers and rsp.headers['Content-Type'] == "application/octet-stream":
            if 'Content-Length' in rsp.headers:
                # not available when http compression is used
                self.log.info("returning binary content, length: " + rsp.headers['Content-Length'])
            else:
                self.log.info("returning binary content - length unknown")
            HTTP_CHUNK_SIZE=4096
            http_chunks = []
            downloaded_bytes = 0
            for http_chunk in rsp.iter_content(chunk_size=HTTP_CHUNK_SIZE):
                if http_chunk:  # filter out keep alive chunks
                    self.log.info(f"got http_chunk - {len(http_chunk)} bytes")
                    downloaded_bytes += len(http_chunk)
                    http_chunks.append(http_chunk)
            if len(http_chunks) == 0:
                raise IOError("no data returned")
            if len(http_chunks) == 1:
                # can return first and only chunk as response
                rsp_content = http_chunks[0]
            else:
                msg = f"retrieved {len(http_chunks)} http_chunks "
                msg += f" {downloaded_bytes} total bytes"
                self.log.info(msg)
                rsp_content = bytearray(downloaded_bytes)
                index = 0
                for http_chunk in http_chunks:
                    rsp_content[index:(index+len(http_chunk))] = http_chunk
                    index += len(http_chunk)
            return rsp_content
        else:
            # assume JSON
            rsp_json = json.loads(rsp.text)
            self.log.debug("rsp_json: {}".format(rsp_json))
            return rsp_json


    def PUT(self, req, body=None, params=None, format="json", replace=False):
        if self.id.http_conn is None:
            raise IOError("object not initialized")

        # try to do a PUT to the domain
        rsp = self._id._http_conn.PUT(req, body=body, params=params, format=format)
        self.log.info("PUT rsp status_code: {}".format(rsp.status_code))

        if rsp.status_code not in (200, 201, 204):
            if rsp.status_code == 409:
                # Conflict error
                if replace:
                    self.log.info("replacing resource: {}".format(req))
                    rsp = self.id._http_conn.DELETE(req)
                    if rsp.status_code != 200:
                        raise IOError(rsp.reason)
                    rsp = self._id._http_conn.PUT(req, body=body, params=params, format=format)
                    if rsp.status_code not in (200, 201):
                        raise IOError(rsp.reason)
                else:
                    raise RuntimeError(rsp.reason)
            else:
                raise IOError(rsp.reason)

        if rsp.text:
            rsp_json = json.loads(rsp.text)
            return rsp_json

    def POST(self, req, body=None, format="json"):
        if self.id.http_conn is None:
            raise IOError("object not initialized")

        # try to do a POST to the domain

        self.log.info("POST: {} [{}]".format(req, self.id.domain))

        rsp = self.id._http_conn.POST(req, body=body, format=format)
        if rsp.status_code == 409:
            raise ValueError("name already exists")
        if rsp.status_code not in (200, 201):
            self.log.error("POST error - status_code: {}, reason: {}".format(rsp.status_code, rsp.reason))
            raise IOError(rsp.reason)

        if 'Content-Type' in rsp.headers and rsp.headers['Content-Type'] == "application/octet-stream":
            if 'Content-Length' in rsp.headers:
                # not available when http compression is used
                self.log.info("returning binary content, length: " + rsp.headers['Content-Length'])
            else:
                self.log.info("returning binary compressed content")
            return rsp.content
        else:
            # assume JSON
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
        #self._name = None

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
        return hash(self.id.id)

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
