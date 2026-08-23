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
    Implements high-level operations for attributes.

    Provides the AttributeManager class, available on high-level objects
    as <obj>.attrs.
"""

from __future__ import absolute_import

import numpy

from h5json.hdf5dtype import special_dtype, check_dtype, guess_dtype
from h5json.hdf5dtype import Reference, RegionReference
from h5json.array_util import array_for_new_object

from . import base
from .base import Empty
from .datatype import Datatype


class AttributeManager(base.MutableMappingHDF5, base.CommonStateObject):

    """
        Allows dictionary-style access to an HDF5 object's attributes.

        These are created exclusively by the library and are available as
        a Python attribute at <object>.attrs

        Like Group objects, attributes provide a minimal dictionary-
        style interface.  Anything which can be reasonably converted to a
        Numpy array or Numpy scalar can be stored.

        Attributes are automatically created on assignment with the
        syntax <obj>.attrs[name] = value, with the HDF5 type automatically
        deduced from the value.  Existing attributes are overwritten.

        To modify an existing attribute while preserving its type, use the
        method modify().  To specify an attribute of a particular type and
        shape, use create().
    """

    def __init__(self, parent):
        """ Private constructor.
        """
        self._parent = parent
        self._attributes = self._parent.id.db.getAttributes(self._parent.id.uuid)

    def _bytesArrayToList(self, data):
        """
        Convert list that may contain bytes type elements to list of string
        elements
        """
        text_types = (bytes, str)
        if isinstance(data, text_types):
            is_list = False
        elif isinstance(data, (numpy.ndarray, numpy.generic)):
            if len(data.shape) == 0:
                is_list = False
                data = data.tolist()  # tolist will return a scalar in this case
                if type(data) in (list, tuple):
                    is_list = True
                else:
                    is_list = False
            else:
                is_list = True
        elif isinstance(data, list) or isinstance(data, tuple):
            is_list = True
        else:
            is_list = False

        if is_list:
            out = []
            for item in data:
                out.append(self._bytesArrayToList(item))  # recursive call
        elif isinstance(data, bytes):
            out = data.decode("utf-8")
        else:
            out = data

        return out

    def __getitem__(self, name):
        """ Read the value of an attribute.
        """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        attr_json = self._parent.id.db.getAttribute(self._parent.id.uuid, name)

        if attr_json is None:
            raise KeyError

        shape_json = attr_json["shape"]
        if shape_json["class"] == "H5S_NULL":
            # null space object, return an Empty instance
            dtype = self._parent.id.db.getDtype(attr_json)
            return Empty(dtype)

        obj_id = self._parent.id.uuid

        arr = self._parent.id.db.getAttributeValue(obj_id, name)

        if arr is None:
            # attribute not found
            raise KeyError

        dtype = arr.dtype
        shape = arr.shape

        # HDF5 has no native complex type - h5json represents complex
        # numbers as a compound with 'r'/'i' float fields (see create()) -
        # convert back to a genuine complex dtype on read.
        if dtype.names == ("r", "i") and all(dtype[n].kind == "f" for n in ("r", "i")) \
                and dtype["r"] == dtype["i"]:
            byteorder = dtype["r"].byteorder
            if dtype.itemsize == 16:
                complex_dt = numpy.dtype('c16').newbyteorder(byteorder)
                arr = arr.view(complex_dt).reshape(shape)
                dtype = arr.dtype
            elif dtype.itemsize == 8:
                complex_dt = numpy.dtype('c8').newbyteorder(byteorder)
                arr = arr.view(complex_dt).reshape(shape)
                dtype = arr.dtype

        # NumPy doesn't support top-level array types, so we have to "fake"
        # the correct type and shape for the array.  For example, consider
        # attr.shape == (5,) and attr.dtype == '(3,)f'. Then:
        if dtype.subdtype is not None:
            subdtype, subshape = dtype.subdtype
            shape = shape + subshape   # (5, 3)
            dtype = subdtype           # 'f'
            self.log.warning(f"attr.__getitem__, convert arr to shape: {shape} and dtype: {dtype}")

        if len(arr.shape) == 0:
            v = arr[()]
            if check_dtype(ref=dtype) is Reference:
                if not v:
                    return None  # null reference
                if isinstance(v, bytes):
                    v = v.decode("utf-8")

                if isinstance(v, Reference):
                    ref = v
                else:
                    ref = Reference(v)
                return ref
            if check_dtype(ref=dtype) is RegionReference:
                if not v:
                    return None  # null reference
                if isinstance(v, RegionReference):
                    return v
                return RegionReference.frombytes(v)
            if isinstance(v, str):
                # if this is not utf-8, return bytes instead
                try:
                    v.encode("utf-8")
                except UnicodeEncodeError:
                    self._parent.log.debug("converting utf8 un-encodable string as bytes")
                    v = v.encode("utf-8", errors="surrogateescape")
            elif isinstance(v, bytes) and check_dtype(vlen=dtype) in (str, bytes):
                # HDF5 doesn't enforce that the declared charset (e.g. ASCII)
                # matches what's actually stored, so decode with
                # surrogateescape to preserve any byte exactly - matching
                # h5py's behavior of always returning attribute vlen
                # strings as `str`, regardless of the declared charset.
                # (A *fixed-length* bytes attribute isn't a vlen string at
                # all, and always stays bytes, matching h5py.)
                v = v.decode("utf-8", errors="surrogateescape")
            return v

        # For vlen string/bytes types, convert 0-d array elements to Python strings
        vlen_base_class = check_dtype(vlen=dtype)
        if vlen_base_class in (str, bytes):
            for i in range(arr.size):
                if isinstance(arr.flat[i], numpy.ndarray) and arr.flat[i].shape == ():
                    val = arr.flat[i][()]
                    if isinstance(val, bytes):
                        val = val.decode("utf-8")
                    arr.flat[i] = str(val)

        if check_dtype(ref=dtype) is RegionReference:
            for i in range(arr.size):
                val = arr.flat[i]
                if isinstance(val, bytes):
                    arr.flat[i] = RegionReference.frombytes(val) if val else None

        return arr

    def __setitem__(self, name, value):
        """ Set a new attribute, overwriting any existing attribute.

        The type and shape of the attribute are determined from the data.  To
        use a specific type or shape, or to preserve the type of an attribute,
        use the methods create() and modify().
        """
        self.create(name, data=value, dtype=guess_dtype(value))

    def __delitem__(self, name):
        """ Delete an attribute (which must already exist). """

        if isinstance(name, bytes):
            name = name.decode("utf-8")

        self._parent.id.db.deleteAttribute(self._parent.id.uuid, name)

    def create(self, name, data, shape=None, dtype=None):
        """ Create new attribute, overwriting any existing attributes.

        name
            Name of the new attribute (required)
        data
            Array to initialize the attribute (required)
        shape
            Shape of the attribute.  Overrides data.shape if both are
            given, in which case the total number of points must be unchanged.
        dtype
            Data type of the attribute.  Overrides data.dtype if both
            are given.
        """
        self._parent.log.info(f"attrs.create({name})")

        if not isinstance(name, str):
            raise TypeError(f"attribute name must be a string, got {type(name)}")

        if self._parent.read_only:
            raise IOError("No write intent")

        obj_id = self._parent.id.uuid

        # First, make sure we have a NumPy array.  We leave the data
        # type conversion for HDF5 to perform.  Unlike a raw Python str/list/
        # tuple (auto-converted to vlen str/bytes below), an already-built
        # numpy array keeps its own dtype as-is - e.g. a 'U'-kind array is
        # *not* auto-converted, matching h5py (HDF5 has no equivalent type,
        # so it's caught later as a TypeError instead).
        if isinstance(data, Reference):
            dtype = special_dtype(ref=Reference)
        elif isinstance(data, RegionReference):
            dtype = special_dtype(ref=RegionReference)
        if not isinstance(data, Empty):
            data = array_for_new_object(data, specified_dtype=dtype)
            if data.dtype.kind == "U":
                raise TypeError("Fixed-length unicode data is not supported")

            # HDF5 stores vlen strings as null-terminated C strings, so an
            # embedded NULL would silently truncate the value - reject it
            # up front instead, matching h5py.
            vlen_class = check_dtype(vlen=data.dtype)
            if vlen_class in (bytes, str):
                for elem in data.flat:
                    raw = elem if isinstance(elem, bytes) else elem.encode("utf-8", errors="surrogateescape")
                    if b"\x00" in raw:
                        raise ValueError("VLEN strings do not support embedded NULLs")

        if shape is None:
            if not isinstance(data, Empty):
                shape = data.shape
        elif isinstance(shape, int):
            shape = (shape,)

        use_htype = None  # If a committed type is given, we must use it in h5a.create.

        if isinstance(dtype, Datatype):
            use_htype = "datatypes:/" + dtype.id.uuid
            dtype = dtype.dtype

            # Special case if data are complex numbers
            is_complex = (data.dtype.kind == 'c') and (dtype.names is None) or (
                dtype.names != ('r', 'i')) or (
                any(dt.kind != 'f' for dt, off in dtype.fields.values())) or (
                dtype.fields['r'][0] == dtype.fields['i'][0])

            if is_complex:
                raise TypeError(f'Wrong committed datatype for complex numbers: {dtype.name}')
        elif dtype is None:
            dtype = data.dtype
        else:
            dtype = numpy.dtype(dtype)  # In case a string, e.g. 'i8' is passed

        if not use_htype and dtype.kind == 'c':
            # HDF5 has no native complex type - h5json represents complex
            # numbers as a compound with 'r'/'i' float fields (matching
            # h5py's own convention), so convert both the dtype and the
            # underlying data the same way before handing off.
            if dtype.itemsize == 8:
                float_dt = numpy.dtype('f4').newbyteorder(dtype.byteorder)
            elif dtype.itemsize == 16:
                float_dt = numpy.dtype('f8').newbyteorder(dtype.byteorder)
            else:
                raise TypeError(f"Unsupported dtype for complex numbers: {dtype}")
            compound_dt = numpy.dtype([('r', float_dt), ('i', float_dt)])
            data = data.view(compound_dt)
            dtype = compound_dt

        # Any top-level array type (dtype.subdtype), or shape/data-shape
        # mismatch, is validated and unpacked by Hdf5db.createAttribute()
        # itself - it needs the original (un-reshaped) data and the
        # original (possibly array-typed) dtype/shape to do that correctly.

        # Make HDF5 datatype and dataspace for the H5A calls
        if use_htype:
            dtype = use_htype

        if isinstance(data, Empty):
            data = None  # hdf5db doesn't know about the empty object
            shape = "H5S_NULL"

        self._parent.id.db.createAttribute(obj_id, name, data, shape=shape, dtype=dtype)

    def modify(self, name, value):
        """ Change the value of an attribute while preserving its type.

        Differs from __setitem__ in that if the attribute already exists, its
        type is preserved.  This can be very useful for interacting with
        externally generated files.

        If the attribute doesn't exist, it will be automatically created.
        """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if name not in self:
            self[name] = value
            return

        obj_id = self._parent.id.uuid
        attr_json = self._parent.id.db.getAttribute(obj_id, name)
        shape_json = attr_json["shape"]

        if shape_json["class"] == "H5S_NULL":
            raise IOError("Empty attributes can't be modified")
        elif shape_json["class"] == "H5S_SCALAR":
            shape = ()
        else:
            shape = tuple(shape_json["dims"])

        dtype = self._parent.id.db.getDtype(attr_json)

        # If the input data is already an array, let dtype conversion happen
        # naturally; otherwise coerce to the existing attribute's dtype so
        # its type is preserved.
        dt = None if isinstance(value, numpy.ndarray) else dtype
        value = numpy.asarray(value, order='C', dtype=dt)

        # Allow the case of () <-> (1,)
        if value.shape != shape and not (value.size == 1 and numpy.prod(shape) == 1):
            raise TypeError("Shape of data is incompatible with existing attribute")

        self.create(name, data=value, shape=shape, dtype=dtype)

    def __len__(self):
        """ Number of attributes attached to the object. """

        obj_id = self._parent.id.uuid
        names = self._parent.id.db.getAttributes(obj_id)
        return len(names)

    def __iter__(self):
        """ Iterate over the names of attributes. """
        obj_id = self._parent.id.uuid
        attrs = self._parent.id.db.getAttributes(obj_id)

        def _get_created(name):
            attr_json = self._parent.id.db.getAttribute(obj_id, name)
            return attr_json["created"]

        track_order = None
        if self._parent._track_order is not None:
            track_order = self._parent._track_order
        elif self._parent.id.create_order is not None:
            track_order = self._parent.id.create_order
        else:
            track_order = False

        if track_order:
            attrs = sorted(attrs, key=lambda x: _get_created(x))
        else:
            attrs = sorted(attrs)

        for name in attrs:
            yield name

    def __contains__(self, name):
        """ Determine if an attribute exists, by name. """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        obj_id = self._parent.id.uuid
        attrs = self._parent.id.db.getAttributes(obj_id)
        if name in attrs:
            return True
        else:
            return False

    def __repr__(self):
        if not self._parent.id.id:
            return "<Attributes of closed HDF5 object>"
        return f"<Attributes of HDF5 object at {id(self._parent.id)}>"

    def __reversed__(self):
        """ Iterate over the names of attributes in reverse order. """
        obj_id = self._parent.id.uuid
        attrs = self._parent.id.db.getAttributes(obj_id)

        def _get_created(name):
            attr_json = self._parent.id.db.getAttribute(obj_id, include_data=False)
            return attr_json["created"]

        if self._parent.track_order:
            attrs = sorted(attrs, key=lambda x: _get_created(x))
        else:
            attrs = sorted(attrs)

        for name in reversed(attrs):
            yield name
