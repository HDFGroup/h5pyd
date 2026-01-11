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
from h5json.hdf5dtype import Reference

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
            if isinstance(v, str):
                # if this is not utf-8, return bytes instead
                try:
                    v.encode("utf-8")
                except UnicodeEncodeError:
                    self._parent.log.debug("converting utf8 un-encodable string as bytes")
                    v = v.encode("utf-8", errors="surrogateescape")
            return v
        return arr

    def __setitem__(self, name, value):
        """ Set a new attribute, overwriting any existing attribute.

        The type and shape of the attribute are determined from the data.  To
        use a specific type or shape, or to preserve the type of an attribute,
        use the methods create() and modify().
        """
        self.create(name, value=value, dtype=guess_dtype(value))

    def __delitem__(self, name):
        """ Delete an attribute (which must already exist). """

        if isinstance(name, bytes):
            name = name.decode("utf-8")

        self._parent.id.db.deleteAttribute(self._parent.id.uuid, name)

    def create(self, name, value, shape=None, dtype=None):
        """ Create new attribute, overwriting any existing attributes.

        name
            Name of the new attribute (required)
        value
            Array to initialize the attribute (required)
        shape
            Shape of the attribute.  Overrides data.shape if both are
            given, in which case the total number of points must be unchanged.
        dtype
            Data type of the attribute.  Overrides data.dtype if both
            are given.
        """
        self._parent.log.info(f"attrs.create({name})")

        if self._parent.read_only:
            raise IOError("No write intent")

        obj_id = self._parent.id.uuid

        # First, make sure we have a NumPy array.  We leave the data
        # type conversion for HDF5 to perform.
        if isinstance(value, Reference):
            dtype = special_dtype(ref=Reference)
        if not isinstance(value, Empty):
            value = numpy.asarray(value, dtype=dtype, order='C')

        if shape is None and not isinstance(value, Empty):
            shape = value.shape

        use_htype = None  # If a committed type is given, we must use it in h5a.create.

        if isinstance(dtype, Datatype):
            use_htype = "datatypes:/" + dtype.id.uuid
            dtype = dtype.dtype

            # Special case if data are complex numbers
            is_complex = (value.dtype.kind == 'c') and (dtype.names is None) or (
                dtype.names != ('r', 'i')) or (
                any(dt.kind != 'f' for dt, off in dtype.fields.values())) or (
                dtype.fields['r'][0] == dtype.fields['i'][0])

            if is_complex:
                raise TypeError(f'Wrong committed datatype for complex numbers: {dtype.name}')
        elif dtype is None:
            if value.dtype.kind == 'U':
                # use vlen for unicode strings
                dtype = special_dtype(vlen=str)
            else:
                dtype = value.dtype
        else:
            dtype = numpy.dtype(dtype)  # In case a string, e.g. 'i8' is passed

        # Where a top-level array type is requested, we have to do some
        # fiddling around to present the data as a smaller array of subarrays.
        if not isinstance(value, Empty):
            if dtype.subdtype is not None:

                subdtype, subshape = dtype.subdtype

                # Make sure the subshape matches the last N axes' sizes.
                if shape[-len(subshape):] != subshape:
                    raise ValueError(f"Array dtype shape {subshape} is incompatible with data shape {shape}")

                # New "advertised" shape and dtype
                shape = shape[0:len(shape) - len(subshape)]
                dtype = subdtype

            # Not an array type; make sure to check the number of elements
            # is compatible, and reshape if needed.
            else:
                if numpy.prod(shape) != numpy.prod(value.shape):
                    raise ValueError("Shape of new attribute conflicts with shape of data")

                if shape != value.shape:
                    value = value.reshape(shape)

                # We need this to handle special string types.

                value = numpy.asarray(value, dtype=dtype)

        # Make HDF5 datatype and dataspace for the H5A calls
        if use_htype:
            dtype = use_htype

        if isinstance(value, Empty):
            value = None  # hdf5db doesn't know about the empty object
            shape = "H5S_NULL"

        self._parent.id.db.createAttribute(obj_id, name, value, shape=shape, dtype=dtype)

    def modify(self, name, value):
        """ Change the value of an attribute while preserving its type.

        Differs from __setitem__ in that if the attribute already exists, its
        type is preserved.  This can be very useful for interacting with
        externally generated files.

        If the attribute doesn't exist, it will be automatically created.
        """
        pass
        # TBD
        """
            if not name in self:
                self[name] = value
            else:
                value = numpy.asarray(value, order='C')

                attr = h5a.open(self._id, self._e(name))

                if attr.get_space().get_simple_extent_type() == h5s.NULL:
                    raise IOError("Empty attributes can't be modified")

                # Allow the case of () <-> (1,)
                if (value.shape != attr.shape) and not \
                   (numpy.prod(value.shape) == 1 and numpy.prod(attr.shape) == 1):
                    raise TypeError("Shape of data is incompatible with existing attribute")
                attr.write(value)
        """

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
            attr_json = self._parent.id.db.getAttribute(obj_id, name, includeData=False)
            return attr_json["created"]

        if self._parent.id.create_order:
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
