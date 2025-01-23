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
import json

from . import base
from .base import jsonToArray, Empty
from .datatype import Datatype
from ..objectid import get_class_for_uuid, GroupID, TypeID, DatasetID
from ..h5type import getTypeItem, createDataType, special_dtype, Reference


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

    def __init__(self, parent, track_order=None):
        """ Private constructor.
        """
        self._parent = parent
        self._track_order = track_order

    @property
    def track_order(self):
        if self._track_order is None:
            return self._parent.track_order
        else:
            return self._track_order

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

        attr_json = self._parent.get_attr(name)

        shape_json = attr_json['shape']
        type_json = attr_json['type']
        dtype = createDataType(type_json)

        # The shape_json may actually be the shape value we passed
        # to the server on PUT attributes rather than the GET response.
        # Finagle the code here to do the right thing in both cases.
        # TBD: Update HSDS to accept the the shape_json as shape
        # parameter so this can be avoided

        if isinstance(shape_json, str):
            # H5S_NULL should be the only possible value
            if shape_json == 'H5S_NULL':
                return Empty(dtype)
            else:
                raise TypeError(f"unexpected attr shape: {shape_json}")
        elif isinstance(shape_json, tuple):
            shape = shape_json
        elif isinstance(shape_json, dict):
            if shape_json['class'] == 'H5S_NULL':
                return Empty(dtype)
            if 'dims' in shape_json:
                shape = shape_json['dims']
            else:
                shape = ()

        value_json = attr_json['value']

        # Do this first, as we'll be fiddling with the dtype for top-level
        # array types
        htype = dtype

        # NumPy doesn't support top-level array types, so we have to "fake"
        # the correct type and shape for the array.  For example, consider
        # attr.shape == (5,) and attr.dtype == '(3,)f'. Then:
        if dtype.subdtype is not None:
            subdtype, subshape = dtype.subdtype
            shape = shape + subshape   # (5, 3)
            dtype = subdtype           # 'f'

        arr = jsonToArray(shape, htype, value_json)

        if len(arr.shape) == 0:
            v = arr[()]
            if isinstance(v, str):
                # if this is not utf-8, return bytes instead
                try:
                    v.encode("utf-8")
                except UnicodeEncodeError:
                    # converting utf8 unencodable string as bytes
                    v = v.encode("utf-8", errors="surrogateescape")
            return v

        return arr

    def __setitem__(self, name, value):
        """ Set a new attribute, overwriting any existing attribute.

        The type and shape of the attribute are determined from the data.  To
        use a specific type or shape, or to preserve the type of an attribute,
        use the methods create() and modify().
        """
        self.create(name, value, dtype=base.guess_dtype(value))

    def __delitem__(self, name):
        """ Delete an attribute (which must already exist). """
        self._parent.del_attr(name)

    def create(self, name, value, shape=None, dtype=None):
        """ Create new attribute(s), overwriting any existing attributes.

        name
            Name of the new attribute or list of names (required)
        value
            Array to initialize the attribute or list of arrays (required)
        shape
            Shape of the attribute.  Overrides data.shape if both are
            given, in which case the total number of points must be unchanged.
        dtype
            Data type of the attribute.  Overrides data.dtype if both
            are given.
        """

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
            use_htype = dtype.id
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
        # fiddling around to present the data as a smaller array of
        # subarrays.
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
        if use_htype is None:
            type_json = getTypeItem(dtype)

        params = {}
        params['replace'] = 1

        attr = {}

        attr['type'] = type_json
        if isinstance(value, Empty):
            attr['shape'] = 'H5S_NULL'
        else:
            attr['shape'] = shape
            if value.dtype.kind != 'c':
                attr['value'] = self._bytesArrayToList(value)
            elif isinstance(value, Reference):
                # special case reference types
                attr['value'] = value.tolist()
            else:
                # Special case: complex numbers
                special_dt = createDataType(type_json)
                tmp = numpy.empty(shape=value.shape, dtype=special_dt)
                tmp['r'] = value.real
                tmp['i'] = value.imag
                attr['value'] = json.loads(json.dumps(tmp.tolist()))

        self._parent.set_attr(name, attr)

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
        with phil:
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
        return self._parent.attr_count

    def __contains__(self, name):
        """ Determine if an attribute exists, by name. """
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if self._parent.has_attr(name):
            return True
        else:
            return False

    def __repr__(self):
        if not self._parent.id.id:
            return "<Attributes of closed HDF5 object>"
        return f"<Attributes of HDF5 object at {self._parent.uuid}>"

    def __iter__(self):
        """ Iterate over the names of attributes. """
        # convert to a list of dicts
        names = self._parent.get_attr_names(track_order=self.track_order)
        for name in names:
            yield name

    def __reversed__(self):
        """ Iterate over the names of attributes in reverse order. """
        names = self._parent.get_attr_names(track_order=self.track_order)
        for name in reversed(names):
            yield name
        # done
