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
import six

from . import base
from .base import phil, with_phil
from .datatype import Datatype
from .objectid import GroupID, DatasetID, TypeID
from .h5type import getTypeItem, createDataType, special_dtype, check_dtype

"""
Convert a list to a tuple, recursively.
Example. [[1,2],[3,4]] -> ((1,2),(3,4))
"""
def toTuple(rank, data):
    if type(data) in (list, tuple):
        if rank > 0:
            return list(toTuple(rank-1, x) for x in data)
        else:
            return tuple(toTuple(rank-1, x) for x in data)
    else:
        return data

"""
Return numpy array from the given json array.
Note: copied from hsds arrayUti.py
"""
def jsonToArray(data_shape, data_dtype, data_json):
    # need some special conversion for compound types --
    # each element must be a tuple, but the JSON decoder
    # gives us a list instead.
    if len(data_dtype) > 1 and not isinstance(data_json, (list, tuple)):
        raise TypeError("expected list data for compound data type")
    npoints = numpy.product(data_shape)
    if type(data_json) in (list, tuple):
        np_shape_rank = len(data_shape)
        converted_data = []
        if npoints == 1 and len(data_json) == len(data_dtype):
            converted_data.append(toTuple(0, data_json))
        else:  
            converted_data = toTuple(np_shape_rank, data_json)
        data_json = converted_data

    arr = numpy.array(data_json, dtype=data_dtype)
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

        if isinstance(parent.id, GroupID):
            self._req_prefix = "/groups/" + parent.id.uuid + "/attributes/"
        elif isinstance(parent.id, TypeID):
            self._req_prefix = "/datatypes/" + parent.id.uuid + "/attributes/"
        elif isinstance(parent.id, DatasetID):
            self._req_prefix = "/datasets/" + parent.id.uuid + "/attributes/"
        else:
            # "unknown id"
            self._req_prefix = "<unknown>"
   
    
    def _bytesArrayToList(self, data):
        """
        Convert list that may contain bytes type elements to list of string elements  
        """
        if six.PY2:
            text_types = (bytes, str, unicode)
        else:
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
                out.append(self._bytesArrayToList(item)) # recursive call  
        elif isinstance(data, bytes):
            if six.PY3:
                out = data.decode("utf-8")
            else:
                out = data
        else:
            out = data
                   
        return out


    @with_phil
    def __getitem__(self, name):
        """ Read the value of an attribute.
        """
        #attr = h5a.open(self._id, self._e(name))
        req = self._req_prefix + name
        try:
            attr_json = self._parent.GET(req)
        except IOError:
            raise KeyError
        
        shape_json = attr_json['shape']
        type_json = attr_json['type']
        if shape_json['class'] == 'H5S_NULL':
            raise IOError("Empty attributes cannot be read")
        value_json = attr_json['value']
        
        dtype = createDataType(type_json)

        if 'dims' in shape_json:
            shape = shape_json['dims']
        else:
            shape = ()

        # Do this first, as we'll be fiddling with the dtype for top-level
        # array types
        htype = dtype

        # NumPy doesn't support top-level array types, so we have to "fake"
        # the correct type and shape for the array.  For example, consider
        # attr.shape == (5,) and attr.dtype == '(3,)f'. Then:
        if dtype.subdtype is not None:
            subdtype, subshape = dtype.subdtype
            shape = shape + subshape   # (5, 3)
            dtype = subdtype                # 'f'

        vlen_base = check_dtype(vlen=dtype)

        #arr = numpy.array(value_json, dtype=htype)
        arr = jsonToArray(shape, htype, value_json)
        vlen_base = check_dtype(vlen=dtype)
        if vlen_base is not None and len(arr.shape) > 0:
            # convert list elements to ndarrays
            nelements = numpy.product(arr.shape)
            arr_1d = arr.reshape((nelements,))
            for i in range(nelements):
                arr_1d[i] = numpy.array(arr_1d[i], dtype=vlen_base)

        if len(arr.shape) == 0:
            return arr[()]
        return arr

    @with_phil
    def __setitem__(self, name, value):
        """ Set a new attribute, overwriting any existing attribute.

        The type and shape of the attribute are determined from the data.  To
        use a specific type or shape, or to preserve the type of an attribute,
        use the methods create() and modify().
        """
        self.create(name, data=value, dtype=base.guess_dtype(value))

    @with_phil
    def __delitem__(self, name):
        """ Delete an attribute (which must already exist). """
        req = self._req_prefix + name
        self._parent.DELETE(req)

    def create(self, name, data, shape=None, dtype=None):
        """ Create a new attribute, overwriting any existing attribute.

        name
            Name of the new attribute (required)
        data
            An array to initialize the attribute (required)
        shape
            Shape of the attribute.  Overrides data.shape if both are
            given, in which case the total number of points must be unchanged.
        dtype
            Data type of the attribute.  Overrides data.dtype if both
            are given.
        """
        self._parent.log.info("attrs.create({})".format(name))


        with phil:

            # First, make sure we have a NumPy array.  We leave the data
            # type conversion for HDF5 to perform.
            data = numpy.asarray(data, order='C')

            if shape is None:
                shape = data.shape

            use_htype = None    # If a committed type is given, we must use it
                                # in the call to h5a.create.

            if isinstance(dtype, Datatype):
                use_htype = dtype.id
                dtype = dtype.dtype
            elif dtype is None:
                if data.dtype.kind == 'U':
                    # use vlen for unicode strings
                    if six.PY3:
                        dtype = special_dtype(vlen=str)
                    else:
                        dtype = special_dtype(vlen=unicode)
                else:
                    dtype = data.dtype
            else:
                dtype = numpy.dtype(dtype) # In case a string, e.g. 'i8' is passed

            #original_dtype = dtype  # We'll need this for top-level array types

            # Where a top-level array type is requested, we have to do some
            # fiddling around to present the data as a smaller array of
            # subarrays.
            if dtype.subdtype is not None:

                subdtype, subshape = dtype.subdtype

                # Make sure the subshape matches the last N axes' sizes.
                if shape[-len(subshape):] != subshape:
                    raise ValueError("Array dtype shape %s is incompatible with data shape %s" % (subshape, shape))

                # New "advertised" shape and dtype
                shape = shape[0:len(shape)-len(subshape)]
                dtype = subdtype

            # Not an array type; make sure to check the number of elements
            # is compatible, and reshape if needed.
            else:

                if numpy.product(shape) != numpy.product(data.shape):
                    raise ValueError("Shape of new attribute conflicts with shape of data")

                if shape != data.shape:
                    data = data.reshape(shape)

            # We need this to handle special string types.
            data = numpy.asarray(data, dtype=dtype)

            # Make HDF5 datatype and dataspace for the H5A calls
            if use_htype is None:
                type_json = getTypeItem(dtype)
                self._parent.log.debug("attrs.create type_json: {}".format(type_json))
               
 
            # This mess exists because you can't overwrite attributes in HDF5.
            # So we write to a temporary attribute first, and then rename.

            req = self._req_prefix + name
            body = {}
            body['type'] = type_json
            body['shape'] = shape
            body['value'] = self._bytesArrayToList(data) 

            try:
                self._parent.PUT(req, body=body)
            except RuntimeError:
                # Resource already exist, try deleting it
                self._parent.log.info("Update to existing attribute ({}), deleting it".format(name))
                self._parent.DELETE(req)
                # now add again
                self._parent.PUT(req, body=body)


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
                   (numpy.product(value.shape) == 1 and numpy.product(attr.shape) == 1):
                    raise TypeError("Shape of data is incompatible with existing attribute")
                attr.write(value)
        """

    @with_phil
    def __len__(self):
        """ Number of attributes attached to the object. """
        # I expect we will not have more than 2**32 attributes
        req = self._req_prefix

        # backup over the '/attributes/' part of the req
        req = req[:-(len('/attributes/'))]
        rsp = self._parent.GET(req)  # get parent obj
        count = rsp['attributeCount']
        # return h5a.get_num_attrs(self._id)
        return count

    def __iter__(self):
        """ Iterate over the names of attributes. """
        req = self._req_prefix
        # backup over the trailing slash in req
        req = req[:-1]
        rsp = self._parent.GET(req)
        attributes = rsp['attributes']
        with phil:

            attrlist = []
            for attr in attributes:
                attrlist.append(attr['name'])
            
        for name in attrlist:
            yield name

    @with_phil
    def __contains__(self, name):
        """ Determine if an attribute exists, by name. """
        exists = True
        req = self._req_prefix + name
        try:
            self._parent.GET(req)
        except IOError:
            #todo - verify this is a 404 response
            exists = False
        return exists

        #return h5a.exists(self._id, self._e(name))

    @with_phil
    def __repr__(self):
        if not self._parent.id.id:
            return "<Attributes of closed HDF5 object>"
        return "<Attributes of HDF5 object at %s>" % id(self._parent.id)
