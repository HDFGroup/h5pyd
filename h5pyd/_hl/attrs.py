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
from .objectid import GroupID, DatasetID, TypeID
from .h5type import getTypeItem, createDataType, special_dtype, Reference


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
        objdb = self._parent.id.http_conn.getObjDb()
        if objdb:
            # _objdb is meta-data pulled from the domain on open.
            # see if we can extract the link json from there
            objid = self._parent.id.uuid
            if objid not in objdb:
                raise IOError("Expected to find {} in objdb".format(objid))
            obj_json = objdb[objid]
            self._objdb_attributes = obj_json["attributes"]
        else:
            self._objdb_attributes = None

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

        if self._objdb_attributes is not None:
            if name not in self._objdb_attributes:
                raise KeyError
            attr_json = self._objdb_attributes[name]
        else:
            req = self._req_prefix + name
            try:
                attr_json = self._parent.GET(req)
            except IOError:
                raise KeyError

        shape_json = attr_json['shape']
        type_json = attr_json['type']
        dtype = createDataType(type_json)
        if shape_json['class'] == 'H5S_NULL':
            return Empty(dtype)
        value_json = attr_json['value']

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
            dtype = subdtype           # 'f'

        arr = jsonToArray(shape, htype, value_json)

        if len(arr.shape) == 0:
            v = arr[()]
            if isinstance(v, str):
                # if this is not utf-8, return bytes instead
                try:
                    v.encode("utf-8")
                except UnicodeEncodeError:
                    self._parent.log.debug("converting utf8 unencodable string as bytes")
                    v = v.encode("utf-8", errors="surrogateescape")
            return v

        return arr

    def get_attributes(self, names=None, pattern=None, limit=None, marker=None, use_cache=True):
        """
        Get all attributes or a subset of attributes from the target object.
        If 'use_cache' is True, use the objdb cache if available.
        The cache cannot be used with pattern, limit, or marker parameters.
        - if 'pattern' is provided, retrieve all attributes with names that match the pattern
          according to Unix pathname pattern expansion rules.
        - if 'limit' is provided, retrieve at most 'limit' attributes.
        - if 'marker' is provided, retrieve attributes whose names occur after the name 'marker' in the target object
        """
        if use_cache and (pattern or limit or marker):
            raise ValueError("use_cache cannot be used with pattern, limit, or marker parameters")

        if names and (pattern or limit or marker or use_cache):
            raise ValueError("names cannot be used with pattern, limit, marker, or cache")

        if self._objdb_attributes is not None:
            # use the objdb cache
            out = {}
            for a in self._objdb_attributes:
                name = a['name']
                out[name] = self._objdb_attributes[name]
            return out

        # Omit trailing slash
        req = self._req_prefix[:-1]
        req += "?IncludeData=1"
        body = {}

        if pattern:
            req += "&pattern=" + pattern
        if limit:
            req += "&Limit=" + str(limit)
        if marker:
            req += "&Marker=" + marker

        if names:
            if isinstance(names, list):
                names = [name.decode('utf-8') if isinstance(name, bytes) else name for name in names]
            else:
                if isinstance(names, bytes):
                    names = names.decode("utf-8")
                names = [names]

            body['attr_names'] = names

        if body:
            rsp = self._parent.POST(req, body=body)
        else:
            rsp = self._parent.GET(req)

        attrs_json = rsp['attributes']
        names = [attr['name'] for attr in attrs_json]
        values = [attr['value'] for attr in attrs_json]
        out = {}

        for i in range(len(names)):
            out[names[i]] = values[i]

        return out

    def __setitem__(self, name, value):
        """ Set a new attribute, overwriting any existing attribute.

        The type and shape of the attribute are determined from the data.  To
        use a specific type or shape, or to preserve the type of an attribute,
        use the methods create() and modify().
        """
        self.create(name, values=value, dtype=base.guess_dtype(value))

    def __delitem__(self, name):
        """ Delete an attribute (which must already exist). """
        if isinstance(name, list):
            names = [name.decode('utf-8') if isinstance(name, bytes) else name for name in name]
            # Omit trailing slash
            req = self._req_prefix[:-1] + "?attr_names=" + "/".join(names)
        else:
            if isinstance(name, bytes):
                name = name.decode("utf-8")
            req = self._req_prefix + name
        self._parent.DELETE(req)

    def create(self, names, values, shape=None, dtype=None):
        """ Create new attribute(s), overwriting any existing attributes.

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
        self._parent.log.info(f"attrs.create({names})")

        # Standardize single attribute arguments to lists
        if not isinstance(names, list):
            names = [names]
            values = [values]

        if shape is not None and not isinstance(shape, list):
            shapes = [shape]
        elif shape is None:
            shapes = [None] * len(names)
        else:
            # Given shape is already a list of shapes
            shapes = shape

        if dtype is not None and not isinstance(dtype, list):
            dtypes = [dtype]
        elif dtype is None:
            dtypes = [None] * len(names)
        else:
            # Given dtype is already a list of dtypes
            dtypes = dtype

        type_jsons = [None] * len(names)

        if (len(names) != len(values)) or (shapes is not None and len(shapes) != len(values)) or\
           (dtypes is not None and len(dtypes) != len(values)):
            raise ValueError("provided names, values, shapes and dtypes must have the same length")

        for i in range(len(names)):
            # First, make sure we have a NumPy array.  We leave the data
            # type conversion for HDF5 to perform.
            if isinstance(values[i], Reference):
                dtypes[i] = special_dtype(ref=Reference)
            if not isinstance(values[i], Empty):
                values[i] = numpy.asarray(values[i], dtype=dtypes[i], order='C')

            if shapes[i] is None and not isinstance(values[i], Empty):
                shapes[i] = values[i].shape

            use_htype = None  # If a committed type is given, we must use it in h5a.create.

            if isinstance(dtypes[i], Datatype):
                use_htype = dtypes[i].id
                dtypes[i] = dtypes[i].dtype

                # Special case if data are complex numbers
                is_complex = (values[i].dtype.kind == 'c') and (dtypes[i].names is None) or (
                    dtypes[i].names != ('r', 'i')) or (
                    any(dt.kind != 'f' for dt, off in dtypes[i].fields.values())) or (
                    dtypes[i].fields['r'][0] == dtypes[i].fields['i'][0])

                if is_complex:
                    raise TypeError(
                        f'Wrong committed datatype for complex numbers: {dtypes[i].name}')
            elif dtypes[i] is None:
                if values[i].dtype.kind == 'U':
                    # use vlen for unicode strings
                    dtypes[i] = special_dtype(vlen=str)
                else:
                    dtypes[i] = values[i].dtype
            else:
                dtypes[i] = numpy.dtype(dtypes[i])  # In case a string, e.g. 'i8' is passed

            # Where a top-level array type is requested, we have to do some
            # fiddling around to present the data as a smaller array of
            # subarrays.
            if not isinstance(values[i], Empty):
                if dtypes[i].subdtype is not None:

                    subdtype, subshape = dtypes[i].subdtype

                    # Make sure the subshape matches the last N axes' sizes.
                    if shapes[i][-len(subshape):] != subshape:
                        raise ValueError(f"Array dtype shape {subshape} is incompatible with data shape {shapes[i]}")

                    # New "advertised" shape and dtype
                    shapes[i] = shapes[i][0:len(shapes[i]) - len(subshape)]
                    dtypes[i] = subdtype

                # Not an array type; make sure to check the number of elements
                # is compatible, and reshape if needed.
                else:
                    if numpy.prod(shapes[i]) != numpy.prod(values[i].shape):
                        raise ValueError("Shape of new attribute conflicts with shape of data")

                    if shapes[i] != values[i].shape:
                        values[i] = values[i].reshape(shapes[i])

                # We need this to handle special string types.

                    values[i] = numpy.asarray(values[i], dtype=dtypes[i])

            # Make HDF5 datatype and dataspace for the H5A calls
            if use_htype is None:
                type_jsons[i] = getTypeItem(dtypes[i])
                self._parent.log.debug("attrs.create type_json: {}".format(type_jsons[i]))

        # This mess exists because you can't overwrite attributes in HDF5.
        # So we write to a temporary attribute first, and then rename.

        params = {}
        body = {}
        if len(names) > 1:
            # Create multiple attributes
            # Omit trailing slash
            req = self._req_prefix[:-1]
            attributes = {}

            for i in range(len(names)):
                attr = {}
                attr['type'] = type_jsons[i]
                if isinstance(values[i], Empty):
                    attr['shape'] = 'H5S_NULL'
                else:
                    attr['shape'] = shapes[i]
                    if values[i].dtype.kind != 'c':
                        attr['value'] = self._bytesArrayToList(values[i])
                    else:
                        # Special case: complex numbers
                        special_dt = createDataType(type_jsons[i])
                        tmp = numpy.empty(shape=values[i].shape, dtype=special_dt)
                        tmp['r'] = values[i].real
                        tmp['i'] = values[i].imag
                        attr['value'] = json.loads(json.dumps(tmp.tolist()))
                attributes[names[i]] = attr

            body['attributes'] = attributes
            params['replace'] = 1

        else:
            # Create single attribute
            req = self._req_prefix + names[0]
            body['type'] = type_jsons[0]
            if isinstance(values[0], Empty):
                body['shape'] = 'H5S_NULL'
            else:
                body['shape'] = shapes[0]
                if values[0].dtype.kind != 'c':
                    body['value'] = self._bytesArrayToList(values[0])
                else:
                    # Special case: complex numbers
                    special_dt = createDataType(type_jsons[0])
                    tmp = numpy.empty(shape=values[0].shape, dtype=special_dt)
                    tmp['r'] = values[0].real
                    tmp['i'] = values[0].imag
                    body['value'] = json.loads(json.dumps(tmp.tolist()))

        try:
            self._parent.PUT(req, body=body, params=params)
        except RuntimeError:
            if len(names) == 1:
                # Resource already exist, try deleting it
                self._parent.log.info(f"Update to existing attribute(s) ({names}), deleting it")
                self._parent.DELETE(req)
                # now add again
                self._parent.PUT(req, body=body, params=params)
            else:
                # putAttributes uses replace parameter by default,
                # so failure is not due to existing attribute
                raise RuntimeError("Failued to create attributes")

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

        if self._objdb_attributes is not None:
            count = len(self._objdb_attributes)
        else:
            # make a server requests
            req = self._req_prefix
            # backup over the '/attributes/' part of the req
            req = req[:-(len('/attributes/'))]
            rsp = self._parent.GET(req)  # get parent obj
            count = rsp['attributeCount']
        return count

    def __iter__(self):
        """ Iterate over the names of attributes. """
        if self._objdb_attributes is not None:

            for name in self._objdb_attributes:
                yield name

        else:
            # make server request
            req = self._req_prefix
            # backup over the trailing slash in req
            req = req[:-1]
            rsp = self._parent.GET(req)
            attributes = rsp['attributes']

            attrlist = []
            for attr in attributes:
                attrlist.append(attr['name'])

            for name in attrlist:
                yield name

    def __contains__(self, name):
        """ Determine if an attribute exists, by name. """
        exists = True
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if self._objdb_attributes is not None:
            exists = name in self._objdb_attributes
        else:
            # make server request
            req = self._req_prefix + name
            try:
                self._parent.GET(req)
            except IOError:
                # todo - verify this is a 404 response
                exists = False
        return exists

    def __repr__(self):
        if not self._parent.id.id:
            return "<Attributes of closed HDF5 object>"
        return f"<Attributes of HDF5 object at {id(self._parent.id)}>"
