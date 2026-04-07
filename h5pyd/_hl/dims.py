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
from h5json.hdf5dtype import createDataType
from . import base
from .dataset import Dataset
from .objectid import DatasetID


def _get_obj_class(objid):
    ''' Helper function to get the class of the object by id
    '''
    attr_json = objid.db.getAttribute(objid.uuid, 'CLASS')
    if not attr_json:
        return None
    else:
        return attr_json['value']


def _set_obj_class(objid, class_name):
    ''' Set the class name for given object '''

    type_json = {
        'charSet': 'H5T_CSET_ASCII',
        'class': 'H5T_STRING',
        'length': len(class_name) + 1,
        'strPad': 'H5T_STR_NULLTERM'
    }
    dtype = createDataType(type_json)
    objid.db.createAttribute(objid.uuid, 'CLASS', class_name, dtype=dtype)


def _set_obj_name(objid, value):
    ''' Set the NAME attribute for the given object '''

    type_json = {
        'class': 'H5T_STRING',
        'charSet': 'H5T_CSET_UTF8',
        'length': 'H5T_VARIABLE',
        'strPad': 'H5T_STR_NULLTERM'
    }
    dtype = createDataType(type_json)
    objid.db.createAttribute(objid.uuid, 'NAME', value, dtype=dtype)


def _get_obj_name(objid):
    ''' return the NAME attribute value '''

    attr_json = objid.db.getAttribute(objid.uuid, 'NAME')
    if not attr_json:
        return None
    else:
        return attr_json["value"]


class DimensionProxy(base.CommonStateObject):
    '''Represents an HDF5 'dimension'.'''

    def _get_reflist(self, scale_id):
        ''' Return value of reference list attribute if present '''
        attr_json = self._id.db.getAttribute(scale_id.uuid, 'REFERENCE_LIST')
        if attr_json:
            return attr_json['value']
        else:
            return []

    def _update_reflist(self, scale_id, remove=False):
        ''' Add a reference to the REFERNCE_LIST attribute for the given scale and dimension index '''

        attr_json = self._id.db.getAttribute(scale_id.uuid, 'REFERENCE_LIST')
        if attr_json is None:
            if remove:
                # nothing to remove, just return
                return
            value = []
        else:
            value = attr_json["value"]

        ref = 'datasets/' + self._id.uuid  # the reference to add or remove

        type_json = {
            'class': 'H5T_COMPOUND',
            'fields': [
                {
                    'name': 'dataset',
                    'type': {
                        'base': 'H5T_STD_REF_OBJ',
                        'class': 'H5T_REFERENCE'
                    }
                },
                {
                    'name': 'index',
                    'type': {
                        'base': 'H5T_STD_I32LE',
                        'class': 'H5T_INTEGER'
                    }
                }
            ]
        }

        if remove:
            # look through existing values and remove any with the same ref and dimension
            value_update = []
            for e in value:
                if e[0] == ref and e[1] == self._dimension:
                    continue
                value_update.append(e)  # keep the current item
            if len(value) == len(value_update):
                # no change, just return
                return
            value = value_update
            if len(value) == 0:
                # Remove REFERENCE_LIST attribute if this dimension scale is
                # not attached to any dataset
                self._id.db.deleteAttribute(scale_id.uuid, 'REFERENCE_LIST')
        else:
            # scan through list and see if this ref is already present
            for e in value:
                if e[0] == ref and e[1] == self._dimension:
                    # reference already exists, just return
                    return
            # not found, append the new ref, dimension tuple
            value.append([ref, self._dimension])

        dtype = createDataType(type_json)

        shape = [len(value),]

        self._id.db.createAttribute(scale_id.uuid, 'REFERENCE_LIST', value, dtype=dtype, shape=shape)

    def _get_dimlist(self):
        """ return a dimension list for given dimension """

        attr_json = self._id.db.getAttribute(self._id.uuid, 'DIMENSION_LIST')

        if attr_json is None:
            return []
        value = attr_json['value']
        if len(value) != self._id.rank:
            raise IOError(f"invalid dimension list value: {value}")
        return value[self._dimension]

    def _update_dimlist(self, scale_id, remove=False):
        ''' append a reference to the DIMENSION_LIST attribute for the given dimension index '''

        attr_json = self._id.db.getAttribute(self._id.uuid, 'DIMENSION_LIST')
        if attr_json is None:
            value = [[] for _ in range(self._id.rank)]
        else:
            value = attr_json['value']

        if len(value) != self._id.rank:
            raise IOError(f"invalid dimension list value: {value}")

        ref = "datasets/" + scale_id.uuid
        if remove and ref not in value[self._dimension]:
            return
        if not remove and ref in value[self._dimension]:
            return

        type_json = {
            'base': {
                'base': 'H5T_STD_REF_OBJ',
                'class': 'H5T_REFERENCE'
            },
            'class': 'H5T_VLEN'
        }
        dtype = createDataType(type_json)
        shape = [self._id.rank,]
        if remove:
            value[self._dimension].remove(ref)
        else:
            value[self._dimension].append(ref)

        self._id.db.createAttribute(self._id.uuid, 'DIMENSION_LIST', value, dtype=dtype, shape=shape)

    @property
    def label(self):
        ''' Get the dimension scale label '''
        labels_json = self._id.db.getAttribute(self._id.uuid, 'DIMENSION_LABELS')

        if not labels_json:
            return ''

        label_values = labels_json["value"]

        if self._dimension >= len(label_values):
            # label get request out of range
            return ''

        return label_values[self._dimension]

    @label.setter
    def label(self, val):
        name = 'DIMENSION_LABELS'
        labels_json = self._id.db.getAttribute(self._id.uuid, 'DIMENSION_LABELS')
        if labels_json:
            labels = labels_json['value']
            if len(labels) != self._id.rank:
                raise ValueError("unexpected lenght of DIMENSION_LABELS attribute")
        else:
            labels = ['' for _ in range(self._id.rank)]

        type_json = {
            'class': 'H5T_STRING',
            'charSet': 'H5T_CSET_UTF8',
            'length': 'H5T_VARIABLE',
            'strPad': 'H5T_STR_NULLTERM'
        }
        dtype = createDataType(type_json)
        labels[self._dimension] = val

        self._id.db.createAttribute(self._id.uuid, name, labels, dtype=dtype)

    def __init__(self, id_, dimension):
        self._id = id_
        self._dimension = int(dimension)

    def __hash__(self):
        return hash((type(self), self._id, self._dimension))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __iter__(self):
        for k in self.keys():
            yield k

    def __len__(self):
        dimlist = self._get_dimlist()

        return len(dimlist)

    def __getitem__(self, item):

        dimlist = self._get_dimlist()
        if dimlist is None:
            dimlist = []

        scale_id = None  # DatasetID instance
        if isinstance(item, int):
            if item >= len(dimlist):
                # no dimension scale
                raise IndexError(f"No dimension scale found for index: {item}")
            ref_id = dimlist[item]
            if ref_id and not ref_id.startswith("datasets/"):
                msg = f"unexpected ref_id: {ref_id}"
                raise IOError(msg)
            scale_id = DatasetID(self._id, ref_id)
        else:
            # Iterate through the dimension scales finding one with the
            # correct name
            for ref_id in dimlist:
                if not ref_id:
                    continue
                if not ref_id.startswith("datasets/"):
                    msg = f"unexpected ref_id: {ref_id}"
                    raise IOError(msg)
                dset_id = DatasetID(self._id, ref_id)
                dim_name = _get_obj_name(dset_id)
                if dim_name == item:
                    # found it!
                    scale_id = dset_id
                    break
        if not scale_id:
            raise KeyError(f'No dimension scale with name {item} found')
        dscale = Dataset(scale_id)
        return dscale

    def attach_scale(self, dscale):
        ''' Attach a scale to this dimension.

        Provide the Dataset of the scale you would like to attach.
        '''
        dset = Dataset(self._id)
        dscale_class = _get_obj_class(dscale.id)
        if not dscale_class:
            dset.dims.create_scale(dscale)
            dscale_class = _get_obj_class(dscale.id)

        if dscale_class != 'DIMENSION_SCALE':
            raise RuntimeError(f"{dscale.name} is not a dimension scale")

        dset_class = _get_obj_class(dset.id)
        if dset_class == 'DIMENSION_SCALE':
            msg = f"{dset.name}"
            raise RuntimeError(msg)

        # Create a DIMENSION_LIST attribute if needed
        self._update_dimlist(dscale.id)

        # create a REFERENCE_LIST attribute for the dimension scale
        self._update_reflist(dscale.id)

    def detach_scale(self, dscale):
        ''' Remove a scale from this dimension.

        Provide the Dataset of the scale you would like to remove.
        '''

        self._update_dimlist(dscale.id, remove=True)
        self._update_reflist(dscale.id, remove=True)

    def items(self):
        ''' Get a list of (name, Dataset) pairs with all scales on this
        dimension.
        '''

        scales = []
        num_scales = self.__len__()
        for i in range(num_scales):
            dscale = self.__getitem__(i)
            dscale_name = _get_obj_name(dscale.id)
            scales.append((dscale_name, dscale))
        return scales

    def keys(self):
        ''' Get a list of names for the scales on this dimension. '''
        return [key for (key, _) in self.items()]

    def values(self):
        ''' Get a list of Dataset for scales on this dimension. '''
        return [val for (_, val) in self.items()]

    def __repr__(self):
        if not self._id:
            return '<Dimension of closed HDF5 dataset>'
        return f'<{self.label} dimension {self._dimension} of HDf5 dataset {self._id.id}>'


class DimensionManager(base.MappingHDF5, base.CommonStateObject):
    '''
        Represents a collection of dimensions associated with a dataset.

        Like AttributeManager, an instance of this class is returned when
        accessing the '.dims' property of a Dataset.
    '''

    def __init__(self, parent):
        ''' Private constructor.
        '''
        self._id = parent.id

    def __getitem__(self, index):
        ''' Return a Dimension object
        '''
        if index > len(self) - 1:
            raise IndexError('Index out of range')
        return DimensionProxy(self._id, index)

    def __len__(self):
        ''' Number of dimensions associated with the dataset. '''
        return self._id.rank

    def __iter__(self):
        ''' Iterate over the dimensions. '''
        for i in range(self._id.rank):
            yield self[i]

    def __repr__(self):
        if not self._id:
            return '<Dimensions of closed HDF5 dataset>'
        return f'<Dimensions of HDF5 dataset at {self._id}>'

    def create_scale(self, dset, name=''):
        ''' Create a new dimension, from an initial scale.

        Provide the dataset and a name for the scale.
        '''

        if not isinstance(name, str):
            raise TypeError("Expected string for dimension_scale name")
        _set_obj_class(dset.id, 'DIMENSION_SCALE')
        _set_obj_name(dset.id, name)
