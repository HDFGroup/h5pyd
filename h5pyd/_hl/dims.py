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
from . import base
from .dataset import Dataset
from .objectid import DatasetID


def _getAttrValue(objid, attr_name):
    """ helper function to get an attribute value.
       Return None if attribute is not found,
       else return attr_json['value']  """
    if objid.has_attr(attr_name):
        attr_json = objid.get_attr(attr_name)
        return attr_json['value']
    return None


class DimensionProxy(base.CommonStateObject):
    '''Represents an HDF5 'dimension'.'''

    @property
    def label(self):
        ''' Get the dimension scale label '''
        label_values = _getAttrValue(self._id, 'DIMENSION_LABELS')

        if label_values:
            return ''

        if self._dimension >= len(label_values):
            # label get request out of range
            return ''

        return label_values[self._dimension]

    @label.setter
    def label(self, val):
        label_name = 'DIMENSION_LABELS'
        if self._id.has_attr(label_name):
            labels = self._id.get_attr(label_name)
        else:
            rank = self._id.rank
            labels = {
                'shape': {
                    'class': 'H5S_SIMPLE',
                    'dims': [rank]
                },
                'type': {
                    'class': 'H5T_STRING',
                    'charSet': 'H5T_CSET_UTF8',
                    'length': 'H5T_VARIABLE',
                    'strPad': 'H5T_STR_NULLTERM'
                },
                'value': ['' for n in range(rank)]
            }
        labels['value'][self._dimension] = val
        self._id.set_attr(label_name, labels)

    def __init__(self, id_, dimension):
        if not isinstance(id_, DatasetID):
            raise TypeError(f"expected DatasetID, but got: {type(id_)}")
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
        dimlist_values = _getAttrValue(self._id, 'DIMENSION_LIST')
        if not dimlist_values:
            return 0

        if self._dimension >= len(dimlist_values):
            # dimension scale len request out of range
            return 0
        return len(dimlist_values[self._dimension])

    def __getitem__(self, item):

        dimlist_values = _getAttrValue(self._id, 'DIMENSION_LIST')
        if dimlist_values is None:
            dimlist_attr_values = []

        if self._dimension >= len(dimlist_attr_values):
            # dimension scale len request out of range")
            return None

        dimlist_values = dimlist_attr_values[self._dimension]
        dset_scale_id = None
        if isinstance(item, int):
            if item >= len(dimlist_values):
                # no dimension scale
                raise IndexError(f"No dimension scale found for index: {item}")
            ref_id = dimlist_values[item]
            if ref_id and not ref_id.startswith("datasets/"):
                msg = f"unexpected ref_id: {ref_id}"
                raise IOError(msg)
            else:
                dset_scale_id = self._id.get(ref_id)
        else:
            # Iterate through the dimension scales finding one with the
            # correct name
            for ref_id in dimlist_values:
                if not ref_id:
                    continue
                if not ref_id.startswith("datasets/"):
                    raise IOError(f"unexpected ref_id: {ref_id}")
                dset_id = self._id.get(ref_id)
                if item == _getAttrValue(dset_id, 'NAME'):
                    # found it!
                    dset_scale_id = dset_id
                    break

        if not dset_scale_id:
            raise KeyError(f"No dimension scale with name '{item}' found'")
        dscale = Dataset(dset_scale_id)

        return dscale

    def attach_scale(self, dscale):
        ''' Attach a scale to this dimension.

        Provide the Dataset of the scale you would like to attach.
        '''
        dset = Dataset(self._id)
        dscale_class = _getAttrValue(dscale.id, 'CLASS')
        if dscale_class is None:
            dset.dims.create_scale(dscale)
            dscale_class = _getAttrValue(dscale.id, 'CLASS')

        if dscale_class != 'DIMENSION_SCALE':
            raise RuntimeError(f"{dscale.name} is not a dimension scale")

        dset_class = _getAttrValue(self._id, 'CLASS')
        if dset_class == 'DIMENSION_SCALE':
            msg = f"{dset.name} cannot attach a dimension scale to a dimension scale"
            raise RuntimeError(msg)

        # Create a DIMENSION_LIST attribute if needed
        rank = self._id.rank
        value = _getAttrValue(self._id, 'DIMENSION_LIST')
        if value:
            # delete and replace later
            self._id.del_attr('DIMENSION_LIST')
        else:
            value = [list() for r in range(rank)]

        dimlist = {
            'creationProperties': {
                'nameCharEncoding': 'H5T_CSET_ASCII'
            },
            'shape': {
                'class': 'H5S_SIMPLE',
                'dims': [rank]
            },
            'type': {
                'base': {
                    'base': 'H5T_STD_REF_OBJ',
                    'class': 'H5T_REFERENCE'
                },
                'class': 'H5T_VLEN'
            },
            'value': value
        }

        # Update the DIMENSION_LIST attribute with the object reference to the
        # dimension scale
        dimlist['value'][self._dimension].append('datasets/' + dscale.id.uuid)
        self._id.set_attr('DIMENSION_list', dimlist)

        if dscale.id.has_attr('REFERENCE_LIST'):
            old_reflist = dscale.id.get_attr('REFERENCE_LIST')
        else:
            old_reflist = {
                'creationProperties': {
                    'nameCharEncoding': 'H5T_CSET_ASCII'
                },
                'shape': {
                    'class': 'H5S_SIMPLE'
                },
                'type': {
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
            }

        new_reflist = {}
        new_reflist["type"] = old_reflist["type"]
        new_reflist["shape"] = old_reflist["shape"]
        if "value" in old_reflist:
            reflist_value = old_reflist["value"]
            if reflist_value is None:
                reflist_value = []
        else:
            reflist_value = []
        reflist_value.append(['datasets/' + dset.id.uuid, self._dimension])
        new_reflist["value"] = reflist_value
        new_reflist["shape"]["dims"] = [len(reflist_value), ]

        # Update the REFERENCE_LIST attribute of the dimension scale
        dscale.id.id.set_attr('REFERENCE_LIST', new_reflist)

    def detach_scale(self, dscale):
        ''' Remove a scale from this dimension.

        Provide the Dataset of the scale you would like to remove.
        '''
        if 'DIMENSION_LIST' not in self._id.attrs:
            raise IOError("no DIMENSION_LIST attr in {dset._id}")
        dimlist = self._id.get_attr('DIMENSION_LIST')
        self._id.del_attr('DIMENSION_LIST')

        try:
            ref = 'datasets/' + dscale.id.uuid
            dimlist['value'][self._dimension].remove(ref)
        except Exception as e:
            # Restore the attribute's old value then raise the same
            # exception
            self._id.set_attr('DIMENSION_LIST', dimlist)
            raise e
        self._id.set_attr('DIMENSION_LIST', dimlist)

        if dscale.id.has_attr('REFERENCE_LIST'):
            old_reflist = dscale.id.get_attr('REFERENCE_LIST')
        else:
            old_reflist = {}

        if "value" in old_reflist and len(old_reflist["value"]) > 0:
            new_refs = list()

            remove = ['datasets/' + self._id.uuid, self._dimension]
            for el in old_reflist['value']:
                if remove[0] != el[0] and remove[1] != el[1]:
                    new_refs.append(el)

            new_reflist = {}
            new_reflist["type"] = old_reflist["type"]
            if len(new_refs) > 0:
                new_reflist["value"] = new_refs
                new_reflist["shape"] = [len(new_refs), ]
                # tbd: replace = True
                dscale.id.set_attr('REFERENCE_LIST', new_reflist)
            else:
                # Remove REFERENCE_LIST attribute if this dimension scale is
                # not attached to any dataset
                if old_reflist:
                    dscale.id.del_attr('REFERENCE_LIST')

    def items(self):
        ''' Get a list of (name, Dataset) pairs with all scales on this
        dimension.
        '''
        scales = []
        num_scales = self.__len__()
        for i in range(num_scales):
            dscale = self.__getitem__(i)
            dscale_name = _getAttrValue(dscale.id.id, 'NAME')
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
        return f'<{self.label} dimension {self._dimension} of HDf5 dataset {self._id.uuid}>'


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
        return len(Dataset(self._id).shape)

    def __iter__(self):
        ''' Iterate over the dimensions. '''
        for i in range(len(self)):
            yield self[i]

    def __repr__(self):
        if not self._id:
            return '<Dimensions of closed HDF5 dataset>'
        return f'<Dimensions of HDF5 dataset at {self._id}>'

    def create_scale(self, dset, name=''):
        ''' Create a new dimension, from an initial scale.

        Provide the dataset and a name for the scale.
        '''

        # CLASS attribute with the value 'DIMENSION_SCALE'
        class_attr = {
            'creationProperties': {
                'nameCharEncoding': 'H5T_CSET_ASCII'
            },
            'shape': {
                'class': 'H5S_SCALAR'
            },
            'type': {
                'charSet': 'H5T_CSET_ASCII',
                'class': 'H5T_STRING',
                'length': 16,
                'strPad': 'H5T_STR_NULLTERM'
            },
            'value': 'DIMENSION_SCALE'
        }

        # NAME attribute with dimension scale's name
        if isinstance(name, bytes):
            name = name.decode('ascii')
        else:
            name = name.encode('utf-8').decode('ascii')

        name_attr = {
            'creationProperties': {
                'nameCharEncoding': 'H5T_CSET_ASCII'
            },
            'shape': {
                'class': 'H5S_SCALAR'
            },
            'type': {
                'charSet': 'H5T_CSET_ASCII',
                'class': 'H5T_STRING',
                'length': len(name) + 1,
                'strPad': 'H5T_STR_NULLTERM'
            },
            'value': name
        }
        self._id.set_attr('CLASS', class_attr)
        try:
            self._id.set_attr('NAME', name_attr)
        except Exception:
            self._id.del_attr('CLASS')
