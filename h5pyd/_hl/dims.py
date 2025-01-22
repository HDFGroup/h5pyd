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
from .. import h5ds


class DimensionProxy(base.CommonStateObject):
    '''Represents an HDF5 'dimension'.'''

    @property
    def label(self):
        ''' Get the dimension scale label '''
        return h5ds.get_label(self._id, self._dimension)

    @label.setter
    def label(self, val):
        h5ds.set_label(self._id, self._dimension, val)

    def __init__(self, dset, dimension):
        if not isinstance(dset, Dataset):
            raise TypeError(f"expected Dataset, but got: {type(dset)}")
        self._id = dset.id
        self._dimension = int(dimension)

    def __hash__(self):
        return hash((type(self), self._id, self._dimension))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __iter__(self):
        for k in self.keys():
            yield k

    def __len__(self):
        return h5ds.get_num_scales(self._id, self._dimension)

    def __getitem__(self, item):
        """ get a dimension scale.
            item can be an int in which case scale at that index will be returned
            or item can be a str in which casee the scale ith that name will be returned """

        if isinstance(item, int):
            scales = []
            h5ds.iterate(self._id, self._dimension, scales.append, 0)
            return Dataset(scales[item])
        else:
            def f(dsid):
                """ Iterate over scales to find a matching name """
                if h5ds.get_scale_name(dsid) == self._e(item):
                    return dsid

            res = h5ds.iterate(self._id, self._dimension, f, 0)
            if res is None:
                raise KeyError(item)
            return Dataset(res)

    def attach_scale(self, dscale):
        ''' Attach a scale to this dimension.

        Provide the Dataset of the scale you would like to attach.
        '''
        h5ds.attach_scale(self._id, dscale.id, self._dimension)

    def detach_scale(self, dscale):
        ''' Remove a scale from this dimension.

        Provide the Dataset of the scale you would like to remove.
        '''
        h5ds.detach_scale(self._id, dscale.id, self._dimension)

    def items(self):
        ''' Get a list of (name, Dataset) pairs with all scales on this
        dimension.
        '''
        scale_ids = []

        # H5DSiterate raises an error if there are no dimension scales,
        # rather than iterating 0 times.
        if len(self) > 0:
            h5ds.iterate(self._id, self._dimension, scale_ids.append, 0)

        scales = []
        for scale_id in scale_ids:
            scale_name = h5ds.get_scale_name(scale_id)
            scales.append(scale_name, Dataset(scale_id))
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

        dset.make_scale(name)
