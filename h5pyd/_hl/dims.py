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
import six
from . import base
from .base import phil, with_phil
from .dataset import Dataset
from .objectid import DatasetID


class DimensionProxy(base.CommonStateObject):
    '''Represents an HDF5 'dimension'.'''

    @property
    @with_phil
    def label(self):
        ''' Get or set the dimension scale label '''
        dset = Dataset(self._id)
        try:
            labels = dset.GET(dset.attrs._req_prefix + 'DIMENSION_LABELS')
            return labels['value'][self._dimension]
        except (IOError, IndexError):
            return ''

    @label.setter
    @with_phil
    def label(self, val):
        # pylint: disable=missing-docstring
        dset = Dataset(self._id)
        req = dset.attrs._req_prefix + 'DIMENSION_LABELS'
        try:
            labels = dset.GET(req)
            dset.DELETE(req)
        except IOError:
            rank = len(dset.shape)
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
        dset.PUT(req, body=labels, replace=True)

    @with_phil
    def __init__(self, id_, dimension):
        self._id = id_
        self._dimension = dimension

    @with_phil
    def __hash__(self):
        return hash((type(self), self._id, self._dimension))

    @with_phil
    def __eq__(self, other):
        return hash(self) == hash(other)

    @with_phil
    def __iter__(self):
        for k in self.keys():
            yield k

    @with_phil
    def __len__(self):
        dset = Dataset(self._id)
        try:
            dimlist = dset.GET(dset.attrs._req_prefix + 'DIMENSION_LIST')
        except IOError:
            return 0
        return len(dimlist['value'][self._dimension])

    @with_phil
    def __getitem__(self, item):
        dset = Dataset(self._id)
        with phil:
            dimlist = dset.GET(dset.attrs._req_prefix + 'DIMENSION_LIST')

            if isinstance(item, int):
                dscale_req = dimlist['value'][self._dimension][item]
                dscale_json = dset.GET('/' + dscale_req)
                return Dataset(DatasetID(parent=None, item=dscale_json,
                                         http_conn=self._id.http_conn))

            else:
                # The assumtion here is that the item argument is the name of a
                # dimension scale attached to this dimension.
                item = str(item).encode('ascii')

                # Iterate through the dimension scales finding one with the
                # correct name
                dscales = dimlist['value'][self._dimension]
                for d in dscales:
                    dscale_json = dset.GET('/' + d)
                    dscale = Dataset(DatasetID(parent=None, item=dscale_json,
                                               http_conn=self._id.http_conn))
                    if dscale.attrs['NAME'] == item:
                        return dscale
                    raise KeyError('No dimension scale with name"{}" found'
                                   .format(item))

    def attach_scale(self, dscale):
        ''' Attach a scale to this dimension.

        Provide the Dataset of the scale you would like to attach.
        '''
        dset = Dataset(self._id)
        try:
            rsp = dscale.GET(dscale.attrs._req_prefix + 'CLASS')
        except IOError:
            dset.dims.create_scale(dscale)
            rsp = None

        if not rsp:
            rsp = dscale.GET(dscale.attrs._req_prefix + 'CLASS')
        if rsp['value'] != 'DIMENSION_SCALE':
            raise RuntimeError(
                '{} is not a dimension scale'.format(dscale.name))

        try:
            rsp = dset.GET(dset.attrs._req_prefix + 'CLASS')
            if rsp['value'] == 'DIMENSION_SCALE':
                raise RuntimeError(
                    '{} cannot attach a dimension scale to a dimension scale'
                    .format(dset.name))
        except IOError:
            pass

        # Create a DIMENSION_LIST attribute if needed
        req = dset.attrs._req_prefix + 'DIMENSION_LIST'
        rank = len(dset.shape)
        value = [list() for r in range(rank)]
        try:
            dimlist = dset.GET(req)
            value = dimlist["value"]
            dset.DELETE(req)
        except IOError:
            pass

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
        dimlist['value'][self._dimension].append('datasets/' + dscale.id.id)
        dset.PUT(req, body=dimlist, replace=True)

        req = dscale.attrs._req_prefix + 'REFERENCE_LIST'

        try:
            old_reflist = dscale.GET(req)
        except IOError:
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
        reflist_value.append(['datasets/' + dset.id.id, self._dimension])
        new_reflist["value"] = reflist_value
        new_reflist["shape"]["dims"] = [len(reflist_value), ]

        # Update the REFERENCE_LIST attribute of the dimension scale
        with phil:
            dscale.PUT(req, body=new_reflist, replace=True)

    def detach_scale(self, dscale):
        ''' Remove a scale from this dimension.

        Provide the Dataset of the scale you would like to remove.
        '''
        dset = Dataset(self._id)
        req = dset.attrs._req_prefix + 'DIMENSION_LIST'
        with phil:
            dimlist = dset.GET(req)
            dset.DELETE(req)
            try:
                ref = 'datasets/' + dscale.id.id
                dimlist['value'][self._dimension].remove(ref)
            except Exception as e:
                # Restore the attribute's old value then raise the same
                # exception
                dset.PUT(req, body=dimlist)
                raise e
            dset.PUT(req, body=dimlist)

        req = dscale.attrs._req_prefix + 'REFERENCE_LIST'
        with phil:
            old_reflist = dscale.GET(req)
            if "value" in old_reflist and len(old_reflist["value"]) > 0:
                new_refs = list()

                remove = ['datasets/' + dset.id.id, self._dimension]
                for el in old_reflist['value']:
                    if remove[0] != el[0] and remove[1] != el[1]:
                        new_refs.append(el)

                new_reflist = {}
                new_reflist["type"] = old_reflist["type"]
                if len(new_refs) > 0:
                    new_reflist["value"] = new_refs
                    new_reflist["shape"] = [len(new_refs), ]
                    dscale.PUT(req, body=new_reflist, replace=True)
                else:
                    # Remove REFERENCE_LIST attribute if this dimension scale is
                    # not attached to any dataset
                    try:
                        dscale.DELETE(req)
                    except OSError:
                        pass

    def items(self):
        ''' Get a list of (name, Dataset) pairs with all scales on this
        dimension.
        '''
        dset = Dataset(self._id)
        with phil:
            scales = []
            try:
                dimlist = dset.GET(dset.attrs._req_prefix + 'DIMENSION_LIST')
            except IOError:
                return scales
            for d in dimlist['value'][self._dimension]:
                dscale_json = dset.GET('/' + d)
                dscale = Dataset(DatasetID(parent=None, item=dscale_json,
                                           http_conn=self._id.http_conn))
                scales.append((dscale.attrs['NAME'], dscale))
        return scales

    def keys(self):
        ''' Get a list of names for the scales on this dimension. '''
        with phil:
            return [key for (key, _) in self.items()]

    def values(self):
        ''' Get a list of Dataset for scales on this dimension. '''
        with phil:
            return [val for (_, val) in self.items()]

    @with_phil
    def __repr__(self):
        if not self._id:
            return '<Dimension of closed HDF5 dataset>'
        return ('<%s dimension %d of HDF5 dataset at %s>'
                % (self.label, self._dimension, id(self._id)))


class DimensionManager(base.MappingHDF5, base.CommonStateObject):
    '''
        Represents a collection of dimensions associated with a dataset.

        Like AttributeManager, an instance of this class is returned when
        accessing the '.dims' property of a Dataset.
    '''

    @with_phil
    def __init__(self, parent):
        ''' Private constructor.
        '''
        self._id = parent.id

    @with_phil
    def __getitem__(self, index):
        ''' Return a Dimension object
        '''
        if index > len(self) - 1:
            raise IndexError('Index out of range')
        return DimensionProxy(self._id, index)

    @with_phil
    def __len__(self):
        ''' Number of dimensions associated with the dataset. '''
        return len(Dataset(self._id).shape)

    @with_phil
    def __iter__(self):
        ''' Iterate over the dimensions. '''
        for i in range(len(self)):
            yield self[i]

    @with_phil
    def __repr__(self):
        if not self._id:
            return '<Dimensions of closed HDF5 dataset>'
        return '<Dimensions of HDF5 dataset at %s>' % self._id

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
        if isinstance(name, six.binary_type):
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
        req_class = dset.attrs._req_prefix + 'CLASS'
        req_name = dset.attrs._req_prefix + 'NAME'
        with phil:
            dset.PUT(req_class, body=class_attr, replace=True)
            try:
                dset.PUT(req_name, body=name_attr, replace=True)
            except Exception:
                dset.DELETE(req_class)
