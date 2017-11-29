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

import numpy as np
import six
import config
from common import ut, TestCase

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py


class TestDimensionScale(TestCase):

    def test_everything(self):
        """Everything related to dimension scales"""
        filename = self.getFileName('test_dimscale')
        print('filename:', filename)
        f = h5py.File(filename, 'w')
        is_h5serv = False
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # HSDS currently supports dimscales, but h5serv does not
            is_h5serv = True

        dset = f.create_dataset('temperatures', (10, 10, 10), dtype='f')
        f.create_dataset('scale_x', data=np.arange(10) * 10e3)
        f.create_dataset('scale_y', data=np.arange(10) * 10e3)
        f.create_dataset('scale_z', data=np.arange(10) * 10e3)
        f.create_dataset('not_scale', data=np.arange(10) * 10e3)

        self.assertIsInstance(dset.dims, h5py._hl.dims.DimensionManager)
        self.assertEqual(len(dset.dims), len(dset.shape))
        for d in dset.dims:
            self.assertIsInstance(d, h5py._hl.dims.DimensionProxy)

        if is_h5serv:
            f.close()  # can't go any farther with h5serv
            return

        # Create and name dimension scales
        dset.dims.create_scale(f['scale_x'], 'Simulation X (North) axis')
        dset.dims.create_scale(f['scale_y'], 'Simulation Y (East) axis')
        dset.dims.create_scale(f['scale_z'], 'Simulation Z (Vertical) axis')

        # Attach a non-dimension scale (and in the process make it a dimension
        # scale)
        dset.dims[1].attach_scale(f['not_scale'])

        # Cannot attach a dimension scale to another dimension scale
        with self.assertRaises(RuntimeError):
            f['scale_x'].dims[0].attach_scale(f['scale_z'])

        self.assertEqual(len(dset.dims[0]), 0)

        dset.dims[0].attach_scale(f['scale_x'])

        self.assertEqual(len(dset.dims[0]), 1)
        self.assertEqual(len(dset.dims[1]), 1)
        self.assertEqual(len(dset.dims[2]), 0)

        dset.dims[1].attach_scale(f['scale_y'])
        dset.dims[2].attach_scale(f['scale_z'])

        self.assertEqual(len(dset.dims[1]), 2)
        self.assertEqual(len(dset.dims[2]), 1)

        scale_x = f['scale_x']
        self.assertEqual(len(scale_x.dims), len(scale_x.shape))
        self.assertEqual(list(scale_x.dims[0]), [])

        dset.dims[1].detach_scale(f['scale_y'])
        self.assertEqual(len(dset.dims[1]), 1)
        dset.dims[1].detach_scale(f['not_scale'])
        self.assertEqual(dset.dims[1].items(), [])

        self.assertEqual(dset.dims[0].label, '')
        self.assertEqual(dset.dims[1].label, '')
        self.assertEqual(dset.dims[2].label, '')

        dset.dims[0].label = 'x'
        dset.dims[1].label = 'y'
        dset.dims[2].label = 'z'

        self.assertEqual(dset.dims[0].label, 'x')
        self.assertEqual(dset.dims[1].label, 'y')
        self.assertEqual(dset.dims[2].label, 'z')

        for s in dset.dims[2].items():
            self.assertIsInstance(s, tuple)
            self.assertIsInstance(s[1], h5py.Dataset)
            if config.get("use_h5py"):
                self.assertIsInstance(s[0], six.string_types)
                self.assertEqual(s[0], 'Simulation Z (Vertical) axis')
            else:
                self.assertIsInstance(s[0], six.binary_type)
                self.assertEqual(s[0], b'Simulation Z (Vertical) axis')

        self.assertIsInstance(dset.dims[0][0], h5py.Dataset)
        self.assertIsInstance(dset.dims[0]['Simulation X (North) axis'],
                              h5py.Dataset)

        with self.assertRaises(IndexError):
            dset.dims[0][10]

        with self.assertRaises(KeyError):
            dset.dims[0]['foobar']

        f.close()


if __name__ == '__main__':
    ut.main()
