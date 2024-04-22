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
import logging
import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestScalarCompound(TestCase):

    def setUp(self):
        filename = self.getFileName("scalar_compound_dset")
        print("filename:", filename)
        self.f = h5py.File(filename, "w")
        self.data = np.array((42.5, -118, "Hello"), dtype=[('a', 'f'), ('b', 'i'), ('c', '|S10')])
        self.dset = self.f.create_dataset('x', data=self.data)

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 0)

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, tuple())

    def test_size(self):
        """ Verify size """
        self.assertEqual(self.dset.size, 1)

    def test_ellipsis(self):
        """ Ellipsis -> scalar ndarray """
        out = self.dset[...]
        # assertArrayEqual doesn't work with compounds; do manually
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.shape, self.data.shape)
        self.assertEqual(out.dtype, self.data.dtype)

    def test_tuple(self):
        """ () -> np.void instance """
        out = self.dset[()]
        self.assertIsInstance(out, np.void)
        self.assertEqual(out.dtype, self.data.dtype)

    def test_slice(self):
        """ slice -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0:4]

    def test_index(self):
        """ index -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0]

    def test_rt(self):
        """ Compound types are read back in correct order (h5py issue 236)"""

        dt = np.dtype([('weight', np.float64),
                       ('cputime', np.float64),
                       ('walltime', np.float64),
                       ('parents_offset', np.uint32),
                       ('n_parents', np.uint32),
                       ('status', np.uint8),
                       ('endpoint_type', np.uint8),])

        testdata = np.ndarray((16,), dtype=dt)
        for key in dt.fields:
            testdata[key] = np.random.random((16,)) * 100

        self.f['test'] = testdata
        outdata = self.f['test'][...]
        self.assertTrue(np.all(outdata == testdata))
        self.assertEqual(outdata.dtype, testdata.dtype)

    def test_assign(self):
        dt = np.dtype([('weight', (np.float64)),
                       ('endpoint_type', np.uint8),])

        testdata = np.ndarray((16,), dtype=dt)
        for key in dt.fields:
            testdata[key] = np.random.random(size=testdata[key].shape) * 100

        ds = self.f.create_dataset('test', (16,), dtype=dt)
        for key in dt.fields:
            ds[key] = testdata[key]

        outdata = self.f['test'][...]

        self.assertTrue(np.all(outdata == testdata))
        self.assertEqual(outdata.dtype, testdata.dtype)

    def test_read(self):
        dt = np.dtype([('weight', (np.float64)),
                       ('endpoint_type', np.uint8),])

        testdata = np.ndarray((16,), dtype=dt)
        for key in dt.fields:
            testdata[key] = np.random.random(size=testdata[key].shape) * 100

        ds = self.f.create_dataset('test', (16,), dtype=dt)

        # Write to all fields
        ds[...] = testdata

        for key in dt.fields:
            outdata = self.f['test'][key]
            np.testing.assert_array_equal(outdata, testdata[key])
            self.assertEqual(outdata.dtype, testdata[key].dtype)

    """
    TBD
    def test_nested_compound_vlen(self):
        dt_inner = np.dtype([('a', h5py.vlen_dtype(np.int32)),
                            ('b', h5py.vlen_dtype(np.int32))])

        dt = np.dtype([('f1', h5py.vlen_dtype(dt_inner)),
                       ('f2', np.int64)])

        inner1 = (np.array(range(1, 3), dtype=np.int32),
                  np.array(range(6, 9), dtype=np.int32))

        inner2 = (np.array(range(10, 14), dtype=np.int32),
                  np.array(range(16, 21), dtype=np.int32))

        data = np.array([(np.array([inner1, inner2], dtype=dt_inner), 2),
                        (np.array([inner1], dtype=dt_inner), 3)],
                        dtype=dt)

        self.f["ds"] = data
        out = self.f["ds"]

        # Specifying check_alignment=False because vlen fields have 8 bytes of padding
        # because the vlen datatype in hdf5 occupies 16 bytes
        self.assertArrayEqual(out, data, check_alignment=False)
    """


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
