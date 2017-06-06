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
import logging
import numpy as np
import math

import config

if config.get("use_h5py"):
    print("use_h5py")
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase
from datetime import datetime
import six


class TestCreateDataset(TestCase):
    def test_create_simple_dset(self):
        filename = self.getFileName("create_simple_dset")
        print("filename:", filename)
        print("h5py:", h5py.__name__)
        f = h5py.File(filename, "w")

        dims = (40, 80)
        dset = f.create_dataset('simple_dset', dims, dtype='f4')

        self.assertEqual(dset.name, "/simple_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.shape[0], 40)
        self.assertEqual(dset.shape[1], 80)
        self.assertEqual(str(dset.dtype), 'float32')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 2)
        self.assertEqual(dset.maxshape[0], 40)
        self.assertEqual(dset.maxshape[1], 80)
        self.assertEqual(dset[0,0], 0)

        dset_ref = f['/simple_dset']
        self.assertTrue(dset_ref is not None)
        if not config.get("use_h5py"):
            # obj ids should be the same with h5pyd (but not h5py)
            self.assertEqual(dset.id.id, dset_ref.id.id)
            # Check dataset's last modified time
            self.assertTrue(isinstance(dset.modified, datetime))

        if h5py.__name__ == "h5pyd":
            # test h5pyd extensions
            print("test h5pyd extensions")
            self.assertEqual(dset.num_chunks, None)
            self.assertEqual(dset.allocated_size, None)

        f.close()

    def test_fillvalue_simple_dset(self):
        filename = self.getFileName("fillvalue_simple_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dims = (10,)
        dset = f.create_dataset('fillvalue_simple_dset', dims, fillvalue=0xdeadbeef, dtype='uint32')

        self.assertEqual(dset.name, "/fillvalue_simple_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 10)
        self.assertEqual(str(dset.dtype), 'uint32')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 10)
        self.assertEqual(dset.fillvalue, 0xdeadbeef)
        self.assertEqual(dset[0], 0xdeadbeef)

        f.close()

    def test_simple_1d_dset(self):
        filename = self.getFileName("simple_1d_dset")
        print("filename:", filename)
        print("h5py:", h5py.__name__)
        f = h5py.File(filename, "w")

        dims = (10,)
        dset = f.create_dataset('simple_1d_dset', dims, dtype='uint32')

        self.assertEqual(dset.name, "/simple_1d_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 10)
        self.assertEqual(str(dset.dtype), 'uint32')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 10)
        self.assertEqual(dset.fillvalue, 0)

        self.assertEqual(dset[0], 0)
        
        dset[:] = np.ones((10,), dtype='uint32')
        vals = dset[:]  # read back
        for i in range(10):
            self.assertEqual(vals[i], 1)

        # Write 2's to the first five elements
        dset[0:5] = [2,] * 5
        vals = dset[:]


        f.close()

    def test_fixed_len_str_dset(self):
        filename = self.getFileName("fixed_len_str_dset")
        print("filename:", filename)
        print("h5py:", h5py.__name__)
        f = h5py.File(filename, "w")

        dims = (10,)
        dset = f.create_dataset('fixed_len_str_dset', dims, dtype='|S6')

        self.assertEqual(dset.name, "/fixed_len_str_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 10)
        self.assertEqual(str(dset.dtype), '|S6')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 10)
        self.assertEqual(dset.fillvalue, b'')

        self.assertEqual(dset[0], b'')
        
        words = (b"one", b"two", b"three", b"four", b"five", b"six", b"seven", b"eight", b"nine", b"ten")
        dset[:] = words
        vals = dset[:]  # read back
        for i in range(10):
            self.assertEqual(vals[i], words[i])

        
        f.close()

    def test_create_dset_by_path(self):
        filename = self.getFileName("create_dset_by_path")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dims = (40,)
        dset = f.create_dataset('/mypath/simple_dset', dims, dtype='i8')

        self.assertEqual(dset.name, "/mypath/simple_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 40)
        self.assertEqual(str(dset.dtype), 'int64')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 40)

        grp = f['/mypath']
        dset_ref = grp['simple_dset']
        self.assertTrue(dset_ref is not None)
        if not config.get("use_h5py"):
            # obj ids should be the same with h5pyd (but not h5py)
            self.assertEqual(dset.id.id, dset_ref.id.id)
            # Check dataset's last modified time
            self.assertTrue(isinstance(dset.modified, datetime))
            #self.assertEqual(dset.modified.tzname(), six.u('UTC'))

        f.close()

    def test_create_dset_gzip(self):
        filename = self.getFileName("create_dset_gzip")
        print("filename:", filename)

        f = h5py.File(filename, "w")

        dims = (40, 80)

        # create some test data
        arr = np.random.rand(dims[0], dims[1])

        dset = f.create_dataset('simple_dset_gzip', data=arr, dtype='f8',
            compression='gzip', compression_opts=9)

        self.assertEqual(dset.name, "/simple_dset_gzip")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.shape[0], 40)
        self.assertEqual(dset.shape[1], 80)
        self.assertEqual(str(dset.dtype), 'float64')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 2)
        self.assertEqual(dset.maxshape[0], 40)
        self.assertEqual(dset.maxshape[1], 80)

        chunks = dset.chunks  # chunk layout auto-generated
        self.assertTrue(chunks is not None)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], 20)
        self.assertEqual(chunks[1], 40)
        self.assertEqual(dset.compression, 'gzip')
        self.assertEqual(dset.compression_opts, 9)

        dset_ref = f['/simple_dset_gzip']
        self.assertTrue(dset_ref is not None)
        if not config.get("use_h5py"):
            # obj ids should be the same with h5pyd (but not h5py)
            self.assertEqual(dset.id.id, dset_ref.id.id)
            # Check dataset's last modified time
            self.assertTrue(isinstance(dset.modified, datetime))
            #self.assertEqual(dset.modified.tzname(), six.u('UTC'))

        f.close()

    def test_bool_dset(self):
        filename = self.getFileName("bool_dset")
        print("filename:", filename)
        print("h5py:", h5py.__name__)
        f = h5py.File(filename, "w")

        dims = (10,)
        dset = f.create_dataset('bool_dset', dims, dtype=np.bool)

        self.assertEqual(dset.name, "/bool_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 10)
        self.assertEqual(str(dset.dtype), 'bool')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 10)
        self.assertEqual(dset.fillvalue, 0)

        self.assertEqual(dset[0], False)
        
        
        vals = dset[:]  # read back
        for i in range(10):
            self.assertEqual(vals[i], False)

        # Write True's to the first five elements
        dset[0:5] = [True,]*5

        dset = None
        dset = f["/bool_dset"]

        # read back
        vals = dset[...]
        for i in range(5):
            if i<5:
                self.assertEqual(vals[i], True)
            else:
                self.assertEqual(vals[i], False)
           
        f.close()


if __name__ == '__main__':
    loglevel = logging.DEBUG
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
