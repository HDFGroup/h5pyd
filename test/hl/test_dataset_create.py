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

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase
from datetime import datetime


class TestCreateDataset(TestCase):


    def test_create_simple_dset(self):
        filename = self.getFileName("create_simple_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dims = (40, 80)
        dset = f.create_dataset('simple_dset', dims, dtype='f4')

        self.assertEqual(dset.name, "/simple_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.ndim, 2)
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
            if not config.get('use_h5py') and isinstance(f.id.id, str) and f.id.id.startswith("g-"):
                self.assertEqual(dset.num_chunks, 0)
                self.assertEqual(dset.allocated_size, 0)

        # try with chunk=True
        dset_chunked = f.create_dataset('chunked_dset', dims, dtype='f4', chunks=True)
        if config.get('use_h5py') or (isinstance(f.id.id, str) and f.id.id.startswith("g-")):
            self.assertTrue(dset_chunked.chunks)
        else:
            # h5serv not reporting chunks
            self.assertTrue(dset_chunked.chunks is None)

        f.close()

    def test_create_float16_dset(self):

        filename = self.getFileName("create_float16_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        if not config.get('use_h5py') and isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # Float16 not supported with h5serv
            return

        nrows = 4
        ncols = 8
        dims = (nrows, ncols)
        dset = f.create_dataset('simple_dset', dims, dtype='f2')

        self.assertEqual(dset.name, "/simple_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.shape[0], nrows)
        self.assertEqual(dset.shape[1], ncols)
        self.assertEqual(str(dset.dtype), 'float16')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 2)
        self.assertEqual(dset.maxshape[0], nrows)
        self.assertEqual(dset.maxshape[1], ncols)
        self.assertEqual(dset[0,0], 0)

        arr = np.zeros((nrows,ncols), dtype="f2")
        for i in range(nrows):
            for j in range(ncols):
                val  = float(i) * 10.0 + float(j)/10.0
                arr[i,j] = val

        # write entire array to dataset
        dset[...] = arr

        arr = dset[...]  # read back
        val = arr[2,4]   # test one value
        self.assertTrue(val > 20.4 - 0.01)
        self.assertTrue(val < 20.4 + 0.01)


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

    def test_fillvalue_char_dset(self):
        filename = self.getFileName("fillvalue_char_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        if not config.get('use_h5py') and isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # the following is failing on h5serv
            f.close()
            return

        dims = (6, 3)
        dtype = np.dtype("S1")
        fillvalue = b'X'
        data = [[b'a', b'', b''],
                [b'b', b'', b''],
                [b'c', b'', b''],
                [b'f', b'o', b'o'],
                [b'b', b'a', b'r'],
                [b'b', b'a', b'z']]
        dset = f.create_dataset('ds1', dims, data=data, fillvalue=fillvalue, dtype=dtype)

        self.assertEqual(dset.name, "/ds1")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.shape[0], 6)
        self.assertEqual(dset.shape[1], 3)
        self.assertEqual(str(dset.dtype), '|S1')
        self.assertEqual(dset.fillvalue, b'X')
        self.assertEqual(dset[0,0], b'a')
        self.assertEqual(dset[5,2], b'z')


        f.close()


    def test_simple_1d_dset(self):
        filename = self.getFileName("simple_1d_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dims = (10,)
        dset = f.create_dataset('simple_1d_dset', dims, dtype='uint32')

        self.assertEqual(dset.name, "/simple_1d_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.ndim, 1)
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
        if isinstance(dset.id.id, str) and dset.id.id.startswith("d-"):
            # HSDS will create a different chunk layout
            self.assertEqual(chunks[0], 40)
            self.assertEqual(chunks[1], 80)
        else:
            self.assertEqual(chunks[0], 20)
            self.assertEqual(chunks[1], 40)
        if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
            # h5serv not setting this
            self.assertEqual(dset.compression, 'gzip')
            self.assertEqual(dset.compression_opts, 9)
        self.assertFalse(dset.shuffle)

        dset_ref = f['/simple_dset_gzip']
        self.assertTrue(dset_ref is not None)
        if not config.get("use_h5py"):
            # obj ids should be the same with h5pyd (but not h5py)
            self.assertEqual(dset.id.id, dset_ref.id.id)
            # Check dataset's last modified time
            self.assertTrue(isinstance(dset.modified, datetime))
            #self.assertEqual(dset.modified.tzname(), six.u('UTC'))

        f.close()

    def test_create_dset_lz4(self):
        filename = self.getFileName("create_dset_lz4")
        print("filename:", filename)

        f = h5py.File(filename, "w")

        if config.get("use_h5py"):
            return # lz4 not supported with h5py

        if "lz4" not in f.compressors:
            print("lz4 not supproted")
            return

        dims = (40, 80)

        # create some test data
        arr = np.random.rand(dims[0], dims[1])

        dset = f.create_dataset('simple_dset_lz4', data=arr, dtype='i4',
            compression='lz4', compression_opts=5)

        self.assertEqual(dset.name, "/simple_dset_lz4")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.shape[0], 40)
        self.assertEqual(dset.shape[1], 80)
        self.assertEqual(str(dset.dtype), 'int32')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 2)
        self.assertEqual(dset.maxshape[0], 40)
        self.assertEqual(dset.maxshape[1], 80)

        chunks = dset.chunks  # chunk layout auto-generated
        self.assertTrue(chunks is not None)
        self.assertEqual(len(chunks), 2)
        if isinstance(dset.id.id, str) and dset.id.id.startswith("d-"):
            # HSDS will create a different chunk layout
            self.assertEqual(chunks[0], 40)
            self.assertEqual(chunks[1], 80)
        else:
            self.assertEqual(chunks[0], 20)
            self.assertEqual(chunks[1], 40)
        self.assertEqual(dset.compression, 'lz4')
        self.assertEqual(dset.compression_opts, 5)
        self.assertFalse(dset.shuffle)

        dset_ref = f['/simple_dset_lz4']
        self.assertTrue(dset_ref is not None)
        if not config.get("use_h5py"):
            # obj ids should be the same with h5pyd (but not h5py)
            self.assertEqual(dset.id.id, dset_ref.id.id)
            # Check dataset's last modified time
            self.assertTrue(isinstance(dset.modified, datetime))
            #self.assertEqual(dset.modified.tzname(), six.u('UTC'))

        f.close()

    def test_create_dset_gzip_and_shuffle(self):
        filename = self.getFileName("create_dset_gzip_and_shuffle")
        print("filename:", filename)

        f = h5py.File(filename, "w")

        dims = (40, 80)

        # create some test data
        arr = np.random.rand(dims[0], dims[1])
        kwds = {"chunks": (4,8)}
        dset = f.create_dataset('simple_dset_gzip_shuffle', data=arr, dtype='f8',
            compression='gzip', shuffle=True, compression_opts=9, **kwds)

        self.assertEqual(dset.name, "/simple_dset_gzip_shuffle")
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
        self.assertTrue(isinstance(chunks, tuple))
        self.assertEqual(len(chunks), 2)
        #self.assertEqual(dset.compression, 'gzip')
        #self.assertEqual(dset.compression_opts, 9)
        self.assertTrue(dset.shuffle)

        dset_ref = f['/simple_dset_gzip_shuffle']
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
        f = h5py.File(filename, "w")

        dims = (10,)
        dset = f.create_dataset('bool_dset', dims, dtype=bool)

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

    def test_require_dset(self):
        filename = self.getFileName("require_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        self.assertEqual(len(f), 0)

        dims = (40, 80)
        dset = f.require_dataset('dset', dims, dtype='f8')

        self.assertEqual(dset.name, "/dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.ndim, 2)
        self.assertEqual(dset.shape[0], 40)
        self.assertEqual(dset.shape[1], 80)
        self.assertEqual(str(dset.dtype), 'float64')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 2)
        self.assertEqual(dset.maxshape[0], 40)
        self.assertEqual(dset.maxshape[1], 80)
        self.assertEqual(dset[0,0], 0)

        self.assertEqual(len(f), 1)

        dset_2 = f.require_dataset('dset', dims, dtype='f8')
        if not config.get("use_h5py"):
            self.assertEqual(dset.id.id, dset_2.id.id)
        self.assertEqual(len(f), 1)

        dset_3 = f.require_dataset('dset', dims, dtype='f4')
        if not config.get("use_h5py"):
            self.assertEqual(dset.id.id, dset_3.id.id)
        self.assertEqual(str(dset_3.dtype), 'float64')

        self.assertEqual(len(f), 1)

        try: 
            f.require_dataset('dset', dims, dtype='f4', exact=True)
            self.assertTrue(False)  # exception expected
        except TypeError:
            pass

        self.assertEqual(len(f), 1)

        f.close()

    def test_create_dset_like(self):
        filename = self.getFileName("create_dset_like")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        def check_props(dset):
            self.assertTrue(isinstance(dset.shape, tuple))
            self.assertEqual(len(dset.shape), 2)
            self.assertEqual(dset.ndim, 2)
            self.assertEqual(dset.shape[0], 40)
            self.assertEqual(dset.shape[1], 80)
            self.assertEqual(str(dset.dtype), 'float32')
            self.assertTrue(isinstance(dset.maxshape, tuple))
            self.assertEqual(len(dset.maxshape), 2)
            self.assertEqual(dset.maxshape[0], 40)
            self.assertEqual(dset.maxshape[1], 80)
            self.assertEqual(dset[0,0], 0)

        dims = (40, 80)
        dset = f.create_dataset('simple_dset', dims, dtype='f4')

        self.assertEqual(dset.name, '/simple_dset')
        check_props(dset)
        
        dset_copy = f.create_dataset_like('similar_dset', dset)
        self.assertEqual(dset_copy.name, '/similar_dset')
        check_props(dset_copy)

        self.assertEqual(len(f), 2)

        f.close()

    def test_create_dset_empty(self):
        filename = self.getFileName("create_dset_empty")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        def check_props(dset):
            self.assertEqual(dset.shape, None)
            self.assertEqual(str(dset.dtype), 'float32')

        # create by providing a type
        dset1 = f.create_dataset('dset1', dtype='f4')
        self.assertEqual(dset1.name, '/dset1')
        check_props(dset1)

        # create using the Empty object
        dset2 = f.create_dataset('dset2', data=h5py.Empty("float32"))
        self.assertEqual(dset2.name, '/dset2')
        check_props(dset2)

        f.close()

    def test_creat_anon_dataset(self):

        def validate_dset(dset):
            self.assertEqual(dset.name, None)
            self.assertTrue(isinstance(dset.shape, tuple))
            self.assertEqual(len(dset.shape), 2)
            self.assertEqual(dset.ndim, 2)
            self.assertEqual(dset.shape[0], 40)
            self.assertEqual(dset.shape[1], 80)
            self.assertEqual(str(dset.dtype), 'float32')
            self.assertTrue(isinstance(dset.maxshape, tuple))
            self.assertEqual(len(dset.maxshape), 2)
            self.assertEqual(dset.maxshape[0], 40)
            self.assertEqual(dset.maxshape[1], 80)
            self.assertEqual(dset[0,0], 0)

        filename = self.getFileName("create_anon_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dims = (40, 80)
        dset = f.create_dataset(None, dims, dtype='f4')

        validate_dset(dset)

        dset_id = dset.id.id
        if not config.get("use_h5py"):
            # Check dataset's last modified time
            self.assertTrue(isinstance(dset.modified, datetime))

            # test h5pyd extensions
            if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
                self.assertEqual(dset.num_chunks, 0)
                self.assertEqual(dset.allocated_size, 0)


        f.close()

        f = h5py.File(filename, "a")  # re-open
        num_links = len(f)
        self.assertEqual(num_links, 0)
        if not config.get("use_h5py"):
            # can get a reference to the dataset using the dataset id
            uuid_ref = f"datasets/{dset_id}"
            dset = f[uuid_ref]
            validate_dset(dset)
            self.assertEqual(dset.id.id, dset_id)

            # explictly delete dataset
            del f[uuid_ref]

            # should not be returned now
            try:
                dset = f[uuid_ref]
                print(f"didn't expect to get: {dset}")
                self.asertTrue(False)
            except IOError:
                pass # expected
        f.close()     


        
if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
