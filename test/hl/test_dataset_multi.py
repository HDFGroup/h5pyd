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
    Dataset multi testing operations.

    Tests all MultiManager operations
"""

import logging
import numpy as np

from common import ut, TestCase
import config

if config.get("use_h5py"):
    import h5py
else:
    from h5pyd import MultiManager
    import h5pyd as h5py


@ut.skipIf(config.get('use_h5py'), "h5py does not support MultiManager")
class TestMultiManager(TestCase):
    def test_multi_read_scalar_dataspaces(self):
        """
        Test reading from multiple datasets with scalar dataspaces
        """
        filename = self.getFileName("multi_read_scalar_dataspaces")
        print("filename:", filename)
        print(f"numpy version: {np.version.version}")
        f = h5py.File(filename, 'w')
        shape = ()
        count = 3
        dt = np.int32

        # Create datasets
        data_in = np.array(1, dtype=dt)
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape, dtype=dt, data=(data_in + i))
            datasets.append(dset)

        mm = MultiManager(datasets)

        # Select via empty tuple
        data_out = mm[()]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            np.testing.assert_array_equal(data_out[i], data_in + i)

        # Select via Ellipsis
        data_out = mm[...]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            np.testing.assert_array_equal(data_out[i], data_in + i)

    def test_multi_read_non_scalar_dataspaces(self):
        """
        Test reading from multiple datasets with non-scalar dataspaces
        """
        filename = self.getFileName("multi_read_non_scalar_dataspaces")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = (10, 10, 10)
        count = 3
        dt = np.int32

        # Create datasets
        data_in = np.reshape(np.arange(np.prod(shape)), shape)
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape, dtype=dt, data=(data_in + i))
            datasets.append(dset)

        mm = MultiManager(datasets)
        data_out = mm[...]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            np.testing.assert_array_equal(data_out[i], data_in + i)

        # Partial Read
        data_out = mm[:, :, 0]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            np.testing.assert_array_equal(data_out[i], (data_in + i)[:, :, 0])

    def test_multi_read_mixed_dataspaces(self):
        """
        Test reading from multiple datasets with scalar and
        non-scalar dataspaces
        """
        filename = self.getFileName("multi_read_mixed_dataspaces")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        scalar_shape = ()
        shape = (10, 10, 10)
        count = 3
        dt = np.int32

        # Create datasets
        data_scalar_in = np.array(1)
        data_nonscalar_in = np.reshape(np.arange(np.prod(shape)), shape)
        data_in = [data_scalar_in, data_nonscalar_in,
                   data_nonscalar_in, data_nonscalar_in]
        datasets = []

        for i in range(count):
            if i == 0:
                dset = f.create_dataset("data" + str(0), scalar_shape, dtype=dt, data=data_scalar_in)
            else:
                dset = f.create_dataset("data" + str(i), shape, dtype=dt, data=(data_nonscalar_in + i))
            datasets.append(dset)

        # Set up MultiManager for read
        mm = MultiManager(datasets=datasets)

        # Select via empty tuple
        data_out = mm[()]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            if i == 0:
                np.testing.assert_array_equal(data_out[i], data_in[i])
            else:
                np.testing.assert_array_equal(data_out[i], data_in[i] + i)

        # Select via Ellipsis
        data_out = mm[...]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            if i == 0:
                np.testing.assert_array_equal(data_out[i], data_in[i])
            else:
                np.testing.assert_array_equal(data_out[i], data_in[i] + i)

    def test_multi_read_mixed_types(self):
        """
        Test reading from multiple datasets with different types
        """
        filename = self.getFileName("multi_read_mixed_types")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = (10, 10, 10)
        count = 4
        dts = [np.int32, np.int64, np.float64, np.dtype("S10")]

        # Create datasets
        data_in = np.reshape(np.arange(np.prod(shape)), shape)
        data_in_fixed_str = np.full(shape, "abcdefghij", dtype=dts[3])
        datasets = []

        for i in range(count):
            if i < 3:
                dset = f.create_dataset("data" + str(i), shape, dtype=dts[i], data=(data_in + i))
            else:
                dset = f.create_dataset("data" + str(i), shape, dtype=dts[i], data=data_in_fixed_str)

            datasets.append(dset)

        # Set up MultiManager for read
        mm = MultiManager(datasets=datasets)

        # Perform read
        data_out = mm[...]

        self.assertEqual(len(data_out), count)

        for i in range(count):
            if i < 3:
                np.testing.assert_array_equal(data_out[i], np.array(data_in + i, dtype=dts[i]))
            else:
                np.testing.assert_array_equal(data_out[i], data_in_fixed_str)

            self.assertEqual(data_out[i].dtype, dts[i])

    def test_multi_read_vlen_str(self):
        """
        Test reading from multiple datasets with a vlen string type
        """
        filename = self.getFileName("multi_read_vlen_str")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = (10, 10, 10)
        count = 3
        dt = h5py.string_dtype(encoding='utf-8')
        data_in = np.full(shape, "abcdefghij", dt)
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape=shape, data=data_in, dtype=dt)
            datasets.append(dset)

        mm = MultiManager(datasets=datasets)
        out = mm[...]

        self.assertEqual(len(out), count)

        for i in range(count):
            self.assertEqual(out[i].dtype, dt)
            out[i] = np.reshape(out[i], shape=np.prod(shape))
            out[i] = np.reshape(np.array([s.decode() for s in out[i]], dtype=dt), shape=shape)
            np.testing.assert_array_equal(out[i], data_in)

    def test_multi_read_mixed_shapes(self):
        """
        Test reading a selection from multiple datasets with different shapes
        """
        filename = self.getFileName("multi_read_mixed_shapes")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shapes = [(150), (10, 15), (5, 5, 6)]
        count = 3
        dt = np.int32
        data = np.arange(150, dtype=dt)
        data_in = [np.reshape(data, shape=s) for s in shapes]
        datasets = []
        sel_idx = 2

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape=shapes[i], dtype=dt, data=data_in[i])
            datasets.append(dset)

        mm = MultiManager(datasets=datasets)
        # Perform multi read with selection
        out = mm[sel_idx]

        # Verify
        for i in range(count):
            np.testing.assert_array_equal(out[i], data_in[i][sel_idx])

    def test_multi_write_scalar_dataspaces(self):
        """
        Test writing to multiple scalar datasets
        """
        filename = self.getFileName("multi_write_scalar_dataspaces")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = ()
        count = 3
        dt = np.int32

        # Create datasets
        zeros = np.zeros(shape, dtype=dt)
        data_in = []
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape, dtype=dt, data=zeros)
            datasets.append(dset)

            data_in.append(np.array([i]))

        mm = MultiManager(datasets)
        # Perform write
        mm[...] = data_in

        # Read back and check
        for i in range(count):
            data_out = f["data" + str(i)][...]
            np.testing.assert_array_equal(data_out, data_in[i])

    def test_multi_write_non_scalar_dataspaces(self):
        """
        Test writing to multiple non-scalar datasets
        """
        filename = self.getFileName("multi_write_non_scalar_dataspaces")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = (10, 10, 10)
        count = 3
        dt = np.int32

        # Create datasets
        zeros = np.zeros(shape, dtype=dt)
        data_in = []
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape, dtype=dt, data=zeros)
            datasets.append(dset)

            d_in = np.array(np.reshape(np.arange(np.prod(shape)), shape) + i, dtype=dt)
            data_in.append(d_in)

        mm = MultiManager(datasets)
        # Perform write
        mm[...] = data_in

        # Read back and check
        for i in range(count):
            data_out = np.array(f["data" + str(i)][...], dtype=dt)
            np.testing.assert_array_equal(data_out, data_in[i])

    def test_multi_write_mixed_dataspaces(self):
        """
        Test writing to multiple scalar and non-scalar datasets
        """
        filename = self.getFileName("multi_write_mixed_dataspaces")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        scalar_shape = ()
        shape = (10, 10, 10)
        count = 3
        dt = np.int32

        # Create datasets
        data_in = []
        data_scalar_in = np.array(1, dtype=dt)
        data_nonscalar_in = np.array(np.reshape(np.arange(np.prod(shape)), shape), dtype=dt)
        datasets = []

        for i in range(count):
            if i == 0:
                dset = f.create_dataset("data" + str(0), scalar_shape, dtype=dt, data=np.array(0, dtype=dt))
                data_in.append(data_scalar_in)
            else:
                dset = f.create_dataset("data" + str(i), shape, dtype=dt, data=np.zeros(shape))
                data_in.append(data_nonscalar_in)
            datasets.append(dset)

        # Set up MultiManager for write
        mm = MultiManager(datasets=datasets)

        # Select via empty tuple
        mm[()] = data_in

        for i in range(count):
            data_out = f["data" + str(i)][...]
            np.testing.assert_array_equal(data_out, data_in[i])

        # Reset datasets
        for i in range(count):
            if i == 0:
                zeros = np.array([0])
            else:
                zeros = np.zeros(shape)
            f["data" + str(i)][...] = zeros

        # Select via Ellipsis
        mm[...] = data_in

        for i in range(count):
            data_out = f["data" + str(i)][...]

            if i == 0:
                np.testing.assert_array_equal(data_out, data_in[i])
            else:
                np.testing.assert_array_equal(data_out, data_in[i])

    def test_multi_write_vlen_str(self):
        """
        Test writing to multiple datasets with a vlen string type
        """
        filename = self.getFileName("multi_write_vlen_str")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = (10, 10, 10)
        count = 3
        dt = h5py.string_dtype(encoding='utf-8')
        data_initial_vlen = np.full(shape, "aaaabbbbcc", dtype=dt)
        data_in_vlen = np.full(shape, "abcdefghij", dtype=dt)
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape=shape, data=data_initial_vlen, dtype=dt)
            datasets.append(dset)

        mm = MultiManager(datasets=datasets)
        # Perform write
        mm[...] = [data_in_vlen, data_in_vlen, data_in_vlen]

        # Verify
        for i in range(count):
            out = f["data" + str(i)][...]
            self.assertEqual(out.dtype, dt)

            out = np.reshape(out, shape=np.prod(shape))
            out = np.reshape(np.array([s.decode() for s in out], dtype=dt), shape=shape)
            np.testing.assert_array_equal(out, data_in_vlen)

    def test_multi_write_mixed_shapes(self):
        """
        Test writing to a selection in multiple datasets with different shapes
        """
        filename = self.getFileName("multi_write_mixed_shapes")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shapes = [(50, 5), (15, 10), (20, 15)]
        count = 3
        dt = np.int32
        data_in = 99
        datasets = []
        sel_idx = 2

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape=shapes[i], dtype=dt, data=np.zeros(shapes[i], dtype=dt))
            datasets.append(dset)

        mm = MultiManager(datasets=datasets)
        # Perform multi write with selection
        mm[sel_idx, sel_idx] = [data_in, data_in + 1, data_in + 2]

        # Verify
        for i in range(count):
            out = f["data" + str(i)][...]
            np.testing.assert_array_equal(out[sel_idx, sel_idx], data_in + i)

    def test_multi_selection(self):
        """
        Test using a different selection
        for each dataset in a MultiManager
        """
        filename = self.getFileName("multi_selection")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        shape = (10, 10, 10)
        count = 3
        dt = np.int32

        # Create datasets
        data_in = np.reshape(np.arange(np.prod(shape), dtype=dt), shape)
        data_in_original = data_in.copy()
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape=shape, dtype=dt, data=data_in)
            datasets.append(dset)

        mm = h5py.MultiManager(datasets=datasets)

        # Selections to read from
        sel = [np.s_[0:10, 0:10, 0:10], np.s_[0:5, 5:10, 1:4:2], np.s_[4, 5, 6]]
        data_out = mm[sel]

        for i in range(count):
            np.testing.assert_array_equal(data_out[i], data_in[sel[i]])

        # If selection list has only a single element, apply it to all dsets
        sel = [np.s_[0:10, 0:10, 0:10]]
        data_out = mm[sel]

        for d in data_out:
            np.testing.assert_array_equal(d, data_in[sel[0]])

        # Selections to write to
        sel = [np.s_[0:10, 0:10, 0:10], np.s_[0:5, 0:5, 0:5], np.s_[0, 0, 0]]
        data_in = [np.zeros_like(data_in), np.ones_like(data_in), np.full_like(data_in, 2)]
        mm[sel] = [data_in[i][sel[i]] for i in range(count)]

        for i in range(count):
            np.testing.assert_array_equal(f["data" + str(i)][sel[i]], data_in[i][sel[i]])

        # Check that unselected regions are unmodified
        np.testing.assert_array_equal(f["data1"][5:, 5:, 5:], data_in_original[5:, 5:, 5:])
        np.testing.assert_array_equal(f["data2"][1:, 1:, 1:], data_in_original[1:, 1:, 1:])

        # Save for later comparison
        data_in_original = mm[...]

        # If selection list has only a single element, apply it to all dsets
        sel = [np.s_[0:6, 0:6, 0:6]]
        data_in = np.full(shape, 3, dtype=dt)
        mm[sel] = [data_in[sel[0]]] * count

        for i in range(count):
            np.testing.assert_array_equal(f["data" + str(i)][sel[0]], data_in[sel[0]])

        # Check that unselected regions are unmodified
        data_out = mm[...]

        for i in range(count):
            np.testing.assert_array_equal(data_out[i][6:, 6:, 6:], data_in_original[i][6:, 6:, 6:])

    def test_multi_field_selection(self):
        """
        Test reading/writing to a field selection on multiple datasets
        """
        filename = self.getFileName("multi_field_selection")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        dt = np.dtype([('a', np.float32), ('b', np.int32), ('c', np.float32)])
        shape = (100,)
        data = np.ones(shape, dtype=dt)
        count = 3
        datasets = []

        for i in range(count):
            dset = f.create_dataset("data" + str(i), shape=shape, data=np.zeros(shape, dtype=dt), dtype=dt)
            datasets.append(dset)

        # Perform read from field 'b'
        mm = MultiManager(datasets=datasets)
        out = mm[..., 'b']

        # Verify data returned
        for i in range(count):
            np.testing.assert_array_equal(out[i], np.zeros(shape, dtype=dt['b']))

        # Perform write to field 'b'
        mm = MultiManager(datasets=datasets)
        mm[..., 'b'] = [data['b'], data['b'], data['b']]

        for i in range(count):
            out = np.array(f["data" + str(i)], dtype=dt)
            np.testing.assert_array_equal(out['a'], np.zeros(shape, dtype=dt['a']))
            np.testing.assert_array_equal(out['b'], data['b'])
            np.testing.assert_array_equal(out['c'], np.zeros(shape, dtype=dt['c']))

        # Test writing to entire compound type
        data = np.zeros(shape, dtype=dt)
        mm[...] = [data, data, data]

        for i in range(count):
            out = np.array(f["data" + str(i)], dtype=dt)
            np.testing.assert_array_equal(out, data)


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
