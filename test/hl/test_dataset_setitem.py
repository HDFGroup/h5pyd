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


class TestSetItemDataset(TestCase):
    def test_set_all(self):
        filename = self.getFileName("dset_set_all")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        extent = 10
        dset = f.create_dataset('dset', (extent, extent), dtype='f8')
        arr = np.random.rand(extent, extent)
        dset[()] = arr
        f.close()

    def test_broadcast(self):
        filename = self.getFileName("dset_broadcast")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        dset = f.create_dataset("dset", (4, 5), dtype=np.int32)
        dset[...] = 42
        for i in range(4):
            self.assertEqual(dset[i, i], 42)
        f.close()

    def test_type_conversion(self):
        filename = self.getFileName("dset_type_conversion")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        dset = f.create_dataset("dset", (4,), dtype=np.int32)
        dset[...] = np.arange(4.0)
        for i in range(4):
            self.assertEqual(dset[i], 0.0 + i)
        f.close()

    def test_setitem_fancy_indexing(self):
        # https://github.com/h5py/h5py/issues/1593
        filename = self.getFileName("dset_setitem_fancy_indexing")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        dset = f.create_dataset('data', (5, 1000, 2), dtype=np.uint8)
        block = np.random.randint(255, size=(5, 3, 2)).astype(np.uint8)
        dset[:, [0, 2, 4], ...] = block

        # read back in full rather than with a fancy selection: h5json can't
        # yet intersect a fancy read with a pending (unflushed) fancy write
        out = dset[...]
        np.testing.assert_array_equal(out[:, [0, 2, 4], :], block)
        # unselected columns are untouched
        np.testing.assert_array_equal(out[:, [1, 3, 5], :], np.zeros((5, 3, 2)))
        f.close()


class TestSetItemBroadcast(TestCase):
    """ Non-scalar NumPy broadcasting for simple (slice/integer) indexing,
    matching h5py.  See https://github.com/HDFGroup/h5pyd/issues/304 """

    def setUp(self):
        super().setUp()
        filename = self.getFileName(f"dset_bcast_{self._testMethodName}")
        self.f = h5py.File(filename, "w")

    def test_broadcast_row(self):
        """ (1, N) value broadcast across all rows (issue #304 repro) """
        dset = self.f.create_dataset("dset", (2, 3), dtype=np.int32)
        dset[...] = [[1, 2, 3]]
        np.testing.assert_array_equal(dset[...], [[1, 2, 3], [1, 2, 3]])

        dset[...] = np.array([[4, 5, 6]], dtype=np.int32)
        np.testing.assert_array_equal(dset[...], [[4, 5, 6], [4, 5, 6]])

    def test_broadcast_lower_rank(self):
        """ (N,) value broadcast to a 2-D selection """
        dset = self.f.create_dataset("dset", (2, 3), dtype=np.int32)
        dset[...] = np.array([1, 2, 3])
        np.testing.assert_array_equal(dset[...], [[1, 2, 3], [1, 2, 3]])

    def test_broadcast_column(self):
        """ (M, 1) value broadcast across all columns """
        dset = self.f.create_dataset("dset", (2, 3), dtype=np.int32)
        dset[...] = [[1], [2]]
        np.testing.assert_array_equal(dset[...], [[1, 1, 1], [2, 2, 2]])

    def test_broadcast_extra_leading_ones(self):
        """ value with more dims than the selection, extra dims of size 1 """
        dset = self.f.create_dataset("dset", (2, 3), dtype=np.int32)
        dset[...] = np.array([[[1, 2, 3]]])
        np.testing.assert_array_equal(dset[...], [[1, 2, 3], [1, 2, 3]])

    def test_broadcast_partial_selection(self):
        """ broadcast into a sub-region; elements outside it are untouched """
        dset = self.f.create_dataset("dset", (4, 3), dtype=np.int32)
        dset[1:3, :] = [[7, 8, 9]]
        np.testing.assert_array_equal(
            dset[...], [[0, 0, 0], [7, 8, 9], [7, 8, 9], [0, 0, 0]])

    def test_broadcast_strided_selection(self):
        """ broadcast into a strided hyperslab """
        dset = self.f.create_dataset("dset", (4, 3), dtype=np.int32)
        dset[0:4:2, :] = [[7, 8, 9]]
        np.testing.assert_array_equal(
            dset[...], [[7, 8, 9], [0, 0, 0], [7, 8, 9], [0, 0, 0]])

    def test_integer_index_extra_leading_one(self):
        """ an integer index drops that axis from the selection shape;
        a value with a leading size-1 dim must still be accepted """
        dset = self.f.create_dataset("dset", (2, 3), dtype=np.int32)
        dset[0, :] = [[1, 2, 3]]
        np.testing.assert_array_equal(dset[...], [[1, 2, 3], [0, 0, 0]])

        dset[:, 1] = [[5, 6]]
        np.testing.assert_array_equal(dset[...], [[1, 5, 3], [0, 6, 0]])

    def test_broadcast_incompatible_shape(self):
        """ shapes that can't broadcast raise TypeError, as in h5py """
        dset = self.f.create_dataset("dset", (2, 3), dtype=np.int32)
        for val in (np.arange(4), np.ones((2, 2)), np.ones((2, 2, 3))):
            with self.assertRaises(TypeError):
                dset[...] = val
        # failed writes must not have modified the dataset
        np.testing.assert_array_equal(dset[...], np.zeros((2, 3)))

    def test_broadcast_array_dtype(self):
        """ array dtype: the trailing sub-shape is the element, and only the
        leading dims take part in broadcasting """
        dset = self.f.create_dataset("dset", (3,), dtype="3int8")
        dset[:] = [[1, 2, 3]]
        np.testing.assert_array_equal(dset[...], [[1, 2, 3]] * 3)

        # a single element is a "scalar" for broadcasting purposes,
        # which h5py doesn't support for array dtypes
        with self.assertRaises(TypeError):
            dset[...] = [4, 5, 6]

    def test_broadcast_compound(self):
        """ broadcast whole compound records, and a single compound field """
        dt = np.dtype([('a', np.int32), ('b', np.float32)])
        dset = self.f.create_dataset("dset", (2, 2), dtype=dt)
        row = np.array([[(1, 1.5), (2, 2.5)]], dtype=dt)
        dset[...] = row
        out = dset[...]
        np.testing.assert_array_equal(out['a'], [[1, 2], [1, 2]])
        np.testing.assert_array_equal(out['b'], [[1.5, 2.5], [1.5, 2.5]])

        dset['a'] = [[7, 8]]
        out = dset[...]
        np.testing.assert_array_equal(out['a'], [[7, 8], [7, 8]])
        np.testing.assert_array_equal(out['b'], [[1.5, 2.5], [1.5, 2.5]])

    def test_fancy_selection_shape_mismatch(self):
        """ broadcasting isn't supported for fancy selections """
        dset = self.f.create_dataset("dset", (4, 3), dtype=np.int32)
        with self.assertRaises(TypeError):
            dset[[0, 2], :] = [[1, 2, 3]]

        # exact shape still works
        dset[[0, 2], :] = [[1, 2, 3], [4, 5, 6]]
        np.testing.assert_array_equal(
            dset[...], [[1, 2, 3], [0, 0, 0], [4, 5, 6], [0, 0, 0]])


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
