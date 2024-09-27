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

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestFancySelectDataset(TestCase):
    def test_dset_2d(self):
        filename = self.getFileName("fancy_select_dset_2d")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset2d = f.create_dataset('dset2d', (10, 10), dtype='i4')
        vals = np.zeros((10, 10), dtype='i4')
        for i in range(10):
            for j in range(10):
                vals[i, j] = i * 10 + j
        dset2d[...] = vals

        coords1 = [2, 5, 6, 9]
        coords2 = [9, 5, 6, 2]  # indexing vector doesn't need to be increasing for h5pyd

        arr = dset2d[5:7, coords1]
        self.assertEqual(arr.shape, (2, 4,))
        for i in range(2):
            row = arr[i]
            for j in range(4):
                self.assertEqual(row[j], (i + 5) * 10 + coords1[j])

        try:
            arr = dset2d[coords1, coords1]
            self.assertEqual(arr.shape, (4,))
            for i in range(4):
                self.assertEqual(arr[i], coords1[i] * 10 + coords1[i])

            arr = dset2d[coords1, coords2]
            self.assertEqual(arr.shape, (4,))
            for i in range(4):
                self.assertEqual(arr[i], coords1[i] * 10 + coords2[i])
        except TypeError:
            if config.get("use_h5py"):
                pass  # multiple indexing vectors not allowed with h5py
            else:
                self.assertTrue(False)  # but should be ok with h5pyd/hsds

        f.close()

    def test_dset_3d(self):
        filename = self.getFileName("fancy_select_dset_3d")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        data = np.arange(4 * 5 * 6).reshape(4, 5, 6)
        for i in range(4):
            for j in range(5):
                for k in range(6):
                    data[i, j, k] = i * j * k

        d_3d = f.create_dataset('dset3d', data=data)

        arr1 = data[:, :, [1, 2, 5]]
        arr2 = d_3d[:, :, [1, 2, 5]]
        self.assertTrue(np.array_equal(arr1, arr2))

        try:
            arr1 = data[:, [0, 1, 3], [1, 2, 5]]
            arr2 = d_3d[:, [0, 1, 3], [1, 2, 5]]
            self.assertTrue(np.array_equal(arr1, arr2))

            arr1 = data[0, [0, 1, 3], [1, 2, 5]]
            arr2 = d_3d[0, [0, 1, 3], [1, 2, 5]]
            self.assertTrue(np.array_equal(arr1, arr2))

            arr1 = data[[0, 2, 3], :, [1, 2, 5]]
            arr2 = d_3d[[0, 2, 3], :, [1, 2, 5]]
            self.assertTrue(np.array_equal(arr1, arr2))

            arr1 = data[[0, 2, 3], [0, 2, 4], :]
            arr2 = d_3d[[0, 2, 3], [0, 2, 4], :]
            self.assertTrue(np.array_equal(arr1, arr2))
        except TypeError:
            if config.get("use_h5py"):
                pass  # multiple indexing vectors not allowed with h5py
            else:
                self.assertTrue(False)  # but should be ok with h5pyd/hsds

        f.close()

    def test_bigdset(self):
        filename = self.getFileName("fancy_select_dset_3d")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        # create a dataset
        dset = f.create_dataset("dset", (5, 1000, 1000), dtype="i4", compression="gzip")
        print(dset.id.id)
        # write some values to the dataset
        dset[:, 1, 10] = [95, 96, 97, 98, 99]
        dset[:, 10, 100] = [195, 196, 197, 198, 199]
        dset[:, 100, 500] = [295, 296, 297, 298, 299]

        # single coordinate, increasing
        arr = dset[:, 10, [10, 100, 500]]
        self.assertEqual(arr.shape, (5, 3))
        self.assertTrue((arr[:, 0] == [0, 0, 0, 0, 0]).all())
        self.assertTrue((arr[:, 1] == [195, 196, 197, 198, 199]).all())
        self.assertTrue((arr[:, 2] == [0, 0, 0, 0, 0]).all())

        # non-increasing indexes
        arr = dset[:, 10, [100, 10, 500]]
        self.assertEqual(arr.shape, (5, 3))
        self.assertTrue((arr[:, 0] == [195, 196, 197, 198, 199]).all())
        self.assertTrue((arr[:, 1] == [0, 0, 0, 0, 0]).all())
        self.assertTrue((arr[:, 2] == [0, 0, 0, 0, 0]).all())

        # test multiple coordinates
        arr = dset[:, [1, 10, 100], [10, 100, 500]]
        self.assertEqual(arr.shape, (5, 3))
        self.assertTrue((arr[:, 0] == [95, 96, 97, 98, 99]).all())
        self.assertTrue((arr[:, 1] == [195, 196, 197, 198, 199]).all())
        self.assertTrue((arr[:, 2] == [295, 296, 297, 298, 299]).all())


if __name__ == '__main__':
    ut.main()
