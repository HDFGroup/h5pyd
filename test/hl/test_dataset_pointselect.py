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


def get_ptsel(dset, pts):
    # return values from th dataset for the given list of points
    dt = dset.dtype
    if config.get("use_h5py"):
        # h5py only supports point selection by boolean mask,
        # so access each point separately and store in an array
        # TBD: remove once this h5py PR is merged: https://github.com/h5py/h5py/pull/1793
        val = np.zeros((len(pts)), dtype=dt)
        for i in range(len(pts)):
            val[i] = dset[pts[i]]
    else:
        val = dset.points[pts]

    return val


def put_ptsel(dset, pts, values):
    # write values to the dataset for the given list of points
    if config.get("use_h5py"):
        for i in range(len(pts)):
            dset[pts[i]] = values[i]
    else:
        dset.points[pts] = values


class TestPointSelectDataset(TestCase):
    def test_boolean_select(self):
        filename = self.getFileName("point_select_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        data = np.zeros((10, 10), dtype='i4')
        for i in range(10):
            for j in range(10):
                data[i, j] = i - j
        dset = f.create_dataset('dset', data=data)
        pos_vals = dset[data > 0]
        self.assertEqual(len(pos_vals), 45)
        for value in pos_vals:
            self.assertTrue(value > 0)

        f.close()

    def test_1d_pointselect(self):
        filename = self.getFileName("test_1d_pointselect")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset1d = f.create_dataset('dset1d', (10,), dtype='i4')
        vals = list(range(10))
        vals.reverse()
        dset1d[...] = vals
        vals = dset1d[...]
        pts = [2, 4, 6, 8]
        arr = get_ptsel(dset1d, pts)
        expected_vals = [7, 5, 3, 1]
        for i in range(len(expected_vals)):
            self.assertEqual(arr[i], expected_vals[i])

        f.close()

        # re-open and test again
        f = h5py.File(filename, "r")
        dset1d = f['dset1d']
        arr = get_ptsel(dset1d, pts)
        for i in range(len(expected_vals)):
            self.assertEqual(arr[i], expected_vals[i])

        f.close()

    def test_1d_pointwrite(self):
        filename = self.getFileName("test_1d_pointwrite")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        count = 10

        dset1d = f.create_dataset('dset1d', (count,), dtype='i4')
        pts = [2, 4, 6, 8]
        values = [1, 10, 100, 1000]
        put_ptsel(dset1d, pts, values)

        arr = dset1d[...]
        expected = 1
        for i in range(count):
            if i in pts:
                self.assertEqual(arr[i], expected)
                expected *= 10
            else:
                self.assertEqual(arr[i], 0)

        f.close()

        # re-open and test again
        f = h5py.File(filename, "r")
        dset1d = f['dset1d']
        arr = dset1d[...]
        expected = 1
        for i in range(count):
            if i in pts:
                self.assertEqual(arr[i], expected)
                expected *= 10
            else:
                self.assertEqual(arr[i], 0)

        f.close()

    def test_2d_pointselect(self):
        filename = self.getFileName("test_2d_pointselect")
        print("filename:", filename)

        f = h5py.File(filename, "w")
        dt = np.int32
        dset2d = f.create_dataset('dset2d', (10, 20), dtype=dt)
        vals = np.zeros((10, 20), dtype=dt)
        for i in range(10):
            for j in range(20):
                vals[i, j] = i * 1000 + j

        dset2d[...] = vals
        vals = dset2d[...]

        pts = [(9 - i, i) for i in range(10)]
        val = get_ptsel(dset2d, pts)

        for i in range(len(pts)):
            self.assertEqual(val[i], (9 - i) * 1000 + i)

        f.close()

        # re-open and test again
        f = h5py.File(filename, "r")
        dset2d = f['dset2d']
        val = get_ptsel(dset2d, pts)
        for i in range(len(pts)):
            self.assertEqual(val[i], (9 - i) * 1000 + i)
        f.close()

    def test_2d_pointwrite(self):
        filename = self.getFileName("test_2d_pointwrite")
        print("filename:", filename)

        f = h5py.File(filename, "w")
        dt = np.int32
        dset2d = f.create_dataset('dset2d', (10, 20), dtype=dt)

        pts = []
        for i in range(10):
            pts.append((i, i))
        values = list(range(10))
        put_ptsel(dset2d, pts, values)

        arr = dset2d[...]
        for i in range(10):
            for j in range(20):
                expected = i if i == j else 0
                self.assertEqual(arr[i, j], expected)

        f.close()

        # re-open and test again
        f = h5py.File(filename, "r")
        dset2d = f['dset2d']
        arr = dset2d[...]
        for i in range(10):
            for j in range(20):
                expected = i if i == j else 0
                self.assertEqual(arr[i, j], expected)
        f.close()

    def test_2d_pointselect_broadcast(self):
        filename = self.getFileName("test_2d_pointselect_broadcast")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset2d = f.create_dataset('dset2d', (10, 20), dtype='i4')
        vals = np.zeros((10, 20), dtype='i4')
        for i in range(10):
            for j in range(20):
                vals[i, j] = i * 1000 + j

        dset2d[...] = vals

        pts = dset2d[[2, 4, 7], :]
        self.assertEqual(len(pts), 3)
        row1 = pts[0, :]
        self.assertEqual(list(row1), list(range(2000, 2020)))
        row2 = pts[1, :]
        self.assertEqual(list(row2), list(range(4000, 4020)))
        row3 = pts[2, :]
        self.assertEqual(list(row3), list(range(7000, 7020)))

        f.close()


if __name__ == '__main__':
    # loglevel = logging.DEBUG
    # logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
