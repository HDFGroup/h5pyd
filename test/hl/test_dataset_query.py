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


class TestQueryDataset(TestCase):

    def test_query_simple_dset(self):
        def doQuery(dset):
            if h5py.__name__ != "h5pyd":
                return  # only test h5pyd query

            # test h5pyd query
            expr = "_ > 100.0 AND _ < 200.0"
            indices = dset.query(expr)
            self.assertEqual(len(indices), expected_count)
            for index in indices:
                self.assertEqual(len(index), 2)
                self.assertTrue(index[0] >= 0 and index[0] < dims[0])
                self.assertTrue(index[1] >= 0 and index[1] < dims[1])
                self.assertTrue(arr[tuple(index)] > 100.0 and arr[tuple(index)] < 200.0)

            # test with limit
            indices = dset.query(expr, limit=10)
            self.assertEqual(len(indices), 10)

            # test with selection
            sel = ((slice(10, 30), slice(20, 60)))
            indices = dset.query(expr, selection=sel)
            self.assertTrue(len(indices) > 0)
            self.assertTrue(len(indices) < expected_count)
            for index in indices:
                self.assertTrue(index[0] >= 10 and index[0] < 30)
                self.assertTrue(index[1] >= 20 and index[1] < 60)
                self.assertTrue(arr[tuple(index)] > 100.0 and arr[tuple(index)] < 200.0)

            # do a __getitem__ with the query expression
            val = dset.__getitem__(..., query=expr)
            self.assertEqual(len(val.shape), 1)
            self.assertEqual(val.shape[0], expected_count)
            self.assertTrue(np.all(val > 100.0) and np.all(val < 200.0))

        filename = self.getFileName("query_simple_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dims = (40, 80)
        dset = f.create_dataset('simple_dset', dims, dtype='f4')

        self.assertEqual(dset.name, "/simple_dset")
        self.assertTrue(isinstance(dset.shape, tuple))

        arr = np.zeros(dims, dtype="f4")
        expected_count = 0
        for i in range(dims[0]):
            for j in range(dims[1]):
                val = float(i) * 10.0 + float(j) / 10.0
                arr[i, j] = val
                if 100.0 < val < 200.0:
                    expected_count += 1

        dset[...] = arr  # write entire array to dataset

        doQuery(dset)

        f.close()

        # re-open and verify contents
        f = h5py.File(filename, "r")
        self.assertTrue('/simple_dset' in f)
        dset = f['/simple_dset']
        self.assertEqual(len(dset.shape), 2)
        self.assertEqual(dset.ndim, 2)
        self.assertEqual(dset.shape[0], dims[0])
        self.assertEqual(dset.shape[1], dims[1])
        self.assertEqual(str(dset.dtype), 'float32')

        doQuery(dset)
        f.close()


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
