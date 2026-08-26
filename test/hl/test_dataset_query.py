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
        expr = "_ > 100.0 AND _ < 200.0"

        def doQuery(dset):
            if h5py.__name__ != "h5pyd":
                return  # only test h5pyd query

            # test h5pyd query
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

        with self.assertRaises(IOError):
            dset.query(expr, update_value=0.0)  # no write intent
        f.close()

        # re-open with modify
        f = h5py.File(filename, "r+")
        dset = f['/simple_dset']

        # create a regionref based on the query expression
        regionref = dset.regionref.query(expr)
        values = dset[regionref]
        self.assertEqual(values.shape, (799,))
        for value in values:
            self.assertTrue(value > 100.0)
            self.assertTrue(value < 200.0)
        dset.attrs["regref"] = regionref

        # set the query values to -1.0
        if h5py.__name__ == "h5pyd":
            indices = dset.query(expr, update_value=-1.0)
            self.assertEqual(len(indices), expected_count)

        f.close()

    def test_query_compound_dset(self):
        if h5py.__name__ != "h5pyd":
            return  # only test h5pyd query

        filename = self.getFileName("query_compound_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dt = np.dtype([('symbol', 'S4'), ('date', 'S8'), ('open', 'i4'), ('close', 'i4')])

        # 4 rows x 3 cols - same stock data as test_table.py's test_query_table,
        # but laid out on a two-dimensional dataset instead of a 1-D table
        data = [
            [("EBAY", "20170102", 3023, 3088), ("AAPL", "20170102", 3054, 2933), ("AMZN", "20170102", 2973, 3011)],
            [("EBAY", "20170103", 3042, 3128), ("AAPL", "20170103", 3182, 3034), ("AMZN", "20170103", 3021, 2788)],
            [("EBAY", "20170104", 2798, 2876), ("AAPL", "20170104", 2834, 2867), ("AMZN", "20170104", 2891, 2978)],
            [("EBAY", "20170105", 2973, 2962), ("AAPL", "20170105", 2934, 3010), ("AMZN", "20170105", 3018, 3086)],
        ]
        dims = (4, 3)
        arr = np.array(data, dtype=dt)
        self.assertEqual(arr.shape, dims)

        dset = f.create_dataset('stock2d', dims, dtype=dt)
        dset[...] = arr

        expected = [(i, j) for i in range(dims[0]) for j in range(dims[1]) if arr[i, j]['symbol'] == b'AAPL']

        # simple field-equality query
        condition = "symbol == b'AAPL'"
        indices = dset.query(condition)
        self.assertEqual(len(indices), len(expected))
        for index in indices:
            self.assertEqual(len(index), 2)
            self.assertEqual(tuple(index) in expected, True)
            self.assertEqual(arr[tuple(index)]['symbol'], b'AAPL')

        # compound query
        condition = "(open > 3000) AND (open < 3100)"
        indices = dset.query(condition)
        self.assertTrue(len(indices) > 0)
        for index in indices:
            val = arr[tuple(index)]
            self.assertTrue(val['open'] > 3000)
            self.assertTrue(val['open'] < 3100)

        # query with limit
        indices = dset.query(condition, limit=1)
        self.assertEqual(len(indices), 1)

        # query with a selection restricting to just the first row
        condition = "symbol == b'AAPL'"
        sel = (slice(0, 1), slice(0, dims[1]))
        indices = dset.query(condition, selection=sel)
        self.assertTrue(len(indices) > 0)
        for index in indices:
            self.assertEqual(index[0], 0)

        f.close()

        # re-open and verify the query still works against persisted data
        f = h5py.File(filename, "r")
        dset = f['stock2d']
        self.assertEqual(dset.shape, dims)
        indices = dset.query(condition)
        self.assertEqual(len(indices), len(expected))

        with self.assertRaises(IOError):
            dset.query(condition, update_value={"open": 0})  # no write intent
        f.close()

        # re-open with modify and update the matching rows' 'open' field
        f = h5py.File(filename, "r+")
        dset = f['stock2d']
        update_val = {"open": 123}
        indices = dset.query(condition, update_value=update_val)
        self.assertEqual(len(indices), len(expected))
        f.flush()

        for index in indices:
            row = dset[tuple(index)]
            self.assertEqual(row['open'], 123)
            self.assertEqual(row['symbol'], b'AAPL')
            # 'close' should be untouched by a field-restricted update
            orig = arr[tuple(index)]
            self.assertEqual(row['close'], orig['close'])

        f.close()


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
