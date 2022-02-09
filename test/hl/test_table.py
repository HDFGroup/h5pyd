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
import math
import logging

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestTable(TestCase):
    def test_create_table(self):
        filename = self.getFileName("create_table_dset")
        print("filename:", filename)
        if config.get("use_h5py"):
            return # Table not supported with h5py
        f = h5py.File(filename, "w")
        if not f.id.id.startswith("g-"):
            return # append not supported with h5serv

        count = 10

        dt = np.dtype([('real', float), ('img', float)])
        table = f.create_table('complex', numrows=10, dtype=dt)

        elem = table[0]
        for i in range(count):
            theta = (4.0 * math.pi)*(float(i)/float(count))
            elem['real'] = math.cos(theta)
            elem['img'] = math.sin(theta)
            table[i] = elem

        self.assertEqual(table.colnames, ['real', 'img'])
        self.assertEqual(table.nrows, count)

        num_rows = 0
        for row in table:
            self.assertEqual(len(row), 2)
            num_rows += 1
        self.assertEqual(num_rows, count)

        # try the same thing using cursor object
        cursor = table.create_cursor()
        num_rows = 0
        for row in cursor:
            self.assertEqual(len(row), 2)
            num_rows += 1
        self.assertEqual(num_rows, count)

        arr = table.read(start=5, stop=6)
        self.assertEqual(arr.shape, (1,))


        f.close()

    def test_query_table(self):
        filename = self.getFileName("query_compound_dset")
        print("filename:", filename)
        if config.get("use_h5py"):
            return # Table not supported with h5py
        f = h5py.File(filename, "w")

        if not f.id.id.startswith("g-"):
            return # append not supported with h5serv

        # write entire array
        data = [
            ("EBAY", "20170102", 3023, 3088),
            ("AAPL", "20170102", 3054, 2933),
            ("AMZN", "20170102", 2973, 3011),
            ("EBAY", "20170103", 3042, 3128),
            ("AAPL", "20170103", 3182, 3034),
            ("AMZN", "20170103", 3021, 2788),
            ("EBAY", "20170104", 2798, 2876),
            ("AAPL", "20170104", 2834, 2867),
            ("AMZN", "20170104", 2891, 2978),
            ("EBAY", "20170105", 2973, 2962),
            ("AAPL", "20170105", 2934, 3010),
            ("AMZN", "20170105", 3018, 3086)
        ]

        dt = np.dtype([('symbol', 'S4'), ('date', 'S8'), ('open', 'i4'), ('close', 'i4')])
        table = f.create_table('stock', dtype=dt)

        table.append(data)

        self.assertEqual(table.nrows, len(data))

        for indx in range(len(data)):
            row = table[indx]
            item = data[indx]
            for col in range(2,3):
                # first two columns will come back as bytes, not strs
                self.assertEqual(row[col], item[col])

        cursor = table.create_cursor()
        indx = 0
        for row in cursor:
            item = data[indx]
            for col in range(2,3):
                # first two columns will come back as bytes, not strs
                self.assertEqual(row[col], item[col])
            indx += 1
        self.assertEqual(indx, len(data))

        cursor = table.create_cursor(start=2, stop=5)
        indx = 2
        for row in cursor:
            item = data[indx]
            for col in range(2,3):
                # first two columns will come back as bytes, not strs
                self.assertEqual(row[col], item[col])
            indx += 1
        self.assertEqual(indx, 5)

        condition = "symbol == b'AAPL'"
        quotes = table.read_where(condition)
        self.assertEqual(len(quotes), 4)
        expected_indices = [1,4,7,10]
        for i in range(4):
            quote = quotes[i]
            self.assertEqual(len(quote), 5)
            self.assertEqual(quote[0], expected_indices[i])
            self.assertEqual(quote[1], b'AAPL')

        # read up to 2 rows
        quotes = table.read_where(condition, limit=2)
        self.assertEqual(len(quotes), 2)

        # use a query cursor
        cursor = table.create_cursor(condition=condition)
        num_rows = 0
        for row in cursor:
            self.assertEqual(len(row), 5)
            num_rows += 1
        self.assertEqual(num_rows, 4)
 
        # try a compound query
        condition = "(open > 3000) & (open < 3100)" 
        quotes = table.read_where(condition)

        self.assertEqual(len(quotes), 5)
        for i in range(4):
            quote = quotes[i]
            self.assertTrue(quote[3] > 3000)
            self.assertTrue(quote[3] < 3100)
        
        # try modifying specific rows
        condition = "symbol == b'AAPL'"
        update_val = {"open": 123}
        indices = table.update_where(condition, update_val)
        self.assertEqual(len(indices), 4)
        self.assertEqual(list(indices), [1,4,7,10])

        row = tuple(table[4])
        self.assertEqual(row, (b'AAPL', b'20170103', 123, 3034))

        # try modifying just one value
        update_val = {'close': 999}
        indices = table.update_where(condition, update_val, limit=1)
        self.assertEqual(len(indices), 1)
        self.assertEqual(list(indices), [1])
        f.close()

if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
