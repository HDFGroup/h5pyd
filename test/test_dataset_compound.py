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

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestDatasetCompound(TestCase):
    def test_create_compound_dset(self):
        filename = self.getFileName("create_compound_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        #curl -v --header "Host: create_compound_dset.h5pyd_test.hdfgroup.org" http://127.0.0.1:5000


        count = 10

        dt = np.dtype([('real', np.float), ('img', np.float)])
        dset = f.create_dataset('complex', (count,), dtype=dt)

        elem = dset[0]
        for i in range(count):
            theta = (4.0 * math.pi)*(float(i)/float(count))
            elem['real'] = math.cos(theta)
            elem['img'] = math.sin(theta)
            dset[i] = elem

        val = dset[0]
        self.assertEqual(val['real'], 1.0)
        f.close()

    def test_query_compound_dset(self):
        filename = self.getFileName("query_compound_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        #curl -v --header "Host: create_compound_dset.h5pyd_test.hdfgroup.org" http://127.0.0.1:5000

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
         
        count = len(data)
        dt = np.dtype([('symbol', 'S4'), ('date', 'S8'), ('open', 'i4'), ('close', 'i4')])
        dset = f.create_dataset('stock', (count,), dtype=dt)
        for i in range(count):
            dset[i] = data[i]
        if config.get("use_h5py"):
            print("read_where not availble for h5py")
        else:    
            quotes = dset.read_where("symbol == b'AAPL'")
            self.assertEqual(len(quotes), 4)
            for i in range(4):
                quote = quotes[i]
                self.assertEqual(quote[0], b'AAPL')
        f.close()



if __name__ == '__main__':
    ut.main()
