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

# test dataset query
#
#

class TestQueryDataset(TestCase):
    def test_query_dset(self):
        filename = self.getFileName("query_dset")
        f = h5py.File(filename, "w")

        count = 100
        dt = np.dtype([('a', np.int), ('b', np.int)])
        dset = f.create_dataset('dset', (count,), dtype=dt)

        elem = dset[0]
        for i in range(count):
            elem['a'] = i // 10
            elem['b'] = i % 10
            dset[i] = elem


        # select from dset1
        if h5py.__name__ == "h5pyd":
            count = 0
            for row in dset.read_where("b>4"):
                self.assertTrue(row[1] > 4)
                count += 1

            self.assertEqual(count, 50)

        f.close()


if __name__ == '__main__':
    ut.main()
