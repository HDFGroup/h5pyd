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

# test fancy selection
#
#

class TestFancySelectDataset(TestCase):
    def test_dset(self):
        filename = self.getFileName("fancy_select_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset2d = f.create_dataset('dset2d', (10,10), dtype='i4')
        vals = np.zeros((10,10), dtype='i4')
        for i in range(10):
            vals[i,i] = 1
        dset2d[...] = vals
        
        rows = dset2d[ 5:7, : ]
        self.assertEqual(len(rows), 2)
        row1 = rows[0]
        row2 = rows[1]
        self.assertEqual(row1[5], 1)
        self.assertEqual(row2[6], 1)
         
        f.close()


if __name__ == '__main__':
    ut.main()
