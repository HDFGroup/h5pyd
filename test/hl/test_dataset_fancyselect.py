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
            for j in range(10):
                vals[i,j] = i*10+j
        dset2d[...] = vals

        coords = [2,5,6,9]

        arr = dset2d[ 5:7, coords ]
        self.assertEqual(arr.shape, (2,4))
        for i in range(2):
            row = arr[i]
            for j in range(4):
                self.assertEqual(row[j], (i+5)*10+coords[j])
        
        f.close()


if __name__ == '__main__':
    ut.main()
