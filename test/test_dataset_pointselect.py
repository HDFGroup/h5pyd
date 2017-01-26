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

# test point selection
#
# Not working yet!
#

class TestPointSelectDataset(TestCase):
    def test__dset(self):
        filename = self.getFileName("point_select_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        data = np.zeros((10,10), dtype='i4')
        for i in range(10):
            for j in range(10):
                data[i,j] = i - j
        dset = f.create_dataset('dset', data=data)
        pos_vals = dset[ data > 0 ]
        self.assertEqual(len(pos_vals), 45)
        for value in pos_vals:
            self.assertTrue(value > 0)
         

        f.close()


if __name__ == '__main__':
    ut.main()
