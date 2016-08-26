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

        primes = [2, 3, 5, 7, 11, 13, 17, 19]
        num_rows = 5

        dset1 = f.create_dataset('dset1', (len(primes),), dtype='i8')
        dset2 = f.create_dataset('dset2', (num_rows, len(primes)), dtype='i8')


        shape = dset2.shape
        self.assertEqual(shape[0], num_rows)
        self.assertEqual(shape[1], len(primes))


        # write primes
        row = primes[:]

        dset1[:] = primes

        for i in range(num_rows):
            row = primes[:]
            for j in range(len(row)):
                row[j] *= (i+1)
            dset2[i, :] = row

        # select from dset1
        points = dset1[[2, 3, 6]]
        print(points)

        f.close()


if __name__ == '__main__':
    ut.main()
