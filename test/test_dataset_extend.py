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


class TestExtendDataset(TestCase):
    def test_extend_dset(self):
        filename = self.getFileName("extend_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        primes = [2, 3, 5, 7, 11, 13, 17, 19]

        dset = f.create_dataset('primes', (1,len(primes)), maxshape=(None, len(primes)), dtype='i8')

        maxshape = dset.maxshape
        self.assertEqual(maxshape[0], None)
        self.assertEqual(maxshape[1], len(primes))
        shape = dset.shape
        self.assertEqual(shape[0], 1)
        self.assertEqual(shape[1], len(primes))
        #print('chunks:', dset.chunks)

        # write primes
        dset[0:,:] = primes

        # extend first dimension of dataset
        dset.resize(2, axis=0)
        maxshape = dset.maxshape
        self.assertEqual(maxshape[0], None)
        self.assertEqual(maxshape[1], len(primes))
        shape = dset.shape
        self.assertEqual(shape[0], 2)
        self.assertEqual(shape[1], len(primes))

        # write second row
        for i in range(len(primes)):
            primes[i] *= 2

        dset[1:,:] = primes

        # retrieve  an element from updated dataset
        self.assertEqual(dset[1,2], 10)

        f.close()


if __name__ == '__main__':
    ut.main()
