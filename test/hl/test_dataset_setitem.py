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


class TestSetItemDataset(TestCase):
    def test_set_all(self):
        filename = self.getFileName("dset_set_all")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        extent = 10
        dset = f.create_dataset('dset', (extent, extent), dtype='f8')
        arr = np.random.rand(extent, extent)
        dset[()] = arr
        f.close()

    def test_broadcast(self):
        filename = self.getFileName("dset_broadcast")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        dset = f.create_dataset("dset", (4,5), dtype=np.int32)
        dset[...] = 42
        for i in range(4):
            self.assertEqual(dset[i,i], 42)
        f.close()

    def test_type_conversion(self):
        filename = self.getFileName("dset_type_conversion")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        dset = f.create_dataset("dset", (4,), dtype=np.int32)
        dset[...] = np.arange(4.0)
        for i in range(4):
            self.assertEqual(dset[i], 0.0 + i)
        f.close()



if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
