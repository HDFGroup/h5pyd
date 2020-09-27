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

import sys
import os
import numpy as np
import config
import logging
if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase

def get_filename():
    if config.get("use_h5py"):
        dirpath = "data" 
    else:
        dirpath = config.get("H5PYD_TEST_FOLDER")
    filename = os.path.join(dirpath, "shuffle_compress.h5")
    return filename


class TestShuffleInspect(TestCase):

    def setUp(self):
        
        filename = get_filename()
        self.f = h5py.File(filename, "r")


    def test_dset(self):
        self.assertEqual(len(self.f), 1)
        self.assertTrue("dset" in self.f)
        dset = self.f["dset"]
        self.assertEqual(dset.compression, "gzip")
        self.assertTrue(dset.shuffle)
        self.assertEqual(dset.shape, (100,))
        arr = dset[0:10]
        for  i in range(10):
            self.assertEqual(arr[i], i)

if __name__ == '__main__':
    print("filename:", get_filename())
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    
    ut.main()

