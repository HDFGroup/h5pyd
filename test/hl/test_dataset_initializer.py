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
import logging
import numpy as np

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase

class TestDatasetInitializer(TestCase):


    def test_create_arange_dset(self):
        filename = self.getFileName("create_arange_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        is_hsds = False
        if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
            is_hsds = True  # HSDS has different permission defaults

        extent = 40
        start = 10
        step = 2
        dims = (extent,)
        initializer="arange"
        initializer_opts = [f"--start={start}", f"--step={step}"]
        kwargs = {"dtype": "i8", "initializer":initializer, "initializer_opts": initializer_opts}
        dset = f.create_dataset('arange_dset', dims, **kwargs)

        self.assertEqual(dset.name, "/arange_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], extent)
        self.assertEqual(str(dset.dtype), 'int64')
        arr = dset[...]   # read all the elements
        for i in range(extent):
            if is_hsds:
                expected = i * step + start
            else:
                expected = 0
            self.assertEqual(arr[i], expected)

        f.close()

if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
