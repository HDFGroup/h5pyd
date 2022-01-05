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
from datetime import datetime
import six


class TestScalarDataset(TestCase):
    def test_scalar_dset(self):
        filename = self.getFileName("scalar_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        
        dset = f.create_dataset('scalar', data=42, dtype='i8')

        val = dset[()]
        self.assertEqual(val, 42)
        self.assertEqual(dset.shape, ())
        self.assertEqual(dset.ndim, 0)

        dset[...] = 24
        val = dset[()]
        self.assertTrue(isinstance(val, np.int64))
        self.assertEqual(val, 24)

        # try will ellipsis
        val = dset[...]
        self.assertTrue(isinstance(val, np.ndarray))
        self.assertEqual(val, 24)

        # try setting value using tuple
        dset[()] = 99
        val = dset[()]
        self.assertEqual(val, 99)

        # Check dataset's last modified time
        if h5py.__name__ == "h5pyd":
            self.assertTrue(isinstance(dset.modified, datetime))

        self.assertEqual(dset.file.filename, filename)
    
        # try creating dataset implicitly
        g1 = f.create_group("g1")
        g1["scalar"] = 42
        dset = g1["scalar"]
        val = dset[()]
        self.assertEqual(val, 42)
    

        f.close()

    def test_scalar_str_dset(self):
        filename = self.getFileName("scalar_str_dset")
        print("filename:", filename)
        str1 = "Hello"
        str2 = "So long"
        str3 = "Thanks for all the fish"

        f = h5py.File(filename, "w")
        dt = h5py.special_dtype(vlen=str)
        dset = f.create_dataset('scalar', data=str1, dtype=dt)

        val = dset[()]
        self.assertEqual(val, str1.encode("utf-8"))
        self.assertEqual(dset.shape, ())
        self.assertEqual(dset.ndim, 0)

        dset[...] = str2
        val = dset[()]
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str2.encode("utf-8"))

        # try will ellipsis
        val = dset[...]

        self.assertTrue(isinstance(val, np.ndarray))
        self.assertEqual(val, str2.encode("ascii"))

        # try setting value using tuple
        dset[()] = str3
        val = dset[()]
    
        self.assertEqual(val, str3.encode("utf-8"))

        # try creating dataset implicitly
        g1 = f.create_group("g1")
        g1["scalar"] = str1
        dset = g1["scalar"]
        val = dset[()]
        self.assertEqual(val, str1.encode("utf-8"))
        self.assertEqual(dset.shape, ())
        self.assertEqual(dset.ndim, 0)

        f.close()


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
