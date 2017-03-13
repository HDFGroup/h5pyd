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

        dset[...] = 24
        val = dset[()]
        self.assertEqual(val, 24)

        # try setting value using tuple
        dset[()] = 99
        val = dset[()]
        self.assertEqual(val, 99)

        self.assertEqual(dset.file.filename, filename)

        # Check dataset's last modified time
        if h5py.__name__ == "h5pyd":
            self.assertTrue(isinstance(dset.modified, datetime))

        f.close()


if __name__ == '__main__':
    ut.main()
