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
import os
import config
from common import ut, TestCase

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py


@ut.skipIf(os.environ.get('H5_DRIVER', '') == 'h5serv',
           'Does not work with h5serv due to an h5json exception')
class TestComplexNumbers(TestCase):
    """ Test read/write of complex numbers """

    def test_complex_dset(self):
        """Read and write complex numbers in a dataset"""
        filename = self.getFileName("create_complex_dset")
        print("filename:", filename)
        count = 10
        dt = np.dtype('complex64')
        with h5py.File(filename, "w") as f:
            dset = f.create_dataset('complex', shape=(count,), dtype=dt)
            self.assertEqual(dset.dtype.names, ('r', 'i'))
            for i in range(count):
                theta = (4.0 * math.pi) * (float(i) / float(count))
                dset[i] = math.cos(theta) + 1j * math.sin(theta)
            val = dset[0]

        self.assertEqual(val.shape, ())
        self.assertEqual(val.dtype.kind, 'c')
        self.assertEqual(val.real, 1.0)
        self.assertEqual(val.imag, 0.)


if __name__ == '__main__':
    ut.main()
