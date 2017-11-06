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
import six
import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestVlenAttribute(TestCase):


    def test_create(self):
        filename = self.getFileName("create_vlen_attribute")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

        g1 = f.create_group('g1')

        dt = h5py.special_dtype(vlen=np.dtype('int32'))
        e0 = np.array([0,1,2])
        e1 = np.array([0,1,2,3])
        data = np.array([e0, e1], dtype=object)

        g1.attrs.create("a1", data, shape=(2,), dtype=dt)

        ret_val = g1.attrs["a1"]
        self.assertEqual(len(ret_val), 2)
        self.assertEqual(list(ret_val[0]), [0,1,2])
        self.assertEqual(list(ret_val[1]), [0,1,2,3])

        # close file
        f.close()




         

if __name__ == '__main__':
    ut.main()




