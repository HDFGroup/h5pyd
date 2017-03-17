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
import config
import numpy as np

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestCommittedType(TestCase):
    def test_createtype(self):
        filename = self.getFileName("committed_type")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        # create a compound numpy type
        dt = np.dtype([('real', np.float), ('img', np.float)])
        f['complex_type'] = dt
        ctype = f['complex_type']
        self.assertEqual(ctype.dtype.name, dt.name)
        self.assertEqual(len(ctype.dtype), len(dt))
        ctype.attrs["attr1"] = "this is a named datatype"
        dset = f.create_dataset('complex_dset', (10,), dtype=f['complex_type'])
        f.close()

if __name__ == '__main__':
    ut.main()
