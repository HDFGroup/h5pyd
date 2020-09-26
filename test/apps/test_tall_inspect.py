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
    filename = os.path.join(dirpath, "tall.h5")
    return filename


class TestTallInspect(TestCase):

    def setUp(self):
        
        filename = get_filename()
        self.f = h5py.File(filename, "r")

    def test_obj_count(self):
        counts = {"groups": 0, "datasets": 0}
        def visit(name):
            obj = self.f[name]
            if isinstance(obj, h5py.Dataset):
                counts["datasets"] += 1
            elif isinstance(obj, h5py.Group):
                counts["groups"] += 1
            else:
                # unexpected class
                self.assertTrue(False)
        self.f.visit(visit)
        self.assertEqual(5, counts["groups"])
        self.assertEqual(4, counts["datasets"])

    def test_attributes(self):
        self.assertEqual(2, len(self.f.attrs))
        self.assertTrue("attr1" in self.f.attrs)
        attr1 = self.f.attrs["attr1"]
        self.assertEqual(attr1.dtype, np.int8)
        self.assertEqual(attr1.shape, (10,))
        for i in range(10):
            self.assertEqual(attr1[i], ((97 + i) % 106))
        self.assertTrue("attr2" in self.f.attrs)
        attr2 = self.f.attrs["attr2"]
        self.assertEqual(attr2.dtype, np.dtype(">i4"))
        self.assertEqual(attr2.shape, (2,2))
        for i in range(2):
            for j in range(2):
                self.assertEqual(attr2[i,j], 2*i+j)
        dset111 = self.f["/g1/g1.1/dset1.1.1"]
        self.assertEqual(2, len(dset111.attrs))
        self.assertTrue("attr1" in dset111.attrs)
        self.assertTrue("attr2" in dset111.attrs)

    def test_links(self):
        g12 = self.f["/g1/g1.2"]
        self.assertEqual(len(g12), 2)
        self.assertTrue("extlink" in g12)
        extlink = g12.get("extlink", getlink=True)
        self.assertEqual(extlink.__class__.__name__, "ExternalLink")
        self.assertEqual(extlink.filename, "somefile")
        self.assertEqual(extlink.path, "somepath")
        g121 = self.f["/g1/g1.2/g1.2.1"]
        self.assertEqual(len(g121), 1)
        softlink = g121.get("slink", getlink=True)
        self.assertEqual(softlink.path, "somevalue")

    def test_dataset(self):
        dset112 = self.f["/g1/g1.1/dset1.1.2"]
        self.assertEqual(dset112.shape, (20,))
        self.assertEqual(dset112.dtype, np.dtype(">i4"))
        arr = dset112[...]
        for i in range(20):
            self.assertEqual(arr[i], i)

if __name__ == '__main__':
    print("filename:", get_filename())
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    
    ut.main()

