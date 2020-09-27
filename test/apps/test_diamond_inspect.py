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
    filename = os.path.join(dirpath, "diamond.h5")
    return filename


class TestDiamondInspect(TestCase):

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
        self.assertEqual(2, counts["groups"])
        self.assertEqual(1, counts["datasets"])

    def test_links(self):
        self.assertEqual(len(self.f), 2)
        self.assertTrue("g1" in self.f)
        self.assertTrue("g2" in self.f)
        g1 = self.f["g1"]
        self.assertEqual(len(g1), 1)
        self.assertTrue("dset" in g1)
        g1_dset = g1["dset"]
        self.assertEqual(g1_dset[...], 42)
        g2 = self.f["g2"]
        self.assertEqual(len(g2), 1)
        self.assertTrue("dset" in g2)
        g2_dset = g1["dset"]
        self.assertEqual(g1_dset.id.id, g2_dset.id.id)

if __name__ == '__main__':
    print("filename:", get_filename())
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    
    ut.main()

