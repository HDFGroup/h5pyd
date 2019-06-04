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
import config
if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py
from common import ut, TestCase

visit_names = []
obj_ids = []

def visit_item(name):
    visit_names.append(name)
    return None

def find_g1_1(name):
    if name.endswith("g1.1"):
        return "found g1.1"  # stop iteration
    return None


class TestVisit(TestCase):
    def test_visit(self):
        filename = self.getFileName("test_visit")
        print("filename:", filename)
        
        f = h5py.File(filename, 'w')
        f.create_group("g1")
        self.assertTrue("g1" in f)
        f.create_group("g2")
        self.assertTrue("g2" in f)
        f.create_group("g1/g1.1")
        self.assertTrue("g1/g1.1" in f)
        f.create_dataset('g1/g1.1/dset', data=42, dtype='i4')
        f["/g1/soft"] = h5py.SoftLink('/g2')
        f.close()
        
        
        # re-open as read-only
        f = h5py.File(filename, 'r')
        f.visit(visit_item)
        self.assertEqual(len(visit_names), 4)
        h5paths = ("g1", 'g2', "g1/g1.1", "g1/g1.1/dset")
        for h5path in h5paths:
            self.assertTrue(h5path in visit_names)
        ret = f.visit(find_g1_1)
        self.assertEqual(ret, "found g1.1")
                
        f.close()



if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
