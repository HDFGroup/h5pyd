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

class TestVisit(TestCase):
    def test_visit(self):
        visit_names = []

        def visit_item(name):
            visit_names.append(name)
            return None

        def find_g1_1(name):
            if name.endswith("g1.1"):
                return "found g1.1"  # stop iteration
            return None

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

    def test_visit_multilink(self):
        obj_ids = set()
        visited_ids = []

        def visit_multilink(name, obj):
            if not config.get("use_h5py"):
                # obj ids not unique in h5py?
                self.assertTrue(obj.id.id in obj_ids)
            visited_ids.append(obj.id.id)

        filename = self.getFileName("test_visit_multilink")
        print("filename:", filename)

        f = h5py.File(filename, 'w')
        g1 = f.create_group("g1")
        obj_ids.add(g1.id.id)
        self.assertTrue("g1" in f)
        g2 = f.create_group("g2")
        obj_ids.add(g2.id.id)

        self.assertTrue("g2" in f)
        g1_1 = f.create_group("g1/g1.1")
        obj_ids.add(g1_1.id.id)
        self.assertTrue("g1/g1.1" in f)
        dset = f.create_dataset('g1/g1.1/dset', data=42, dtype='i4')
        obj_ids.add(dset.id.id)

        # add some extra link
        g2["g1_link"] = g1
        g1_link = g2["g1_link"]
        # verify that g1 and g1_link point to the same HDF object
        self.assertEqual(g1.id, g1_link.id)  # id comparisons always work
        if not config.get("use_h5py"):
            # numeric id comparisons works for h5pyd but not h5py
            self.assertEqual(g1.id.id, g1_link.id.id)
        self.assertEqual(g1.id.__hash__(), g1_link.id.__hash__())
        g2["dset_link"] = dset
        g1["dset_link"] = dset
        del g1_1["dset"]

        f.close()

        # re-open as read-only
        f = h5py.File(filename, 'r')
        f.visititems(visit_multilink)

        f.close()
        self.assertEqual(len(visited_ids), len(obj_ids))



if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
