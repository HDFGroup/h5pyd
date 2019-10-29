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


class TestObjRef(TestCase):


    def test_create(self):
        filename = self.getFileName("objref_test")
        print(filename)

        f = h5py.File(filename, 'w')
        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)
        r = f['/']
        is_h5serv = False
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            is_h5serv = True  # h5serv doesn't have support for objref datasets yet

        # create subgroup g1
        r.create_group('g1')
        self.assertTrue('g1' in r)
        g1 = r['g1']

        # create subgroup g1/g1.1
        g11 = g1.create_group('g1.1')

        # get ref to g1/g1.1
        g11_ref = g11.ref
        self.assertTrue(isinstance(g11_ref, h5py.Reference))
        self.assertEqual(g11.name, "/g1/g1.1")

        # create subgroup g2
        r.create_group('g2')
        self.assertEqual(len(r), 2)
        g2 = r['g2']

        # get ref to g1/g1.1 from g2
        g11ref = g2[g11_ref]

        # create subgroup /g1/g1.1/foo
        g11ref.create_group("foo")
        self.assertEqual(len(g11), 1)
        self.assertTrue("foo" in g11)

        # create datset /g2/d1
        d1 = g2.create_dataset('d1', (10,), dtype='i8')

        # get ref to d1
        d1_ref = d1.ref
        dt = h5py.special_dtype(ref=h5py.Reference)
        self.assertTrue(dt.metadata['ref'] is h5py.Reference)
        ref = h5py.check_dtype(ref=dt)
        self.assertEqual(ref, h5py.Reference)


        if is_h5serv:
            return  # ref types not supported in h5serv

        # create dataset of ref types
        dset = g1.create_dataset('myrefs', (10,), dtype=dt)
        ref = h5py.check_dtype(ref=dset.dtype)
        self.assertEqual(ref, h5py.Reference)

        dset[0] = g11_ref
        dset[1] = d1_ref

        a_ref = dset[0]
        obj = f[a_ref]
        if not config.get("use_h5py"):
            self.assertEqual(obj.id.id, g11.id.id)  # ref to g1.1
        self.assertEqual(obj.name, "/g1/g1.1")

        b_ref = dset[1]
        obj = f[b_ref]
        if not config.get("use_h5py"):
            self.assertEqual(obj.id.id, d1.id.id)  # ref to d1
        self.assertEqual(obj.name, "/g2/d1")

        # try the same thing using attributes
        ref_values = [g11_ref, d1_ref]
        g1.attrs.create("a1", ref_values, dtype=dt)

        # pass a single ref to attribute create
        obj = f[g11_ref]
        g1.attrs.create("b1", g11_ref, dtype=dt)

        # read back the attribute
        attr = g1.attrs["a1"]
        self.assertEqual(attr.shape, (2,))
        ref = h5py.check_dtype(ref=attr.dtype)
        self.assertEqual(ref, h5py.Reference)
        a0_ref = attr[0]
        obj = f[a0_ref]
        if not config.get("use_h5py"):
            self.assertEqual(obj.id.id, g11.id.id)  # ref to g1.1
        self.assertEqual(obj.name, "/g1/g1.1")
        a1_ref = attr[1]
        obj = f[a1_ref]
        if not config.get("use_h5py"):
            self.assertEqual(obj.id.id, d1.id.id)  # ref to d1
        self.assertEqual(obj.name, "/g2/d1")
        f.close()


        # try opening in read-mode
        f = h5py.File(filename, 'r')
        g1 = f['/g1']
        g11 = f['g1/g1.1']
        d1 = f["/g2/d1"]
        attr = g1.attrs["a1"]
        self.assertEqual(attr.shape, (2,))
        ref = h5py.check_dtype(ref=attr.dtype)
        self.assertEqual(ref, h5py.Reference)
        a0_ref = attr[0]
        obj = f[a0_ref]
        if not config.get("use_h5py"):
            self.assertEqual(obj.id.id, g11.id.id)  # ref to g1.1

        self.assertEqual(obj.name, "/g1/g1.1")
        a1_ref = attr[1]
        obj = f[a1_ref]
        if not config.get("use_h5py"):
            self.assertEqual(obj.id.id, d1.id.id)  # ref to d1
        self.assertEqual(obj.name, "/g2/d1")



    def test_delete(self):
        filename = self.getFileName("objref_delete_test")
        print(filename)
        f = h5py.File(filename, 'w')
        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)
        is_h5serv = False
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            is_h5serv = True  # h5serv doesn't have support for objref datasets yet

        # create a dataset
        dset = f.create_dataset('dset', data=[1,2,3])
        dset_ref = dset.ref

        if not is_h5serv:
            f.attrs["dset_ref"] = dset_ref
            del f['dset']
            try:
                obj = f[dset_ref]
                if config.get("use_h5py"):
                    # TBD - HSDS is not triggering this exception since the object
                    # is not being deleted.
                    self.assertTrue(False)
            except ValueError:
                pass # expected

        f.close()


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
