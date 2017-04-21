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
import json
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

        r.create_group('g1')
        self.assertTrue('g1' in r)
        g1 = r['g1']


        g11 = g1.create_group('g1.1')

        g11_ref = g11.ref
        #print("g11_ref:", g11_ref)
        #print("uuid:", g11_ref.id.uuid)
        #print("domain:", g11_ref.id.domain)
        #print("type:", g11_ref.id.objtype_code)
        #print("g11_ref_tolist:", g11_ref.tolist())

        # todo - fix
        #self.assertTrue(isinstance(g11_ref, h5py.Reference))


        r.create_group('g2')
        self.assertEqual(len(r), 2)
        g2 = r['g2']
        #print("json dump:", json.dumps(g2.__dict__))
        """
        g11ref = g2[g11_ref]
        #print("g11ref:", g11ref)
        #print("g11ref name:", g11ref.name)
        #print("g11ref type:", type(g11ref))
        g11ref.create_group("foo")
        """
        d1 = g2.create_dataset('d1', (10,), dtype='i8')

        d1_ref = d1.ref

        dt = h5py.special_dtype(ref=h5py.Reference)
        #print("dt:", dt)
        #print("dt.kind:", dt.kind)
        #print("dt.meta:", dt.metadata['ref'])
        print("special_dtype:", type(dt.metadata['ref']))
        self.assertTrue(dt.metadata['ref'] is h5py.Reference)

        dset = g1.create_dataset('myrefs', (10,), dtype=dt)
        #print("dset kind:", dset.dtype.kind)
        #print("dset.dtype.kind:", dset.dtype.kind)
        ref = h5py.check_dtype(ref=dset.dtype)
        #print("check_dtype:", ref)
        null_ref = dset[0]
        #print("null_ref:", null_ref)
        dset[0] = g11_ref
        dset[1] = d1_ref
        #g2.attrs['dataset'] = dset.ref

        # todo - references as data will need h5pyd equivalent of h5t module
        # g2.attrs.create('dataset', dset.ref, dtype=dt)
        #print("g11_ref type:", type(g11_ref))
        a_ref = dset[0]
        #print("a_ref", type(a_ref) )

        f.close()

if __name__ == '__main__':
    ut.main()




