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
import logging
import six
import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase

# Note: this test currently works with the HSDS server but not h5serv

class TestVlenTypes(TestCase):


    def test_create_vlen(self):
        filename = self.getFileName("create_vlen_attribute")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

        is_hsds = False
        if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
            is_hsds = True  # HSDS has different permission defaults
        if not is_hsds:
            # vlen ref types not working for h5serv, so abort here
            f.close()
            return

        g1 = f.create_group('g1')
        print("g1_id:", g1.id.id)
        g1_1 = g1.create_group('g1_1')
        g1_1.attrs["name"] = 'g1_1'
        g1_2 = g1.create_group('g1_2')
        g1_2.attrs["name"] = 'g1_2'
        g1_3 = g1.create_group('g1_3')
        g1_3.attrs["name"] = 'g1_3'

        # create an attribute that is a VLEN int32
        dtvlen = h5py.special_dtype(vlen=np.dtype('int32'))
        e0 = np.array([0,1,2])
        e1 = np.array([0,1,2,3])
        data = np.array([e0, e1], dtype=object)

        g1.attrs.create("a1", data, shape=(2,), dtype=dtvlen)

        ret_val = g1.attrs["a1"]
        self.assertTrue(isinstance(ret_val, np.ndarray))
        print("got a1:", ret_val)
        print("a1 type:", ret_val.dtype)
        self.assertEqual(len(ret_val), 2)
        print("type ret_val[0]:", type(ret_val[0]))
        self.assertTrue(isinstance(ret_val[0], np.ndarray))
        # py36  attribute[a1]: [array([0, 1, 2], dtype=int32) array([0, 1, 2, 3], dtype=int32)]
        # py27  [(0, 1, 2) (0, 1, 2, 3)]
        self.assertEqual(list(ret_val[0]), [0,1,2])
        self.assertEqual(ret_val[0].dtype, np.dtype('int32'))
        self.assertTrue(isinstance(ret_val[1], np.ndarray))
        self.assertEqual(ret_val[1].dtype, np.dtype('int32'))

        self.assertEqual(list(ret_val[1]), [0,1,2,3])

        # create an attribute that is VLEN ObjRef
        dtref = h5py.special_dtype(ref=h5py.Reference)
        dtvlen = h5py.special_dtype(vlen=dtref)
        e0 = np.array((g1_1.ref,), dtype=dtref)
        print("g1_1.ref:", g1_1.ref)
        print("e0:", e0)
        e1 = np.array((g1_1.ref,g1_2.ref), dtype=dtref)
        e2 = np.array((g1_1.ref,g1_2.ref,g1_3.ref), dtype=dtref)
        data = [e0,e1,e2]

        g1.attrs.create("b1", data, shape=(3,),dtype=dtvlen)

        vlen_val = g1.attrs["b1"]  # read back attribute
        self.assertTrue(isinstance(vlen_val, np.ndarray))
        self.assertEqual(len(vlen_val), 3)
        for i in range(3):
            print("i:", i)
            e = vlen_val[i]
            self.assertTrue(isinstance(e, np.ndarray))
            ref_type = h5py.check_dtype(ref=e.dtype)
            self.assertEqual(ref_type, h5py.Reference)
            self.assertEqual(e.shape, ((i+1),))
            # first element is always a ref to g1
            print(e, type(e))
            refd_group = f[e[0]]
            print(refd_group.id.id)
            print(refd_group.attrs['name'])
            self.assertEqual(refd_group.attrs['name'], 'g1_1')

        # create an attribute with compound type of vlen objref and int32
        dtcompound = np.dtype([('refs', dtvlen), ('number', 'int32')])
        # create np array with data for the attribute
        # note: two step process is needed, see: https://github.com/h5py/h5py/issues/573 
        data = np.zeros((2,), dtype=dtcompound)
        data[0] = (e1, 1)
        data[1] = (e2, 2)
    
        g1.attrs.create("c1", data, shape=(2,), dtype=dtcompound)
        compound_val = g1.attrs["c1"]
        self.assertTrue(isinstance(compound_val, np.ndarray))
        self.assertEqual(len(compound_val), 2)
        self.assertEqual(len(compound_val.dtype), 2)
        for i in range(2):
            item = compound_val[i]
            print(i, ":", item)
            print(type(item))
            self.assertTrue(isinstance(item, np.void))
            self.assertEqual(len(item), 2)
            e = item[0]
            self.assertEqual(len(e), i+2)
            refd_group = f[e[0]]
            print(refd_group.id.id)
            print(refd_group.attrs['name'])
            self.assertEqual(refd_group.attrs['name'], 'g1_1')
            self.assertEqual(item[1], i+1)

        # close file
        f.close()




         

if __name__ == '__main__':
    loglevel = logging.DEBUG
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()




