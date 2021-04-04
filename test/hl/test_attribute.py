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
import logging
import config


if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestAttribute(TestCase):


    def test_create(self):
        filename = self.getFileName("create_attribute")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

        is_hsds = False
        if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
            is_hsds = True  # HSDS has different permission defaults

        g1 = f.create_group('g1')
        
        g1.attrs['a1'] = 42

        n = g1.attrs['a1']
        self.assertEqual(n, 42)

        self.assertTrue('a1' in g1.attrs)
        self.assertTrue(u'a1' in g1.attrs)
        self.assertTrue(b'a1' in g1.attrs)

        self.assertEqual(len(g1.attrs), 1)

        g1.attrs['b1'] = list(range(10))

        # try replacing 'a1'
        g1.attrs['a1'] = 24

        n = g1.attrs['a1']
        self.assertEqual(n, 24)

        self.assertEqual(len(g1.attrs), 2)

        # create an attribute with explict UTF type
        dt = h5py.special_dtype(vlen=str)
        g1.attrs.create('c1', "Hello HDF", dtype=dt)
        self.assertTrue('c1' in g1.attrs)
        value = g1.attrs['c1']
        self.assertEqual(value, "Hello HDF")

        # create attribute with as a fixed length string
        g1.attrs.create('d1', np.string_("This is a numpy string"))
        value = g1.attrs['d1']
        self.assertEqual(value, b"This is a numpy string")

        attr_names = []
        for a in g1.attrs:
            attr_names.append(a)
        self.assertEqual(len(attr_names), 4)
        self.assertTrue('a1' in attr_names)
        self.assertTrue('b1' in attr_names)
        self.assertTrue('c1' in attr_names)
        self.assertTrue('d1' in attr_names)

        # create attribute with null space
        empty = h5py.Empty("float32")
        g1.attrs.create('null_float', empty)
        value = g1.attrs['null_float']
        self.assertEqual(value, empty)

        # create an array attribute
        g1.attrs["ones"] = np.ones((10,))
        arr = g1.attrs["ones"]
        self.assertTrue(isinstance(arr, np.ndarray))
        self.assertEqual(arr.shape, (10,))
        for i in range(10):
            self.assertEqual(arr[i], 1)

        # array of strings
        g1.attrs['strings'] = [np.string_("Hello"), np.string_("Good-bye")]
        arr = g1.attrs['strings']
        self.assertEqual(arr.shape, (2,))
        self.assertEqual(arr[0], b"Hello")
        self.assertEqual(arr[1], b"Good-bye")
        self.assertEqual(arr.dtype.kind, 'S')
     
        # scalar byte values
        g1.attrs['e1'] = "Hello"
        s = g1.attrs['e1']
        self.assertEqual(s, "Hello" )

        # scalar objref attribute
        g11 = g1.create_group('g1.1') # create subgroup g1/g1.1
        g11.attrs['name'] = 'g1.1'   # tag group with an attribute

        if is_hsds:
            # following is not working with h5serv
            g11_ref = g11.ref   # get ref to g1/g1.1
            self.assertTrue(isinstance(g11_ref, h5py.Reference))
            refdt = h5py.special_dtype(ref=h5py.Reference)  # create ref dtype
            g1.attrs.create('f1', g11_ref, dtype=refdt)     # create attribute with ref to g1.1
            ref = g1.attrs['f1'] # read back the attribute

            refobj = f[ref]  # get the ref'd object
            self.assertTrue('name' in refobj.attrs)  # should see the tag attribute
            self.assertEqual(refobj.attrs['name'], 'g1.1')  # check tag value

        # close file
        f.close()


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
