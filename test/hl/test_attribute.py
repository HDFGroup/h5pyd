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
import config


if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestAttribute(TestCase):

    def test_create(self):
        filename = self.getFileName("create_attribfute")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

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
        self.assertEqual(s, "Hello")

        # scalar objref attribute
        g11 = g1.create_group('g1.1')  # create subgroup g1/g1.1
        g11.attrs['name'] = 'g1.1'   # tag group with an attribute

        g11_ref = g11.ref   # get ref to g1/g1.1
        self.assertTrue(isinstance(g11_ref, h5py.Reference))
        refdt = h5py.special_dtype(ref=h5py.Reference)  # create ref dtype
        g1.attrs.create('f1', g11_ref, dtype=refdt)     # create attribute with ref to g1.1
        ref = g1.attrs['f1']  # read back the attribute

        refobj = f[ref]  # get the ref'd object
        self.assertTrue('name' in refobj.attrs)  # should see the tag attribute
        self.assertEqual(refobj.attrs['name'], 'g1.1')  # check tag value

        # close file
        f.close()

    def test_create_multiple(self):
        if config.get('use_h5py') or self.hsds_version() < "0.9.0":
            return

        filename = self.getFileName("create_attribute_multiple")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

        g1 = f.create_group('g1')

        num_attrs = 10
        # No shape or dtype specified
        names = ['attr' + str(i) for i in range(num_attrs)]
        values = [np.arange(50)] * num_attrs
        g1.attrs.create(names, values)

        for i in range(num_attrs):
            self.assertTrue(names[i] in g1.attrs)
            self.assertTrue(np.array_equal(g1.attrs[names[i]], values[i]))

        # Test replacing existing attributes
        new_values = [np.arange(100)] * num_attrs
        g1.attrs.create(names, new_values)

        for i in range(num_attrs):
            self.assertTrue(names[i] in g1.attrs)
            self.assertTrue(np.array_equal(g1.attrs[names[i]], new_values[i]))

        # Test creating attributes with shape and dtype specified
        names = ['attr' + str(i) for i in range(num_attrs, 2 * num_attrs)]
        values = [np.arange(i + 1) for i in range(num_attrs)]
        dtypes = [np.int32] * num_attrs
        shapes = [(i + 1,) for i in range(num_attrs)]
        g1.attrs.create(names, values, shapes, dtypes)

        for i in range(num_attrs):
            self.assertTrue(names[i] in g1.attrs)
            self.assertTrue(np.array_equal(g1.attrs[names[i]], values[i]))
            self.assertEqual(g1.attrs[names[i]].dtype, dtypes[i])
            self.assertEqual(g1.attrs[names[i]].shape, shapes[i])

    def test_get_multiple(self):
        if config.get('use_h5py') or self.hsds_version() < "0.9.0":
            return

        filename = self.getFileName("get_attribute_multiple")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

        # create attributes
        num_attrs = 10
        g1 = f.create_group('g1')
        names = ['attr' + str(i) for i in range(num_attrs)]
        values = [np.arange(50) for i in range(num_attrs)]

        for i in range(10):
            g1.attrs[names[i]] = values[i]

        # get all attributes
        values_out = g1.attrs.get_attributes()

        self.assertEqual(len(values_out), 10)
        for i in range(10):
            self.assertTrue(names[i] in values_out)
            self.assertTrue(np.array_equal(values_out[names[i]], values[i]))

        # get attributes from cache
        values_out = g1.attrs.get_attributes(use_cache=True)
        self.assertEqual(len(values_out), 10)
        for i in range(10):
            self.assertTrue(names[i] in values_out)
            self.assertTrue(np.array_equal(values_out[names[i]], values[i]))

        # get attributes that match the pattern 'attr5'
        pattern = "attr5"
        values_out = g1.attrs.get_attributes(pattern=pattern, use_cache=False)

        self.assertTrue("attr5" in values_out)
        self.assertTrue(np.array_equal(values_out["attr5"], values[5]))

        # get attributes that match the pattern 'att*' (all attributes)
        pattern = "att*"
        values_out = g1.attrs.get_attributes(pattern=pattern, use_cache=False)

        self.assertEqual(len(values_out), 10)

        for i in range(10):
            self.assertTrue(names[i] in values_out)
            self.assertTrue(np.array_equal(values_out[names[i]], values[i]))

        # get the first five attributes
        limit = 5
        values_out = g1.attrs.get_attributes(limit=limit, use_cache=False)

        self.assertEqual(len(values_out), 5)

        for i in range(5):
            self.assertTrue(names[i] in values_out)
            self.assertTrue(np.array_equal(values_out[names[i]], values[i]))

        # get all attributes after 'attr4
        marker = "attr4"
        values_out = g1.attrs.get_attributes(marker=marker, limit=limit, use_cache=False)

        self.assertEqual(len(values_out), 5)

        for i in range(6, 10):
            self.assertTrue(names[i] in values_out)
            self.assertTrue(np.array_equal(values_out[names[i]], values[i]))

    def test_delete_multiple(self):
        if config.get('use_h5py') or self.hsds_version() < "0.9.0":
            return

        filename = self.getFileName("delete_attribute_multiple")
        print("filename:", filename)
        f = h5py.File(filename, 'w')

        # create attributes
        num_attrs = 10
        g1 = f.create_group('g1')
        names = ['attr' + str(i) for i in range(num_attrs)]
        values = [np.arange(50) for i in range(num_attrs)]

        for i in range(10):
            g1.attrs[names[i]] = values[i]

        # delete the first five attributes
        del g1.attrs[names[0:5]]

        # check that the first five attributes are gone
        for i in range(5):
            self.assertFalse(names[i] in g1.attrs)

        # check that the last five attributes are still there
        for i in range(5, 10):
            self.assertTrue(names[i] in g1.attrs)
            self.assertTrue(np.array_equal(g1.attrs[names[i]], values[i]))

        # delete single attribute
        del g1.attrs[names[5]]

        self.assertFalse(names[5] in g1.attrs)

        for i in range(6, 10):
            self.assertTrue(names[i] in g1.attrs)
            self.assertTrue(np.array_equal(g1.attrs[names[i]], values[i]))


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
