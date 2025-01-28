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
        filename = self.getFileName("create_attribute")
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
        g1.attrs.create('d1', np.bytes_("This is a numpy string"))
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
        g1.attrs['strings'] = [np.bytes_("Hello"), np.bytes_("Good-bye")]
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


class TestTrackOrder(TestCase):

    titles = ("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")

    def fill_attrs(self, obj):
        count = len(self.titles)
        attrs = obj.attrs
        for i in range(count):
            title = self.titles[i]
            val = i + 1
            attrs[title] = val

    def test_track_order(self):
        filename = self.getFileName("test_test_track_order_attribute")
        print(f"filename: {filename}")
        # use max_age as 0 because pending writes messes up the tracking order
        # TBD: find work-around for this
        with h5py.File(filename, 'w', max_age=0.0) as f:
            grp1 = f.create_group('grp1', track_order=True)
            self.fill_attrs(grp1)
            self.assertEqual(list(grp1.attrs), list(self.titles))
            dset1 = f.create_dataset('dset1', data=[42,], track_order=True)
            self.fill_attrs(dset1)
            dset2 = f.create_dataset_like('dset2', dset1)
            self.fill_attrs(dset2)
            self.assertEqual(list(dset1.attrs), list(self.titles))
            self.assertEqual(list(dset2.attrs), list(self.titles))
        # group should return track order
        with h5py.File(filename) as f:
            grp1 = f['grp1']
            self.assertEqual(list(grp1.attrs), list(self.titles))
            dset1 = f['dset1']
            self.assertEqual(list(dset1.attrs), list(self.titles))
            dset2 = f['dset2']
            self.assertEqual(list(dset2.attrs), list(self.titles))

    def test_track_order_cfg(self):
        filename = self.getFileName("test_test_track_order_attribute")
        print(f"filename: {filename}")
        cfg = h5py.get_config()
        with h5py.File(filename, 'w', max_age=0.0) as f:
            cfg.track_order = True
            grp1 = f.create_group('grp1')
            dset1 = f.create_dataset('dset1', data=[42,])
            cfg.track_order = False  # reset
            self.fill_attrs(grp1)
            self.fill_attrs(dset1)
            self.assertEqual(list(grp1.attrs), list(self.titles))
            self.assertEqual(list(dset1.attrs), list(self.titles))

        with h5py.File(filename) as f:
            grp1 = f['grp1']
            self.assertEqual(list(grp1.attrs), list(self.titles))
            dset1 = f['dset1']
            self.assertEqual(list(dset1.attrs), list(self.titles))

    def test_no_track_order(self):
        filename = self.getFileName("test_test_no_track_order_attribute")
        print(f"filename: {filename}")
        f = h5py.File(filename, 'w', max_age=0.0)
        g1 = f.create_group('test')  # name alphanumeric
        self.fill_attrs(g1)
        self.assertEqual(list(g1.attrs), sorted(list(self.titles)))

    def test_track_order_overwrite_delete(self):
        filename = self.getFileName("test_test_track_order_overwrite_delete")
        print(f"filename: {filename}")
        f = h5py.File(filename, 'w', max_age=0.0)

        g1 = f.create_group("g1", track_order=True)  # creation order
        self.fill_attrs(g1)
        title = 'three'
        self.assertEqual(g1.attrs[title], 3)
        # overwrite attribute
        g1.attrs[title] = 42.0
        self.assertEqual(g1.attrs[title], 42.0)
        # delete attribute
        self.assertIn(title, g1.attrs)
        del g1.attrs[title]
        self.assertNotIn(title, g1.attrs)

    def test_track_order_not_inherited(self):
        """
        Test that if a File has track order enabled and a sub group does not,
        that alphanumeric order is used within the sub group
        """
        filename = self.getFileName("test_test_track_order_not_inherited")
        print(f"filename: {filename}")
        f = h5py.File(filename, 'w', track_order=True, max_age=0.0)
        g1 = f.create_group('test')
        self.fill_attrs(g1)

        self.assertEqual(list(g1.attrs), sorted(list(self.titles)))


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
