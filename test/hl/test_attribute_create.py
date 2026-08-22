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

"""
    Tests the h5py.AttributeManager.create() method.
"""
import numpy as np
import config


if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestArray(TestCase):

    """
        Check that top-level array types can be created and read.
    """

    def setUp(self):
        filename = self.getFileName("attribute_create")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')

    def _get_type_class(self, name):
        """ Return the h5json type class (e.g. 'H5T_ARRAY') for the
        given attribute - the hsds equivalent of h5a.open(...).get_type() """
        attr_json = self.f.id.db.getAttribute(self.f.id.uuid, name)
        return attr_json["type"]["class"]

    def test_int(self):
        # See issue 498
        name = "int_array_attr"
        dt = np.dtype('(3,)i')
        data = np.arange(3, dtype='i')

        self.f.attrs.create(name, data=data, dtype=dt)

        if config.get("use_h5py"):
            aid = h5py.h5a.open(self.f.id, name.encode('utf-8'))
            htype = aid.get_type()
            self.assertEqual(htype.get_class(), h5py.h5t.ARRAY)
        else:
            self.assertEqual(self._get_type_class(name), "H5T_ARRAY")

        out = self.f.attrs[name]

        self.assertArrayEqual(out, data)

    def test_string_dtype(self):
        # See issue 498 discussion
        self.f.attrs.create("string_dtype_attr", data=42, dtype='i8')

    def test_str(self):
        # See issue 1057
        name = "str_attr"
        self.f.attrs.create(name, chr(0x03A9))
        out = self.f.attrs[name]
        self.assertEqual(out, chr(0x03A9))
        self.assertIsInstance(out, str)

    def test_tuple_of_unicode(self):
        # Test that a tuple of unicode strings can be set as an attribute. It will
        # be converted to a numpy array of vlen unicode type:
        name = "tuple_of_unicode_attr"
        data = ('a', 'b')
        self.f.attrs.create(name, data=data)
        result = self.f.attrs[name]
        self.assertTrue(all(result == data))
        self.assertEqual(result.dtype, np.dtype('O'))

    def test_unicode_np_array(self):
        # However, a numpy array of type U being passed in will not be
        # automatically converted, and should raise an error as it does
        # not map to a h5py dtype
        data = np.array(['a', 'b'], dtype='U1')
        with self.assertRaises(TypeError):
            self.f.attrs.create('x', data=data)

    def test_shape_scalar(self):
        name = "shape_scalar_attr"
        self.f.attrs.create(name, data=42, shape=1)
        result = self.f.attrs[name]
        self.assertEqual(result.shape, (1,))

    def test_shape_array(self):
        name = "shape_array_attr"
        self.f.attrs.create(name, data=np.arange(3), shape=3)
        result = self.f.attrs[name]
        self.assertEqual(result.shape, (3,))

    def test_dtype(self):
        dt = np.dtype('(3,)i')
        array = np.arange(3, dtype='i')
        self.f.attrs.create("dtype_attr", data=array, dtype=dt)
        # Array dtype shape is incompatible with data shape
        array = np.arange(4, dtype='i')
        with self.assertRaises(ValueError):
            self.f.attrs.create('x', data=array, dtype=dt)
        # Shape of new attribute conflicts with shape of data
        dt = np.dtype('()i')
        with self.assertRaises(ValueError):
            self.f.attrs.create('x', data=array, shape=(5,), dtype=dt)

    def test_key_type(self):
        with self.assertRaises(TypeError):
            self.f.attrs.create(1, data=('a', 'b'))


if __name__ == '__main__':
    ut.main()
