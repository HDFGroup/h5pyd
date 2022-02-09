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
from __future__ import absolute_import
import sys
import logging
import numpy as np
import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase

"""
    Tests the h5py.Dataset.__getitem__ method.

    This module does not specifically test type conversion.  The "type" axis
    therefore only tests objects which interact with the slicing system in
    unreliable ways; for example, compound and array types.

    See test_dataset_getitem_types for type-conversion tests.

    Tests are organized into TestCases by dataset shape and type.  Test
    methods vary by slicing arg type.

    1. Dataset shape:
        Empty
        Scalar
        1D
        3D

    2. Type:
        Float
        Compound
        Array

    3. Slicing arg types:
        Ellipsis
        Empty tuple
        Regular slice
        Indexing
        Index list
        Boolean mask
        Field names
"""

"""
Disabled since low-level interface not supported with h5pyd
Update using new NULL dataset constructor once h5py 2.7 is out.
"""

class TestEmpty(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_testempty")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.dset = self.f.create_dataset('x',dtype='S10')
        self.empty_obj = h5py.Empty(np.dtype("S10"))

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 0)

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, None)

    def test_size(self):
        """ Verify shape """
        self.assertEqual(self.dset.size, None)

    def test_nbytes(self):
        """ Verify nbytes """
        self.assertEqual(self.dset.nbytes, 0)

    def test_ellipsis(self):
        self.assertEqual(self.dset[...], self.empty_obj)

    def test_tuple(self):
        self.assertEqual(self.dset[()], self.empty_obj)

    def test_slice(self):
        """ slice -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0:4]

    def test_multi_block_slice(self):
        """ MultiBlockSlice -> ValueError """
        """ TBD """
        #with self.assertRaises(ValueError):
        #    self.dset[h5py.MultiBlockSlice()]

    def test_index(self):
        """ index -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0]

    def test_indexlist(self):
        """ index list -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[[1,2,5]]

    def test_mask(self):
        """ mask -> ValueError """
        mask = np.array(True, dtype='bool')
        with self.assertRaises(ValueError):
            self.dset[mask]

    def test_fieldnames(self):
        """ field name -> ValueError """
        with self.assertRaises(ValueError):
            self.dset['field']
      

class TestScalarFloat(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_testscalarflot")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.array(42.5, dtype='f')
        self.dset = self.f.create_dataset('x', data=self.data)

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 0)

    def test_size(self):
        """ Verify size """
        self.assertEqual(self.dset.size, 1)

    def test_nbytes(self):
        """ Verify nbytes """
        self.assertEqual(self.dset.nbytes, self.data.dtype.itemsize)  # not sure if 'f' is always alias for 'f4'

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, tuple())

    def test_ellipsis(self):
        """ Ellipsis -> scalar ndarray """
        out = self.dset[...]
        self.assertArrayEqual(out, self.data)

    def test_tuple(self):
        """ () -> bare item """
        out = self.dset[()]
        self.assertArrayEqual(out, self.data.item())

    def test_slice(self):
        """ slice -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0:4]

    def test_index(self):
        """ index -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0]

    # FIXME: NumPy has IndexError instead
    def test_indexlist(self):
        """ index list -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[[1,2,5]]

    # FIXME: NumPy permits this
    def test_mask(self):
        """ mask -> ValueError """
        mask = np.array(True, dtype='bool')
        with self.assertRaises(ValueError):
            self.dset[mask]

    def test_fieldnames(self):
        """ field name -> ValueError (no fields) """
        with self.assertRaises(ValueError):
            self.dset['field']


class TestScalarCompound(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_testscalarcompound")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.array((42.5, -118, "Hello"), dtype=[('a', 'f'), ('b', 'i'), ('c', '|S10')])
        #self.dset = self.f.create_dataset('x', data=self.data)
        self.dset = self.f.create_dataset('x', (), dtype=[('a', 'f'), ('b', 'i'), ('c', '|S10')])
        self.dset[...] =  (42.5, -118, "Hello")

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 0)

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, tuple())

    def test_size(self):
        """ Verify size """
        self.assertEqual(self.dset.size, 1)

    def test_nbytes(self):
        """ Verify nbytes """
        self.assertEqual(self.dset.nbytes, self.data.dtype.itemsize)

    def test_ellipsis(self):
        """ Ellipsis -> scalar ndarray """
        out = self.dset[...]
        # assertArrayEqual doesn't work with compounds; do manually
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.shape, self.data.shape)
        self.assertEqual(out.dtype, self.data.dtype)

    def test_tuple(self):
        """ () -> np.void instance """
        out = self.dset[()]
        self.assertIsInstance(out, np.void)
        self.assertEqual(out.dtype, self.data.dtype)

    def test_slice(self):
        """ slice -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0:4]

    def test_index(self):
        """ index -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0]

    # FIXME: NumPy has IndexError instead
    def test_indexlist(self):
        """ index list -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[[1,2,5]]

    # FIXME: NumPy permits this
    def test_mask(self):
        """ mask -> ValueError  """
        mask = np.array(True, dtype='bool')
        with self.assertRaises(ValueError):
            self.dset[mask]

    # failed with earlier h5py versions
    @ut.skip
    def test_fieldnames(self):
        """ field name -> bare value """
        #TBD: fix when field access is supported in h5serv/hsds
        out = self.dset['a']
        self.assertIsInstance(out, np.float32)
        self.assertEqual(out, self.dset['a'])



class TestScalarArray(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_testscalararray")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.dt = np.dtype('(3,2)f')
        self.data = np.array([(3.2, -119), (42, 99.8), (3.14, 0)], dtype='f')
        self.dset = self.f.create_dataset('x', (), dtype=self.dt)
        try:
            self.dset[...] = self.data
        except (IOError, OSError) as oe:
            #TBD this is failing on HSDS
            if not self.is_hsds():
                raise oe

    # FIXME: HSDS failure
    @ut.expectedFailure
    def test_ellipsis(self):
        """ Ellipsis -> ndarray promoted to underlying shape """
        out = self.dset[...]
        self.assertArrayEqual(out, self.data)

    # FIXME: HSDS failure
    @ut.expectedFailure
    def test_tuple(self):
        """ () -> same as ellipsis """
        out = self.dset[...]
        self.assertArrayEqual(out, self.data)

    def test_slice(self):
        """ slice -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0:4]

    def test_index(self):
        """ index -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[0]

    def test_indexlist(self):
        """ index list -> ValueError """
        with self.assertRaises(ValueError):
            self.dset[[]]

    def test_mask(self):
        """ mask -> ValueError """
        mask = np.array(True, dtype='bool')
        with self.assertRaises(ValueError):
            self.dset[mask]

    def test_fieldnames(self):
        """ field name -> ValueError (no fields) """
        with self.assertRaises(ValueError):
            self.dset['field']


class Test1DZeroFloat(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_test1dzerofloat")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.ones((0,), dtype='f')
        self.dset = self.f.create_dataset('x', data=self.data)

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 1)

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, (0,))

    def test_ellipsis(self):
        """ Ellipsis -> ndarray of matching shape """
        self.assertNumpyBehavior(self.dset, self.data, np.s_[...])

    def test_tuple(self):
        """ () -> same as ellipsis """
        self.assertNumpyBehavior(self.dset, self.data, np.s_[()])

    def test_slice(self):
        """ slice -> ndarray of shape (0,) """
        self.assertNumpyBehavior(self.dset, self.data, np.s_[0:4])

    def test_slice_stop_less_than_start(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[7:5])

    def test_index(self):
        """ index -> out of range """
        with self.assertRaises(IndexError):
            self.dset[0]

    def test_indexlist(self):
        """ index list """
        self.assertNumpyBehavior(
            self.dset,
            self.data,
            np.s_[[]]
        )

    def test_mask(self):
        """ mask -> ndarray of matching shape """
        mask = np.ones((0,), dtype='bool')
        self.assertNumpyBehavior(
            self.dset, self.data, np.s_[mask])

    def test_fieldnames(self):
        """ field name -> ValueError (no fields) """
        with self.assertRaises(ValueError):
            self.dset['field']


class Test1DFloat(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_test1dfloat")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.arange(13).astype('f')
        self.dset = self.f.create_dataset('x', data=self.data)
        self.dset[...] = self.data

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 1)

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, (13,))

    def test_ellipsis(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[...])

    def test_tuple(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[()])

    def test_slice_simple(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[0:4])

    def test_slice_zerosize(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[4:4])

    def test_slice_strides(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[1:7:3])

    def test_slice_negindexes(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[-8:-2:3])

    def test_slice_stop_less_than_start(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[7:5])

    def test_slice_outofrange(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[100:400:3])

    def test_slice_backwards(self):
        """ we disallow negative steps """
        with self.assertRaises(ValueError):
            self.dset[::-1]

    def test_slice_zerostride(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[::0])

    def test_index_simple(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[3])

    def test_index_neg(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[-4])

    # FIXME: NumPy permits this... it adds a new axis in front
    def test_index_none(self):
        with self.assertRaises(TypeError):
            self.dset[None]

    def test_index_illegal(self):
        """ Illegal slicing argument """
        with self.assertRaises(TypeError):
            self.dset[{}]

    def test_index_outofrange(self):
        with self.assertRaises(IndexError):
            self.dset[100]

    def test_indexlist_simple(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[[1,2,5]])

    def test_indexlist_single_index_ellipsis(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[[0], ...])

    def test_indexlist_numpyarray_single_index_ellipsis(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[np.array([0]), ...])

    def test_indexlist_numpyarray_ellipsis(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[np.array([1, 2, 5]), ...])

    def test_indexlist_empty(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[[]])

    def test_indexlist_outofrange(self):
        with self.assertRaises(IndexError):
            self.dset[[100]]

    def test_indexlist_nonmonotonic(self):
        """ we require index list values to be strictly increasing """
        with self.assertRaises(TypeError):
            self.dset[[1,3,2]]

    def test_indexlist_repeated(self):
        """ we forbid repeated index values """
        with self.assertRaises(TypeError):
            self.dset[[1,1,2]]

    def test_mask_true(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[self.data > -100])

    def test_mask_false(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[self.data > 100])

    def test_mask_partial(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[self.data > 5])

    def test_mask_wrongsize(self):
        """ we require the boolean mask shape to match exactly """
        with self.assertRaises(TypeError):
            self.dset[np.ones((2,), dtype='bool')]

    def test_fieldnames(self):
        """ field name -> ValueError (no fields) """
        with self.assertRaises(ValueError):
            self.dset['field']


class Test2DZeroFloat(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_test2dzerofloat")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.ones((0,3), dtype='f')
        self.dset = self.f.create_dataset('x', data=self.data)

    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 2)

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, (0, 3))

    def test_indexlist(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[:,[0,1,2]])

class Test2DFloat(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_test2dfloat")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.ones((5,3), dtype='f')
        self.dset = self.f.create_dataset('x', data=self.data)
         
    def test_ndim(self):
        """ Verify number of dimensions """
        self.assertEqual(self.dset.ndim, 2)

    def test_size(self):
        """ Verify size """
        self.assertEqual(self.dset.size, 15)

    def test_nbytes(self):
        """ Verify nbytes """
        self.assertEqual(self.dset.nbytes, 15*self.data.dtype.itemsize) 

    def test_shape(self):
        """ Verify shape """
        self.assertEqual(self.dset.shape, (5, 3))

    def test_indexlist(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[:,[0,1,2]])

    @ut.expectedFailure
    def test_index_emptylist(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[:, []])
        # test with single empty list failing - but results seems compat
        # with h5py 3.2.1 at least
        self.assertNumpyBehavior(self.dset, self.data, np.s_[[]])

class Test3DFloat(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_test3dfloat")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.data = np.ones((4,6,8), dtype='f')
        self.dset = self.f.create_dataset('x', data=self.data, dtype='f')

    def test_index_simple(self):
        self.assertNumpyBehavior(self.dset, self.data, np.s_[1,2:4,3:6])

class TestVeryLargeArray(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        filename = self.getFileName("dataset_testverylargearray")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')
        self.dset = self.f.create_dataset('x', shape=(2**15, 2**16))

    @ut.skipIf(sys.maxsize < 2**31, 'Maximum integer size >= 2**31 required')
    def test_size(self):
        self.assertEqual(self.dset.size, 2**31)

if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
