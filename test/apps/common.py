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
import os
import os.path as op
import tempfile
import time
import config


from six import unichr

import numpy as np

if sys.version_info >= (2, 7) or sys.version_info >= (3, 2):
    import unittest as ut
else:
    try:
        import unittest2 as ut
    except ImportError:
        raise ImportError(
            'unittest2 is required to run the test suite with python-%d.%d'
            % (sys.version_info[:2])
            )


# Check if non-ascii filenames are supported
# Evidently this is the most reliable way to check
# See also h5py issue #263 and ipython #466
# To test for this, run the testsuite with LC_ALL=C
try:
    testfile, fname = tempfile.mkstemp(unichr(0x03b7))
except UnicodeError:
    UNICODE_FILENAMES = False
else:
    UNICODE_FILENAMES = True
    os.close(testfile)
    os.unlink(fname)
    del fname
    del testfile


class TestCase(ut.TestCase):

    """
        Base class for unit tests.
    """

    @property
    def test_user1(self):
        # HS_USERNAME is the username h5pyd will look up if
        #   if not provided in the File constructor
        user1 = {}
        if  "HS_USERNAME" in os.environ:
            user1["name"] = os.environ["HS_USERNAME"]
        else:
            user1["name"] = "test_user1"
        if "HS_PASSWORD" in os.environ:
            user1["password"] = os.environ["HS_PASSWORD"]
        else:
            # only use "test_user1/test" for desktop testing
            user1["password"] = "test"
        return user1

    @property
    def test_user2(self):
        user2 = {}
        if  "TEST12_USERNAME" in os.environ:
            user2["name"] = os.environ["TEST2_USERNAME"]
        else:
            user2["name"] = "test_user2"
        if "TEST2_PASSWORD" in os.environ:
            user2["password"] = os.environ["TEST2_PASSWORD"]
        else:
            # only use "test_user1/test" for desktop testing
            user2["password"] = "test"
        return user2

    @classmethod
    def use_h5py():
        """ Use the standard H5PY package rather than h5pyd"""
        if "USE_H5PY" in os.environ and os.environ["USE_H5PY"]:
            return True
        else:
            return False

    @classmethod
    def setUpClass(cls):
        pass
        #cls.tempdir = tempfile.mkdtemp(prefix='h5py-test_')

    @classmethod
    def tearDownClass(cls):
        pass
        #shutil.rmtree(cls.tempdir)

    def setUp(self):
        self.test_dir = str(int(time.time()))
        #self.f = h5py.File(self.mktemp(), 'w')

    def tearDown(self):
        try:
            if self.f:
                self.f.close()
        except:
            pass

    if not hasattr(ut.TestCase, 'assertSameElements'):
        # shim until this is ported into unittest2
        def assertSameElements(self, a, b):
            for x in a:
                match = False
                for y in b:
                    if x == y:
                        match = True
                if not match:
                    raise AssertionError("Item '%s' appears in a but not b" % x)

            for x in b:
                match = False
                for y in a:
                    if x == y:
                        match = True
                if not match:
                    raise AssertionError("Item '%s' appears in b but not a" % x)

    def assertArrayEqual(self, dset, arr, message=None, precision=None):
        """ Make sure dset and arr have the same shape, dtype and contents, to
            within the given precision.

            Note that dset may be a NumPy array or an HDF5 dataset.
        """
        if precision is None:
            precision = 1e-5
        if message is None:
            message = ''
        else:
            message = ' (%s)' % message

        if np.isscalar(dset) or np.isscalar(arr):
            self.assertTrue(
                np.isscalar(dset) and np.isscalar(arr),
                'Scalar/array mismatch ("%r" vs "%r")%s' % (dset, arr, message)
            )
            self.assertTrue(
                dset - arr < precision,
                "Scalars differ by more than %.3f%s" % (precision, message)
            )
            return

        self.assertTrue(
            dset.shape == arr.shape,
            "Shape mismatch (%s vs %s)%s" % (dset.shape, arr.shape, message)
            )
        self.assertTrue(
            dset.dtype == arr.dtype,
            "Dtype mismatch (%s vs %s)%s" % (dset.dtype, arr.dtype, message)
            )

        if arr.dtype.names is not None:
            for n in arr.dtype.names:
                message = '[FIELD %s] %s' % (n, message)
                self.assertArrayEqual(dset[n], arr[n], message=message, precision=precision)
        elif arr.dtype.kind in ('i', 'f'):
            self.assertTrue(
                np.all(np.abs(dset[...] - arr[...]) < precision),
                "Arrays differ by more than %.3f%s" % (precision, message)
                )
        else:
            self.assertTrue(
                np.all(dset[...] == arr[...]),
                "Arrays are not equal (dtype %s) %s" % (arr.dtype.str, message)
                )

    def assertNumpyBehavior(self, dset, arr, s):
        """ Apply slicing arguments "s" to both dset and arr.

        Succeeds if the results of the slicing are identical, or the
        exception raised is of the same type for both.

        "arr" must be a Numpy array; "dset" may be a NumPy array or dataset.
        """
        exc = None
        try:
            arr_result = arr[s]
        except Exception as e:
            exc = type(e)

        if exc is None:
            self.assertArrayEqual(dset[s], arr_result)
        else:
            with self.assertRaises(exc):
                dset[s]

    def getFileName(self, basename):
        """
        Get filepath for a test case given a testname
        """

        if config.get("use_h5py"):
            if not op.isdir("out"):
                os.mkdir("out")
            filename = "out/" + basename + ".h5"
        else:
            if "H5PYD_TEST_FOLDER" in os.environ:
                domain = os.environ["H5PYD_TEST_FOLDER"]
            else:
                domain = "h5pyd_test.hdfgroup.org"
            if domain.find('/') > -1:
                # Use path-style domain naming
                filename = op.join(domain, basename)
                filename += ".h5"
            else:
                filename = basename + "." + domain
        return filename


    def getPathFromDomain(self, domain):
        """
        Convert DNS-style domain name to filepath
        E.g. "mytest.h5pyd_test.hdfgroup.org" to
             "/org/hdfgroup/h5pyd_test/mytest
        """
        if domain.find('/') > -1:
            # looks like the domain already is specified as a path
            return domain

        names = domain.split('.')
        names.reverse()
        path = '/'
        for name in names:
            if name:
                 path += name
                 path += '/'
        path = path[:-1]  # strip trailing slash
        return path


