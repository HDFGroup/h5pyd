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


class TestDatasetSwmrRead(TestCase):
    """ Testing SWMR functions when reading a dataset.
    Skip this test if the HDF5 library does not have the SWMR features.
    """

    def setUp(self):

        filename = self.getFileName("test_dataset_swmr_read")
        print("filename:", filename)
        self.f = h5py.File(filename, 'w')

        self.data = np.arange(13).astype('f')
        self.dset = self.f.create_dataset('data', chunks=(13,), maxshape=(None,), data=self.data)
        fname = self.f.filename
        self.f.close()

        self.f = h5py.File(fname, 'r', swmr=True)
        self.dset = self.f['data']

    def test_initial_swmr_mode_on(self):
        """ Verify that the file is initially in SWMR mode"""
        self.assertTrue(self.f.swmr_mode)

    def test_read_data(self):
        self.assertArrayEqual(self.dset, self.data)

    def test_refresh(self):
        self.dset.refresh()

    def test_force_swmr_mode_on_raises(self):
        """ Verify when reading a file cannot be forcibly switched to swmr mode.
        When reading with SWMR the file must be opened with swmr=True."""
        with self.assertRaises(Exception):
            self.f.swmr_mode = True
        self.assertTrue(self.f.swmr_mode)

    def test_force_swmr_mode_off_raises(self):
        """ Switching SWMR write mode off is only possible by closing the file.
        Attempts to forcibly switch off the SWMR mode should raise a ValueError.
        """
        with self.assertRaises(ValueError):
            self.f.swmr_mode = False
        self.assertTrue(self.f.swmr_mode)


class TestDatasetSwmrWrite(TestCase):
    """ Testing SWMR functions when reading a dataset.
    Skip this test if the HDF5 library does not have the SWMR features.
    """

    def setUp(self):
        """ First setup a file with a small chunked and empty dataset.
        No data written yet.
        """

        filename = self.getFileName("test_data_swmr_write")
        print("filename:", filename)

        # Note that when creating the file, the swmr=True is not required for
        # write, but libver='latest' is required.
        self.f = h5py.File(filename, 'w', libver='latest')

        self.data = np.arange(4).astype('f')
        kwargs = {"dtype": self.data.dtype, "shape": (0,), "maxshape": (None,), "chunks": (2,)}
        self.dset = self.f.create_dataset('data', **kwargs)

    def test_initial_swmr_mode_off(self):
        """ Verify that the file is not initially in SWMR mode"""
        self.assertFalse(self.f.swmr_mode)

    def test_switch_swmr_mode_on(self):
        """ Switch to SWMR mode and verify """
        self.f.swmr_mode = True
        self.assertTrue(self.f.swmr_mode)

    def test_switch_swmr_mode_off_raises(self):
        """ Switching SWMR write mode off is only possible by closing the file.
        Attempts to forcibly switch off the SWMR mode should raise a ValueError.
        """
        self.f.swmr_mode = True
        self.assertTrue(self.f.swmr_mode)
        with self.assertRaises(ValueError):
            self.f.swmr_mode = False
        self.assertTrue(self.f.swmr_mode)

    def test_extend_dset(self):
        """ Extend and flush a SWMR dataset
        """
        self.f.swmr_mode = True
        self.assertTrue(self.f.swmr_mode)

        self.dset.resize(self.data.shape)
        self.dset[:] = self.data
        self.dset.flush()

        # Refresh and read back data for assertion
        self.dset.refresh()
        self.assertArrayEqual(self.dset, self.data)

    def test_extend_dset_multiple(self):

        self.f.swmr_mode = True
        self.assertTrue(self.f.swmr_mode)

        self.assertEqual(self.dset.maxshape, (None,))

        self.dset.resize((4,))

        self.assertEqual(self.dset.maxshape, (None,))

        self.dset[0:] = self.data
        self.dset.flush()

        # Refresh and read back 1st data block for assertion
        self.dset.refresh()
        self.assertArrayEqual(self.dset, self.data)

        self.dset.resize((8,))
        self.dset[4:] = self.data
        self.dset.flush()

        # Refresh and read back 1st data block for assertion
        self.dset.refresh()
        self.assertArrayEqual(self.dset[0:4], self.data)
        self.assertArrayEqual(self.dset[4:8], self.data)


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
