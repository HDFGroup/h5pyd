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
import math
import logging

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase

# test point selection
#
#

class TestPointSelectDataset(TestCase):
    def test_boolean_select(self):
        filename = self.getFileName("point_select_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        data = np.zeros((10,10), dtype='i4')
        for i in range(10):
            for j in range(10):
                data[i,j] = i - j
        dset = f.create_dataset('dset', data=data)
        pos_vals = dset[ data > 0 ]
        self.assertEqual(len(pos_vals), 45)
        for value in pos_vals:
            self.assertTrue(value > 0)

        f.close()

    def test_1d_pointselect(self):
        filename = self.getFileName("test_1d_pointselect")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset1d = f.create_dataset('dset1d', (10,), dtype='i4')
        vals = list(range(10))
        vals.reverse()
        dset1d[...] = vals
        vals = dset1d[...]
        pts = dset1d[ [2,4,6,8] ]
        expected_vals = [7,5,3,1]
        for i in range(len(expected_vals)):
            self.assertEqual(pts[i], expected_vals[i])

        f.close()

    def test_2d_pointselect(self):
        filename = self.getFileName("test_2d_pointselect")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset2d = f.create_dataset('dset2d', (10,20), dtype='i4')
        vals = np.zeros((10,20), dtype='i4')
        for i in range(10):
            for j in range(20):
                vals[i,j] = i*1000 + j

        dset2d[...] = vals
        vals = dset2d[...]
        # TBD: selection with a list for one axis is not working in HSDS
        if config.get("use_h5py"):
            pts = dset2d[  5, (5,10,15) ]
        else:
            # But this type of selection not working for h5py
            # cf: https://github.com/h5py/h5py/issues/966
            pts = dset2d[ [ (5,5), (5,10), (5,15) ] ]

        expected_vals =  [5005,5010,5015]
        for i in range(len(expected_vals)):
            self.assertEqual(pts[i],expected_vals[i])

        pts = dset2d[[1,2]]
        if config.get("use_h5py"):
            # TBD: fix for h5pyd
            self.assertEqual(pts.shape, (2,20))
            for i in range(20):
                self.assertEqual(pts[0,i], vals[1,i])
                self.assertEqual(pts[1,i], vals[2,i])


        f.close()

    def test_2d_pointselect_broadcast(self):
        filename = self.getFileName("test_2d_pointselect_broadcast")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        dset2d = f.create_dataset('dset2d', (10,20), dtype='i4')
        vals = np.zeros((10,20), dtype='i4')
        for i in range(10):
            for j in range(20):
                vals[i,j] = i*1000 + j

        dset2d[...] = vals
        if config.get("use_h5py"):
            # TODO - not working for h5pyd
            pts = dset2d[(2,4,7), :]
            self.assertEqual(len(pts),3)
            row1 = pts[0,:]
            self.assertEqual(list(row1), list(range(2000, 2020)))
            row2 = pts[1,:]
            self.assertEqual(list(row2), list(range(4000, 4020)))
            row3 = pts[2,:]
            self.assertEqual(list(row3), list(range(7000, 7020)))


        f.close()


if __name__ == '__main__':
    #loglevel = logging.DEBUG
    #logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
