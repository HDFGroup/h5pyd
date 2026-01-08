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

import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase
from copy import copy
import time
import logging


class TestFileRead(TestCase):

    def test_tall(self):
        filename = "/home/test_user1/test/tall.h5"
        print("filename:", filename)
        now = time.time()
        f = h5py.File(filename, 'r')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 2)
        self.assertTrue("g1" in f)
        self.assertTrue("g2" in f)
        self.assertEqual(f.mode, 'r')

        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)

        # Check domain's timestamps
        if h5py.__name__ == "h5pyd":
            self.assertTrue(f.created < now - 30)
            self.assertTrue(f.modified < now - 30)

            # TBD this block requires getStats
            """
            self.assertTrue(len(f.owner) > 0)
            version = f.serverver
            server version should be of form "n.n.n"
            n = version.find(".")
            self.assertTrue(n >= 1)
            limits = f.limits
            for k in ('min_chunk_size', 'max_chunk_size', 'max_request_size'):
                self.assertTrue(k in limits)
            """

        r = f['/']
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertEqual(len(r.attrs), 2)
        self.assertTrue("attr1" in r.attrs)
        self.assertTrue("attr2" in r.attrs)
        self.assertTrue("/g1/g1.1/dset1.1.1" in r)
        dset111 = r["/g1/g1.1/dset1.1.1"]
        self.assertTrue(isinstance(dset111, h5py.Dataset))
        self.assertEqual(dset111.shape, (10, 10))
        self.assertEqual(dset111.dtype.itemsize, 4)
        self.assertEqual(dset111.dtype.kind, 'i')
        self.assertEqual(dset111.dtype.byteorder, '>')
        for i in range(10):
            for j in range(10):
                self.assertEqual(dset111[i, j], i * j)
        self.assertTrue("g1" in r)
        g1 = f["g1"]
        self.assertTrue(isinstance(g1, h5py.Group))
        self.assertEqual(len(g1), 2)

        ext_link = g1.get("g1.2/extlink", getlink=True)
        self.assertTrue(isinstance(ext_link, h5py.ExternalLink))
        self.assertEqual(ext_link.filename, "somefile")
        self.assertEqual(ext_link.path, "somepath")

        soft_link = g1.get("g1.2/g1.2.1/slink", getlink=True)
        self.assertTrue(isinstance(soft_link, h5py.SoftLink))
        self.assertEqual(soft_link.path, "somevalue")

        f.close()


if __name__ == '__main__':
    loglevel = logging.DEBUG
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
