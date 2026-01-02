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

import logging
import config


if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase


class TestConfig(TestCase):

    def test_config_h5py(self):
        cfg = h5py.get_config()

        self.assertEqual(cfg.bool_names, (b"FALSE", b"TRUE"))
        self.assertEqual(cfg.complex_names, ("r", "i"))
        self.assertEqual(cfg.track_order, False)

        cfg.bool_names = ("nope", "yep")
        cfg.complex_names = ("real", "imag")
        cfg.track_order = True

        cfg2 = h5py.get_config()
        self.assertEqual(cfg2.bool_names, ("nope", "yep"))
        self.assertEqual(cfg2.complex_names, ("real", "imag"))
        self.assertEqual(cfg2.track_order, True)

    def test_config_hs(self):
        if config.get("use_h5py"):
            return  # test with h5pyd only
        cfg = h5py.get_config()
        self.assertTrue(cfg.hs_endpoint.startswith("http"))
        cfg["XYZ"] = 42
        cfg2 = h5py.get_config()
        self.assertEqual(cfg2["XYZ"], 42)


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
