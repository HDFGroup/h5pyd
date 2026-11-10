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
""" Values in ~/.hscfg are read whole, including any "=" they contain """

import os
import unittest

from stubs import scratch_home

from h5pyd.config import Config
from h5pyd._apps.config import Config as AppConfig
from h5pyd._apps import hsconfigure

PASSWORD = "pa=ss=word"
API_KEY = "c2VjcmV0a2V5MQ=="  # base64 values often end in "="


class TestConfigParse(unittest.TestCase):

    def _check(self, cfg):
        self.assertEqual(cfg["hs_password"], PASSWORD)
        self.assertEqual(cfg["hs_api_key"], API_KEY)
        self.assertEqual(cfg["hs_endpoint"], "https://hsds.example.org")

    def test_values_with_equals(self):
        with scratch_home() as (home, cwd):
            path = os.path.join(home, ".hscfg")
            with open(path, "w") as f:
                f.write("hs_endpoint = https://hsds.example.org\n")
                f.write(f"hs_password = {PASSWORD}\n")
                f.write(f"hs_api_key={API_KEY}\n")
            os.chmod(path, 0o600)
            self._check(Config())
            self._check(AppConfig())

    def test_hsconfigure_round_trip(self):
        with scratch_home():
            hsconfigure.saveConfig("u", PASSWORD, "https://hsds.example.org", API_KEY)
            self._check(Config())
            self._check(AppConfig())


if __name__ == "__main__":
    unittest.main()
