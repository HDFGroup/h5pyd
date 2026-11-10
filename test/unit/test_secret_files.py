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
""" Modes of the files h5pyd writes secrets to, and warnings about files others can read """

import json
import os
import stat
import unittest
import warnings

from stubs import scratch_home

from h5pyd import _credentials
from h5pyd.config import Config
from h5pyd.openid import OpenIDHandler
from h5pyd._apps import hsconfigure
from h5pyd._apps.config import Config as AppConfig


def file_mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def write(path, text, mode):
    with open(path, "w") as f:
        f.write(text)
    os.chmod(path, mode)


class StubOpenID(OpenIDHandler):
    def acquire(self):
        self._token = {"accessToken": "tok"}

    def refresh(self):
        pass


@unittest.skipUnless(os.name == "posix", "file modes are POSIX only")
class TestSecretFiles(unittest.TestCase):

    def setUp(self):
        _credentials._reset_warnings()

    def _warnings(self, fn):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            fn()
        return [str(w.message) for w in caught if issubclass(w.category, UserWarning)]

    def test_save_config_mode(self):
        with scratch_home(umask=0o002) as (home, cwd):
            path = os.path.join(home, ".hscfg")
            hsconfigure.saveConfig("u", "pw", "https://hsds.example.org", None)
            self.assertEqual(file_mode(path), 0o600)
            with open(path) as f:
                self.assertIn("hs_password = pw\n", f.read())

            # an existing readable file is tightened when it's rewritten
            os.chmod(path, 0o644)
            hsconfigure.saveConfig("u", "pw2", "https://hsds.example.org", None)
            self.assertEqual(file_mode(path), 0o600)

    def test_token_cache_mode(self):
        with scratch_home(umask=0o002) as (home, cwd):
            path = os.path.join(home, ".hstokencfg")
            handler = StubOpenID("https://hsds.example.org")
            self.assertEqual(handler.token, "tok")  # acquires and writes the cache
            self.assertEqual(file_mode(path), 0o600)
            with open(path) as f:
                self.assertEqual(json.load(f), {"https://hsds.example.org": {"accessToken": "tok"}})

            os.chmod(path, 0o644)
            handler._token = {"accessToken": "tok2"}
            handler.write_token_cache()
            self.assertEqual(file_mode(path), 0o600)

    def test_readable_config_with_secret_warns_once(self):
        with scratch_home() as (home, cwd):
            path = os.path.join(home, ".hscfg")
            write(path, "hs_username = u\nhs_password = pw\n", 0o644)

            def load_twice():
                Config._cfg.clear()
                Config()
                Config._cfg.clear()
                Config()
                AppConfig()

            msgs = self._warnings(load_twice)
            self.assertEqual(len(msgs), 1, msgs)
            self.assertIn("hs_password", msgs[0])
            self.assertIn("chmod 600", msgs[0])
            self.assertNotIn("pw\n", msgs[0])

    def test_private_or_secretless_config_no_warning(self):
        for text, mode in (("hs_password = pw\n", 0o600),
                           ("hs_username = u\nhs_password = \n", 0o644),
                           ("hs_endpoint = https://hsds.example.org\n", 0o644)):
            _credentials._reset_warnings()
            with scratch_home() as (home, cwd):
                write(os.path.join(home, ".hscfg"), text, mode)
                msgs = self._warnings(lambda: (Config._cfg.clear(), Config(), AppConfig()))
                self.assertEqual(msgs, [], (text, oct(mode)))

    def test_shared_token_cache_warns(self):
        with scratch_home() as (home, cwd):
            path = os.path.join(home, ".hstokencfg")
            write(path, json.dumps({"https://hsds.example.org": {"accessToken": "tok"}}), 0o664)
            msgs = self._warnings(lambda: StubOpenID("https://hsds.example.org"))
            self.assertEqual(len(msgs), 1, msgs)
            self.assertIn("token cache", msgs[0])

            _credentials._reset_warnings()
            os.chmod(path, 0o600)
            self.assertEqual(self._warnings(lambda: StubOpenID("https://hsds.example.org")), [])

    def test_cwd_config_writable_by_others_warns(self):
        with scratch_home() as (home, cwd):
            write(os.path.join(cwd, ".hscfg"), "hs_endpoint = https://hsds.example.org\n", 0o666)
            msgs = self._warnings(lambda: (Config._cfg.clear(), Config()))
            self.assertEqual(len(msgs), 1, msgs)
            self.assertIn("current directory", msgs[0])

            _credentials._reset_warnings()
            os.chmod(os.path.join(cwd, ".hscfg"), 0o644)
            self.assertEqual(self._warnings(lambda: (Config._cfg.clear(), Config())), [])


if __name__ == "__main__":
    unittest.main()
