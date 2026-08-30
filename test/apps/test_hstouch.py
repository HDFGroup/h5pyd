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

import sys
import os
import subprocess
import logging
import config
import h5pyd

from common import ut, TestCase

APP = os.path.join("..", "..", "h5pyd", "_apps", "hstouch.py")


def run_hstouch(*args):
    """ Run the hstouch app as a subprocess, returning the completed process """
    cmd = [sys.executable, APP] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


class TestHstouch(TestCase):

    def setUp(self):
        if config.get("use_h5py"):
            self.skipTest("hstouch is an HSDS-only command line tool")
        folder = os.environ.get("H5PYD_TEST_FOLDER", "/home/test_user1/h5pyd_test/")
        if not folder.endswith("/"):
            folder += "/"
        self.folder = folder
        self.domain = folder + "test_hstouch_domain.h5"
        self._cleanup_domain(self.domain)

    def tearDown(self):
        self._cleanup_domain(self.domain)

    def _cleanup_domain(self, domain):
        parent = os.path.dirname(domain.rstrip("/")) + "/"
        name = os.path.basename(domain)
        try:
            folder = h5pyd.Folder(parent, mode="a")
        except IOError:
            return
        try:
            folder.delete_item(name)
        except IOError:
            pass
        finally:
            folder.close()

    def test_help(self):
        result = subprocess.check_output([sys.executable, APP, "-h"])
        self.assertTrue(len(result) > 100)

    def test_create_and_touch_domain(self):
        """ This also covers the FileNotFoundError regression: creating the
        domain requires hstouch to first check whether it already exists
        via `h5py.File(domain, mode="r")`, the exact call path where
        hsds_plugin.py briefly raised a bare, errno-less FileNotFoundError()
        instead of one carrying the real status code/reason. """
        # domain doesn't exist yet - hstouch should create it
        rsp = run_hstouch(self.domain)
        self.assertEqual(rsp.returncode, 0, rsp.stdout + rsp.stderr)

        # confirm it now exists
        f = h5pyd.File(self.domain, mode="r")
        f.close()

        # touch again - domain already exists, hstouch should just update
        # its lastModified timestamp rather than erroring out
        rsp = run_hstouch(self.domain)
        self.assertEqual(rsp.returncode, 0, rsp.stdout + rsp.stderr)

    def test_parent_domain_not_found(self):
        """ hstouch's parent-folder existence check (via Folder, not File)
        should report a missing parent cleanly rather than crashing or
        printing a blank error - this path wasn't affected by the
        FileNotFoundError regression covered by test_create_and_touch_domain
        below (Folder's "not found" handling was never broken), but it's
        worth covering directly since it exercises the same error-message
        contract. """
        bad_domain = self.folder + "no_such_subfolder_xyz/domain.h5"
        rsp = run_hstouch(bad_domain)
        output = rsp.stdout + rsp.stderr
        self.assertNotEqual(rsp.returncode, 0)
        self.assertIn("not found", output)
        self.assertNotIn("Unexpected error", output)
        self.assertNotIn("Traceback", output)


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
