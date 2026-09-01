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

APP = os.path.join("..", "..", "h5pyd", "_apps", "hsacl.py")


def run_hsacl(*args):
    """ Run the hsacl app as a subprocess, returning the completed process """
    cmd = [sys.executable, APP] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


class TestHsacl(TestCase):

    def setUp(self):
        if config.get("use_h5py"):
            self.skipTest("hsacl is an HSDS-only command line tool")
        folder = os.environ.get("H5PYD_TEST_FOLDER", "/home/test_user1/h5pyd_test/")
        if not folder.endswith("/"):
            folder += "/"
        self.folder = folder
        self.domain = folder + "test_hsacl_domain.h5"
        self._cleanup_domain(self.domain)
        f = h5pyd.File(self.domain, mode="w")
        f.close()

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

    def test_get_acl_on_folder(self):
        """ baseline - reading ACLs for a folder has always worked """
        rsp = run_hsacl(self.folder)
        self.assertEqual(rsp.returncode, 0, rsp.stdout + rsp.stderr)
        self.assertIn(self.test_user1["name"], rsp.stdout)

    def test_get_acl_on_file_domain(self):
        """ Regression test for a bug where hsacl crashed with:
            AttributeError: 'File' object has no attribute 'getACLs'
        for any non-folder (file) domain, since getACL/getACLs/putACL
        were only ever defined on Folder, not on File. """
        rsp = run_hsacl(self.domain)
        output = rsp.stdout + rsp.stderr
        self.assertEqual(rsp.returncode, 0, output)
        self.assertNotIn("AttributeError", output)
        self.assertIn(self.test_user1["name"], rsp.stdout)

    def test_set_acl_on_file_domain(self):
        """ same underlying regression, exercised via putACL / File.putACL """
        username = self.test_user2["name"]
        rsp = run_hsacl(self.domain, "+cr", username)
        output = rsp.stdout + rsp.stderr
        self.assertEqual(rsp.returncode, 0, output)
        self.assertNotIn("AttributeError", output)

        # confirm the permission was actually set
        rsp = run_hsacl(self.domain, username)
        self.assertEqual(rsp.returncode, 0, rsp.stdout + rsp.stderr)
        self.assertIn(username, rsp.stdout)

    def test_domain_not_found(self):
        """ Regression test: hsacl opens the domain via
        `h5py.File(domain, mode=mode)`, the exact call path where
        hsds_plugin.py briefly raised a bare, errno-less FileNotFoundError()
        - this used to surface as a blank "Unexpected error: " message
        instead of "domain not found". """
        bad_domain = self.folder + "definitely_does_not_exist_xyz.h5"
        rsp = run_hsacl(bad_domain)
        output = rsp.stdout + rsp.stderr
        self.assertNotEqual(rsp.returncode, 0)
        self.assertIn("not found", output)
        self.assertNotIn("Traceback", output)


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
