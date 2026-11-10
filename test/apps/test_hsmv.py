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

APP = os.path.join("..", "..", "h5pyd", "_apps", "hsmv.py")


def run_hsmv(*args):
    """ Run the hsmv app as a subprocess, returning the completed process """
    cmd = [sys.executable, APP] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


class TestHsmv(TestCase):

    def setUp(self):
        if config.get("use_h5py"):
            self.skipTest("hsmv is an HSDS-only command line tool")
        folder = os.environ.get("H5PYD_TEST_FOLDER", "/home/test_user1/h5pyd_test/")
        if not folder.endswith("/"):
            folder += "/"
        self.folder = folder
        self.src = folder + "test_hsmv_src.h5"
        self.des = folder + "test_hsmv_des.h5"
        for domain in (self.src, self.des):
            self._cleanup_domain(domain)
        with h5pyd.File(self.src, "w") as f:
            f.create_group("g1").create_dataset("ds", data=[1, 2, 3])
            f.attrs["a1"] = 42

    def tearDown(self):
        for domain in (self.src, self.des):
            self._cleanup_domain(domain)

    def _cleanup_domain(self, domain):
        parent = os.path.dirname(domain.rstrip("/")) + "/"
        name = os.path.basename(domain.rstrip("/"))
        try:
            folder = h5pyd.Folder(parent, mode="a")
        except IOError:
            return
        try:
            folder.delete_item(name)
        except (IOError, KeyError):
            pass
        finally:
            folder.close()

    def _check_content(self, domain):
        with h5pyd.File(domain, "r") as f:
            self.assertEqual(list(f["g1/ds"][...]), [1, 2, 3])
            self.assertEqual(f.attrs["a1"], 42)

    def test_move(self):
        rsp = run_hsmv(self.src, self.des)
        self.assertEqual(rsp.returncode, 0, rsp.stderr)
        self._check_content(self.des)
        with self.assertRaises(IOError):
            h5pyd.File(self.src, "r")

    def test_move_to_folder(self):
        target_folder = self.folder + "test_hsmv_folder/"
        self._cleanup_domain(target_folder)
        h5pyd.Folder(target_folder, mode="w").close()
        moved = target_folder + os.path.basename(self.src)
        try:
            rsp = run_hsmv(self.src, target_folder)
            self.assertEqual(rsp.returncode, 0, rsp.stderr)
            self._check_content(moved)
        finally:
            self._cleanup_domain(moved)
            self._cleanup_domain(target_folder)

    def test_same_path(self):
        # moving a domain onto itself fails and leaves it intact
        for target in (self.src, self.folder, self.folder + "./" + os.path.basename(self.src)):
            rsp = run_hsmv(self.src, target)
            self.assertNotEqual(rsp.returncode, 0, target)
            self.assertIn("same domain", rsp.stderr)
            self._check_content(self.src)

    def test_folder_source(self):
        rsp = run_hsmv(self.folder, self.des)
        self.assertNotEqual(rsp.returncode, 0)
        self._check_content(self.src)
        with self.assertRaises(IOError):
            h5pyd.File(self.des, "r")


if __name__ == "__main__":
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
