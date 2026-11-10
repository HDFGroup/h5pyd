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
""" What hsls, hsget and hsload print about names and files that come from a domain or an
input file """

import logging
import os
import shutil
import subprocess
import sys
import tempfile

import h5py
import numpy as np

import config
import h5pyd

from common import ut, TestCase

APPS = os.path.join("..", "..", "h5pyd", "_apps")

ESCAPE_NAME = "\x1b]52;c;SGk=\x07"  # an OSC 52 sequence: sets the terminal's clipboard


def run_app(app, *args):
    cmd = [sys.executable, os.path.join(APPS, app + ".py")] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


class TestCliOutput(TestCase):

    def setUp(self):
        if config.get("use_h5py"):
            self.skipTest("command line tools are HSDS-only")
        folder = os.environ.get("H5PYD_TEST_FOLDER", "/home/test_user1/h5pyd_test/")
        if not folder.endswith("/"):
            folder += "/"
        self.domain = folder + "test_cli_output.h5"
        self.tmpdir = tempfile.mkdtemp(prefix="h5pyd_cli_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_hsls_escapes_names(self):
        with h5pyd.File(self.domain, "w") as f:
            g = f.create_group("g" + ESCAPE_NAME)
            g.attrs[ESCAPE_NAME] = "v\x1b[2J"
            g.attrs["history"] = "created 2020\nmodified 2021"
        rsp = run_app("hsls", "-r", "--showattrs", self.domain)
        self.assertEqual(rsp.returncode, 0, rsp.stderr)
        self.assertNotIn("\x1b", rsp.stdout)
        self.assertNotIn("\x07", rsp.stdout)
        self.assertIn("g\\x1b]52;c;SGk=\\x07", rsp.stdout)
        self.assertIn("v\\x1b[2J", rsp.stdout)
        # a multi-line value still prints over several lines
        self.assertIn("created 2020\nmodified 2021", rsp.stdout)

    def test_hsget_warns_on_external_link_filenames(self):
        with h5pyd.File(self.domain, "w") as f:
            f["abs"] = h5pyd.ExternalLink("/etc/target.h5", "/x")
            f["near"] = h5pyd.ExternalLink("target.h5", "/x")
        output = os.path.join(self.tmpdir, "out.h5")
        rsp = run_app("hsget", self.domain, output)
        self.assertEqual(rsp.returncode, 0, rsp.stderr)
        warnings = [line for line in rsp.stderr.splitlines() if "external link to" in line]
        self.assertEqual(len(warnings), 1, rsp.stderr)
        self.assertIn("'/etc/target.h5'", warnings[0])
        with h5py.File(output, "r") as f:
            self.assertEqual(f.get("abs", getlink=True).filename, "/etc/target.h5")

    def test_hsload_external_sources(self):
        secret = os.path.join(self.tmpdir, "secret.bin")
        with open(secret, "wb") as f:
            f.write(bytes(range(16)))
        input_file = os.path.join(self.tmpdir, "input.h5")
        with h5py.File(input_file, "w") as f:
            f.create_dataset("ext", shape=(16,), dtype="u1", external=[(secret, 0, 16)])

        rsp = run_app("hsload", input_file, self.domain)
        self.assertEqual(rsp.returncode, 0, rsp.stderr)
        self.assertIn(repr(secret), rsp.stderr)
        with h5pyd.File(self.domain, "r") as f:
            np.testing.assert_array_equal(f["ext"][...], np.arange(16))

        rsp = run_app("hsload", "--no-external-sources", input_file, self.domain)
        self.assertEqual(rsp.returncode, 0, rsp.stderr)
        self.assertIn("values not copied", rsp.stderr)
        with h5pyd.File(self.domain, "r") as f:
            np.testing.assert_array_equal(f["ext"][...], np.zeros(16))


if __name__ == "__main__":
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
