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
""" Command line tools: escaping server strings, reporting files that hsload reads on behalf
of an input file, external link filenames written by hsget, and S3 over https """

import contextlib
import io
import os
import shutil
import tempfile
import unittest
from unittest import mock

import h5py
import numpy as np

from stubs import scratch_home

from h5pyd._apps.config import safe_str
from h5pyd._apps import utillib


class TestSafeStr(unittest.TestCase):

    def test_control_characters_escaped(self):
        self.assertEqual(safe_str("\x1b]52;c;SGk=\x07"), "\\x1b]52;c;SGk=\\x07")
        self.assertEqual(safe_str("a\tb\nc\rd"), "a\\x09b\\x0ac\\x0dd")
        self.assertEqual(safe_str("\x7f\x9b[2J"), "\\x7f\\x9b[2J")
        self.assertEqual(safe_str("\x00"), "\\x00")

    def test_multiline_keeps_tab_and_newline(self):
        self.assertEqual(safe_str("a\tb\nc\x1b[2J\r", multiline=True), "a\tb\nc\\x1b[2J\\x0d")

    def test_printable_unchanged(self):
        for s in ("tall.h5", "/home/test_user1/x y.h5", "données é ü 数据", "a\\x1b"):
            self.assertEqual(safe_str(s), s)
        self.assertEqual(safe_str(42), "42")
        self.assertEqual(safe_str(None), "None")


class TestExternalSources(unittest.TestCase):
    """ hsload reports the other files an input file's datasets read, and can skip them """

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="h5pyd_unit_ext_")
        self.secret = os.path.join(self.dir, "secret.bin")
        with open(self.secret, "wb") as f:
            f.write(bytes(range(16)))
        self.vds_src = os.path.join(self.dir, "vds_src.h5")
        with h5py.File(self.vds_src, "w") as f:
            f.create_dataset("data", data=np.arange(16, dtype="u1"))
        self.input = os.path.join(self.dir, "input.h5")
        with h5py.File(self.input, "w") as f:
            f.create_dataset("ext", shape=(16,), dtype="u1", external=[(self.secret, 0, 16)])
            layout = h5py.VirtualLayout(shape=(16,), dtype="u1")
            layout[:] = h5py.VirtualSource(self.vds_src, "data", shape=(16,))
            f.create_virtual_dataset("vds", layout)
            f.create_dataset("plain", data=np.ones(16, dtype="u1"))
            # virtual datasets whose source is in this file, but which still read other files:
            # one over an external-storage dataset, one through an external link
            layout = h5py.VirtualLayout(shape=(16,), dtype="u1")
            layout[:] = h5py.VirtualSource(".", "/ext", shape=(16,))
            f.create_virtual_dataset("vds_self", layout)
            f["elink"] = h5py.ExternalLink(self.vds_src, "/")
            layout = h5py.VirtualLayout(shape=(16,), dtype="u1")
            layout[:] = h5py.VirtualSource(".", "/elink/data", shape=(16,))
            f.create_virtual_dataset("vds_elink", layout)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    NAMES = ("ext", "vds", "plain", "vds_self", "vds_elink")

    def _load(self, mode="w", **kwargs):
        output = os.path.join(self.dir, "output.h5")
        fin = h5py.File(self.input, "r")
        fout = h5py.File(output, mode)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            utillib.load_file(fin, fout, **kwargs)
        with h5py.File(output, "r") as f:
            values = {name: f[name][...] for name in self.NAMES}
        return stderr.getvalue(), values

    def test_sources_reported(self):
        stderr, values = self._load()
        self.assertIn(repr(self.secret), stderr)
        self.assertIn(repr(self.vds_src), stderr)
        self.assertNotIn("'/plain'", stderr)
        # values are still copied by default
        np.testing.assert_array_equal(values["ext"], np.arange(16))
        np.testing.assert_array_equal(values["vds"], np.arange(16))

    def test_no_external_sources(self):
        stderr, values = self._load(no_external_sources=True)
        self.assertIn(repr(self.secret), stderr)
        self.assertIn("values not copied", stderr)
        for name in ("ext", "vds", "vds_self", "vds_elink"):
            self.assertIn(f"dataset '/{name}': values not copied", stderr)
            np.testing.assert_array_equal(values[name], np.zeros(16), name)
        np.testing.assert_array_equal(values["plain"], np.ones(16))

    def test_same_file_virtual_sources_reported(self):
        stderr, values = self._load()
        self.assertIn("dataset '/vds_self': values are read from virtual source in this file, dataset '/ext'", stderr)
        self.assertIn("dataset '/vds_elink': values are read from virtual source in this file", stderr)

    def test_append_keeps_skipped_datasets_in_step(self):
        # appending with --no-external-sources still grows the skipped datasets.  (This input
        # has no external link: appending re-creates links, which fails for one that exists.)
        input_file = os.path.join(self.dir, "append_input.h5")
        with h5py.File(input_file, "w") as f:
            f.create_dataset("ext", shape=(16,), dtype="u1", external=[(self.secret, 0, 16)])
            layout = h5py.VirtualLayout(shape=(16,), dtype="u1")
            layout[:] = h5py.VirtualSource(".", "/ext", shape=(16,))
            f.create_virtual_dataset("vds_self", layout)
            f.create_dataset("plain", data=np.ones(16, dtype="u1"))
        output = os.path.join(self.dir, "append_output.h5")
        for i in range(3):
            fin = h5py.File(input_file, "r")
            fout = h5py.File(output, "a" if i else "w")
            with contextlib.redirect_stderr(io.StringIO()):
                utillib.load_file(fin, fout, append=(i > 0), no_external_sources=True)
        with h5py.File(output, "r") as f:
            shapes = {name: f[name].shape for name in ("ext", "vds_self", "plain")}
            np.testing.assert_array_equal(f["ext"][...], np.zeros((3, 16)))
            np.testing.assert_array_equal(f["plain"][...], np.ones((3, 16)))
        self.assertEqual(set(shapes.values()), {(3, 16)}, shapes)


class TestExternalLinkWarning(unittest.TestCase):
    """ external links copied into a local file warn when they leave its directory """

    def test_warning(self):
        tmpdir = tempfile.mkdtemp(prefix="h5pyd_unit_links_")
        try:
            src = h5py.File(os.path.join(tmpdir, "src.h5"), "w")
            src["abs"] = h5py.ExternalLink("/etc/target.h5", "/x")
            src["up"] = h5py.ExternalLink("../../target.h5", "/x")
            src["near"] = h5py.ExternalLink("sub/target.h5", "/x")
            src["win_abs"] = h5py.ExternalLink("\\etc\\target.h5", "/x")
            src["win_drive"] = h5py.ExternalLink("C:target.h5", "/x")
            des = h5py.File(os.path.join(tmpdir, "des.h5"), "w")
            ctx = {"srcid_desobj_map": {}, "verbose": False}
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                utillib.create_links(src, des, ctx)
            self.assertEqual(des.get("abs", getlink=True).filename, "/etc/target.h5")  # still copied
            src.close()
            des.close()
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        warned = stderr.getvalue()
        self.assertEqual(len(warned.splitlines()), 4, warned)
        for target in ("'/etc/target.h5'", "'../../target.h5'", "etc\\\\target.h5", "'C:target.h5'"):
            self.assertIn(target, warned)
        self.assertNotIn("sub/target.h5", warned)

    def test_leaves_directory(self):
        outside = ("/etc/x.h5", "\\etc\\x.h5", "C:/x.h5", "C:x.h5", "\\\\srv\\share\\x.h5",
                   "../x.h5", "a/../../x.h5", "a\\..\\x.h5")
        inside = ("x.h5", "sub/x.h5", "sub\\x.h5", "a..b.h5", "hdf5://home/x.h5")
        for name in outside:
            self.assertTrue(utillib.leaves_directory(name), name)
        for name in inside:
            self.assertFalse(utillib.leaves_directory(name), name)


class TestS3Https(unittest.TestCase):

    def test_s3_filesystem_kwargs(self):
        try:
            from h5pyd._apps import hsload
        except SystemExit:
            self.skipTest("hsload dependencies not installed")
        if not hsload.S3FS_IMPORT:
            self.skipTest("s3fs not installed")
        with scratch_home():
            for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_GATEWAY"):
                os.environ.pop(k, None)
            with mock.patch.object(hsload.s3fs, "S3FileSystem") as s3:
                hsload.get_s3_filesystem()
                self.assertEqual(s3.call_args.kwargs, {"anon": True})

                os.environ["AWS_GATEWAY"] = "http://minio.local:9000"
                hsload.get_s3_filesystem()
                kwargs = s3.call_args.kwargs
                self.assertNotIn("use_ssl", kwargs)
                self.assertEqual(kwargs["client_kwargs"], {"endpoint_url": "http://minio.local:9000"})


if __name__ == "__main__":
    unittest.main()
