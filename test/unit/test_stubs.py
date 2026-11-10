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
""" Checks that the offline test helpers in stubs.py work with h5pyd itself """

import os
import unittest

from stubs import FakeHttpConn, StubServer, scratch_home

from h5pyd.config import Config
from h5pyd.httpconn import HttpConn


class TestStubs(unittest.TestCase):

    def test_scratch_home_isolates_config(self):
        os.environ["HS_ENDPOINT"] = "http://outside.example"
        try:
            with scratch_home() as (home_dir, cwd_dir):
                self.assertEqual(os.path.expanduser("~"), home_dir)
                self.assertEqual(os.path.realpath(os.getcwd()), os.path.realpath(cwd_dir))
                self.assertNotIn("HS_ENDPOINT", os.environ)
                cfg = Config()
                self.assertEqual(cfg["hs_endpoint"], "")
            self.assertEqual(os.environ["HS_ENDPOINT"], "http://outside.example")
            self.assertFalse(os.path.exists(home_dir))
        finally:
            del os.environ["HS_ENDPOINT"]

    def test_stub_server_with_httpconn(self):
        def handler(method, path, params, body):
            if path == "/about":
                return 200, {"state": "READY", "hsds_version": "1.0.3"}
            return 404, None

        with scratch_home():
            with StubServer(handler) as srv:
                conn = HttpConn("/home/test/x.h5", endpoint=srv.endpoint, retries=0)
                conn.open()
                rsp = conn.GET("/about")
                self.assertEqual(rsp.status_code, 200)
                self.assertEqual(rsp.json()["state"], "READY")
                conn.close()
        self.assertEqual(srv.requests[-1][0:2], ("GET", "/about"))
        self.assertEqual(srv.requests[-1][2].get("domain"), "/home/test/x.h5")

    def test_fake_http_conn_records(self):
        conn = FakeHttpConn(lambda method, req, params, body: (201, {"ok": 1}))
        self.assertTrue(conn.isClosed())
        conn.open()
        rsp = conn.PUT("/groups/g-1/links", body={"a": 1}, params={"x": "y"})
        self.assertEqual(rsp.status_code, 201)
        self.assertEqual(conn.requests, [("PUT", "/groups/g-1/links", {"x": "y"}, {"a": 1})])


if __name__ == "__main__":
    unittest.main()
