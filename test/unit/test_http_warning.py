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
""" Warnings about where credentials are sent: plain http, and an endpoint other than the one
they were configured for """

import os
import socket
import time
import unittest
import warnings
from unittest import mock

import requests

from stubs import scratch_home

from h5pyd import _credentials
from h5pyd.httpconn import HttpConn
from h5pyd.openid import KeycloakOpenID


def caught_warnings(fn):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        fn()
    return [str(w.message) for w in caught if issubclass(w.category, UserWarning)]


class TestHttpWarning(unittest.TestCase):

    def setUp(self):
        _credentials._reset_warnings()

    def _conn(self, endpoint, **kwargs):
        return lambda: HttpConn("/home/u/x.h5", endpoint=endpoint, **kwargs)

    def test_remote_http_with_password_warns_once(self):
        with scratch_home():
            make = self._conn("http://hsds.example.org", username="u", password="pw")
            msgs = caught_warnings(lambda: (make(), make()))
            self.assertEqual(len(msgs), 1, msgs)
            self.assertIn("unencrypted http", msgs[0])
            self.assertNotIn("pw", msgs[0].replace("http", ""))

    def test_remote_http_with_api_key_warns(self):
        with scratch_home():
            msgs = caught_warnings(self._conn("http://hsds.example.org:5101", api_key="KEY"))
            self.assertEqual(len(msgs), 1, msgs)

    def test_no_warning(self):
        cases = ("http://127.0.0.1:5101", "http://localhost:5101", "http://[::1]:5101",
                 "http+unix://%2Ftmp%2Fhs%2Fsn_1.sock", "https://hsds.example.org")
        with scratch_home():
            for endpoint in cases:
                msgs = caught_warnings(self._conn(endpoint, username="u", password="pw"))
                self.assertEqual(msgs, [], endpoint)
            # no credentials, nothing to warn about
            msgs = caught_warnings(self._conn("http://hsds.example.org"))
            self.assertEqual(msgs, [])

    def test_off_switch(self):
        with scratch_home():
            os.environ["H5PYD_ALLOW_HTTP_AUTH"] = "1"
            msgs = caught_warnings(self._conn("http://hsds.ns.svc.cluster.local", username="u", password="pw"))
            self.assertEqual(msgs, [])

    def test_keycloak_plain_http_warns(self):
        config = {"keycloak_uri": "http://kc.example.org", "keycloak_realm": "r", "keycloak_client_id": "c"}
        rsp = mock.Mock(status_code=200)
        rsp.json.return_value = {"token_type": "Bearer", "access_token": "tok"}
        with scratch_home():
            handler = KeycloakOpenID("https://hsds.example.org", config=config, username="u", password="pw")
            with mock.patch("h5pyd.openid.requests.post", return_value=rsp) as post:
                msgs = caught_warnings(handler.acquire)
            self.assertEqual(len(msgs), 1, msgs)
            self.assertIn("Keycloak password", msgs[0])
            self.assertIn("timeout", post.call_args.kwargs)

    def test_keycloak_timeout(self):
        # a server that accepts the connection and never answers
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        config = {"keycloak_uri": f"http://127.0.0.1:{port}", "keycloak_realm": "r", "keycloak_client_id": "c"}
        try:
            with scratch_home():
                handler = KeycloakOpenID("https://hsds.example.org", config=config, username="u",
                                         password="pw", timeout=0.5)
                start = time.time()
                with self.assertRaises(requests.exceptions.Timeout):
                    handler.acquire()
                self.assertLess(time.time() - start, 10)
        finally:
            server.close()


class TestEndpointMismatch(unittest.TestCase):

    def setUp(self):
        _credentials._reset_warnings()

    def _with_config(self, fn):
        with scratch_home() as (home, cwd):
            path = os.path.join(home, ".hscfg")
            with open(path, "w") as f:
                f.write("hs_endpoint = https://a.example.org\nhs_username = u\nhs_password = pw\n")
            os.chmod(path, 0o600)
            return caught_warnings(fn)

    def test_config_credentials_to_other_endpoint_warn(self):
        msgs = self._with_config(lambda: HttpConn("/home/u/x.h5", endpoint="https://b.example.org"))
        self.assertEqual(len(msgs), 1, msgs)
        self.assertIn("https://a.example.org", msgs[0])
        self.assertIn("https://b.example.org", msgs[0])

    def test_same_endpoint_no_warning(self):
        for endpoint in ("https://a.example.org", "https://A.example.org:443/", None):
            _credentials._reset_warnings()
            msgs = self._with_config(lambda: HttpConn("/home/u/x.h5", endpoint=endpoint))
            self.assertEqual(msgs, [], endpoint)

    def test_explicit_credentials_no_warning(self):
        msgs = self._with_config(lambda: HttpConn("/home/u/x.h5", endpoint="https://b.example.org",
                                                  username="v", password="other"))
        self.assertEqual(msgs, [])


if __name__ == "__main__":
    unittest.main()
