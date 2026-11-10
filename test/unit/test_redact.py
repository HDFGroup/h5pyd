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
""" API keys and URL userinfo are kept out of logs and exception messages, and an OpenID
handler isn't sent as an api_key parameter """

import logging
import socket
import unittest

from stubs import StubServer, scratch_home

from h5pyd._credentials import redact
from h5pyd.httpconn import HttpConn
from h5pyd.openid import OpenIDHandler

SECRET = "SECRETKEY123"


def closed_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class StubOpenID(OpenIDHandler):
    def __init__(self, endpoint):
        super().__init__(endpoint, use_token_cache=False)
        self._token = {"accessToken": "tok"}

    def acquire(self):
        pass

    def refresh(self):
        pass


class TestRedact(unittest.TestCase):

    def test_redact(self):
        self.assertEqual(redact("GET http://h/x?api_key=abc&domain=/d"), "GET http://h/x?api_key=***&domain=/d")
        self.assertEqual(redact("http://user:pw@host:5101/about"), "http://***@host:5101/about")
        self.assertEqual(redact("https://user@host/about"), "https://***@host/about")
        self.assertEqual(redact("user@domain.org owns /home/x"), "user@domain.org owns /home/x")
        self.assertEqual(redact({"api_key": "x"}), "{'api_key': 'x'}")  # only query strings are matched

    def _failing_requests(self, conn):
        """ run each request against a closed port, returning the exception messages """
        conn.open()
        messages = []
        for call in (lambda: conn.GET("/about"),
                     lambda: conn.PUT("/", body={}),
                     lambda: conn.POST("/groups", body={}),
                     lambda: conn.DELETE("/")):
            with self.assertRaises(IOError) as cm:
                call()
            messages.append(str(cm.exception))
        conn.close()
        return messages

    def test_api_key_not_logged(self):
        endpoint = f"http://127.0.0.1:{closed_port()}"
        with scratch_home():
            conn = HttpConn("/home/u/x.h5", endpoint=endpoint, api_key=SECRET, retries=0, timeout=2)
            with self.assertLogs("h5pyd", level="DEBUG") as logs:
                messages = self._failing_requests(conn)
        text = "\n".join(logs.output + messages)
        self.assertIn("api_key=***", text)  # the request URL was logged, redacted
        self.assertNotIn(SECRET, text)

    def test_userinfo_not_logged(self):
        endpoint = f"http://user:hunter2@127.0.0.1:{closed_port()}"
        with scratch_home():
            conn = HttpConn("/home/u/x.h5", endpoint=endpoint, retries=0, timeout=2)
            with self.assertLogs("h5pyd", level="DEBUG") as logs:
                messages = self._failing_requests(conn)
        text = "\n".join(logs.output + messages)
        self.assertIn("***@127.0.0.1", text)
        self.assertNotIn("hunter2", text)

    def test_api_key_parameter(self):
        def handler(method, path, params, body):
            return 200, {}

        with scratch_home():
            with StubServer(handler) as srv:
                # a string key is sent as before
                conn = HttpConn("/home/u/x.h5", endpoint=srv.endpoint, api_key=SECRET, retries=0)
                conn.open()
                conn.GET("/about")
                conn.PUT("/", body={})
                conn.close()
                # an OpenID handler isn't
                conn = HttpConn("/home/u/x.h5", endpoint=srv.endpoint, retries=0)
                conn._api_key = StubOpenID(srv.endpoint)
                conn.open()
                conn.GET("/about")
                conn.PUT("/", body={})
                conn.POST("/groups", body={})
                conn.DELETE("/")
                conn.close()
        sent = [r[2].get("api_key") for r in srv.requests]
        self.assertEqual(sent, [SECRET, SECRET, None, None, None, None])


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    unittest.main()
