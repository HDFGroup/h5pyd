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
"""
Helpers for the offline tests in test/unit.  Nothing here needs a running HSDS.

- FakeHttpConn: a stand-in for h5pyd.httpconn.HttpConn that records each request and
  answers from a handler function.
- scratch_home(): a context manager that points HOME (and USERPROFILE) at a new temporary
  directory and makes another temporary directory the current directory.
- StubServer: a local HTTP server on 127.0.0.1 that records requests and answers from a
  handler function, for code that builds its own HttpConn.
"""

import contextlib
import json
import os
import shutil
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qsl


class FakeResponse:
    """ The parts of h5pyd.httpconn.HttpResponse that callers read """

    def __init__(self, status_code=200, body=None, reason=None):
        self.status_code = status_code
        self.reason = reason or ("OK" if status_code < 400 else "Error")
        self._body = body

    def json(self):
        if isinstance(self._body, (bytes, str)):
            return json.loads(self._body)
        return self._body

    @property
    def text(self):
        if self._body is None:
            raise IOError("no data returned")
        if isinstance(self._body, bytes):
            return self._body
        return json.dumps(self._body).encode("utf-8")

    @property
    def content(self):
        return self.text


class FakeHttpConn:
    """ Records every request and answers through handler(method, req, params, body).

    The handler returns a FakeResponse, or a (status_code, body) tuple.  The default handler
    answers 200 with an empty JSON object.  Starts closed, like HttpConn, so that
    HsdsPlugin.open() goes through its normal path. """

    def __init__(self, handler=None, server_info=None):
        self.handler = handler
        self.requests = []  # list of (method, req, params, body)
        self._closed = True
        self._server_info = server_info or {"state": "READY", "hsds_version": "1.0.3"}

    def open(self):
        self._closed = False

    def close(self):
        self._closed = True

    def isClosed(self):
        return self._closed

    def serverInfo(self):
        return dict(self._server_info)

    def _request(self, method, req, params=None, body=None, **kwargs):
        params = dict(params) if params else {}
        self.requests.append((method, req, params, body))
        if self.handler is None:
            return FakeResponse(200, {})
        rsp = self.handler(method, req, params, body)
        if isinstance(rsp, tuple):
            rsp = FakeResponse(*rsp)
        return rsp

    def GET(self, req, params=None, **kwargs):
        return self._request("GET", req, params=params, **kwargs)

    def PUT(self, req, body=None, params=None, **kwargs):
        return self._request("PUT", req, params=params, body=body, **kwargs)

    def POST(self, req, body=None, params=None, **kwargs):
        return self._request("POST", req, params=params, body=body, **kwargs)

    def DELETE(self, req, params=None, **kwargs):
        return self._request("DELETE", req, params=params, **kwargs)

    def requests_for(self, method):
        return [r for r in self.requests if r[0] == method]


def _is_h5pyd_env_var(name):
    return name.startswith("HS_") or name.startswith("H5PYD_")


@contextlib.contextmanager
def scratch_home(umask=None):
    """ Point HOME at a new temporary directory and chdir into another one.

    Also removes HS_* and H5PYD_* environment variables and clears h5pyd's cached config, so
    the developer's own ~/.hscfg and environment can't leak into a test.  Yields
    (home_dir, cwd_dir).  Restores the environment, the cached config, the current directory
    and the umask on exit, and removes both directories. """

    from h5pyd.config import Config

    saved_env = dict(os.environ)
    saved_cfg = dict(Config._cfg)
    saved_cwd = os.getcwd()
    home_dir = tempfile.mkdtemp(prefix="h5pyd_unit_home_")
    cwd_dir = tempfile.mkdtemp(prefix="h5pyd_unit_cwd_")
    saved_umask = os.umask(umask) if umask is not None else None
    try:
        for k in list(os.environ):
            if _is_h5pyd_env_var(k):
                del os.environ[k]
        os.environ["HOME"] = home_dir
        os.environ["USERPROFILE"] = home_dir
        Config._cfg.clear()
        os.chdir(cwd_dir)
        yield home_dir, cwd_dir
    finally:
        os.chdir(saved_cwd)
        if saved_umask is not None:
            os.umask(saved_umask)
        os.environ.clear()
        os.environ.update(saved_env)
        Config._cfg.clear()
        Config._cfg.update(saved_cfg)
        shutil.rmtree(home_dir, ignore_errors=True)
        shutil.rmtree(cwd_dir, ignore_errors=True)


class StubServer:
    """ A local HTTP server for tests.  Use as a context manager:

        with StubServer(handler) as srv:
            endpoint = srv.endpoint   # http://127.0.0.1:<port>

    handler(method, path, params, body) returns (status_code, body), where body is a dict
    (sent as JSON), bytes or None.  Every request is recorded in srv.requests as
    (method, path, params, body). """

    def __init__(self, handler):
        self.handler = handler
        self.requests = []
        stub = self

        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                parts = urlsplit(self.path)
                params = dict(parse_qsl(parts.query, keep_blank_values=True))
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                stub.requests.append((self.command, parts.path, params, body))
                status, rsp_body = stub.handler(self.command, parts.path, params, body)
                if isinstance(rsp_body, (dict, list)):
                    data = json.dumps(rsp_body).encode("utf-8")
                    ctype = "application/json"
                else:
                    data = rsp_body or b""
                    ctype = "application/octet-stream"
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_PUT = do_POST = do_DELETE = _handle

            def log_message(self, *args):
                pass  # keep test output quiet

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()
