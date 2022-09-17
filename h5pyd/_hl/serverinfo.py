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

from __future__ import absolute_import

import time
from .httpconn import HttpConn
from .config import Config


def getServerInfo(endpoint=None, username=None, password=None, api_key=None, **kwds):

    cfg = Config()  # get credentials from .hscfg file (if found)

    if endpoint is None and "hs_endpoint" in cfg:
        endpoint = cfg["hs_endpoint"]

    if username is None and "hs_username" in cfg:
        username = cfg["hs_username"]

    if password is None and "hs_password" in cfg:
        password = cfg["hs_password"]

    if api_key is None and "hs_api_key" in cfg:
        api_key = cfg["hs_api_key"]

    # http_conn without a domain
    http_conn = HttpConn(
        None, endpoint=endpoint, username=username, password=password, api_key=api_key
    )

    # need some special logic for the first request in local mode
    # to give the sockets time to initialize
    if endpoint.startswith("local"):
        connect_backoff = [0.5, 1, 2, 4, 8, 16]
    else:
        connect_backoff = []

    connect_try = 0

    while True:
        try:
            rsp = http_conn.GET("/about")
            break
        except IOError as ioe:
            if connect_try < len(connect_backoff):
                time.sleep(connect_backoff[connect_try])
            else:
                raise
            connect_try += 1

    if rsp.status_code == 400:
        # h5serv uses info for status
        rsp = http_conn.GET("/info")

    if rsp.status_code != 200:
        raise IOError(rsp.status_code, rsp.reason)

    rspJson = rsp.json()

    # mix in client connect info
    rspJson["endpoint"] = endpoint
    if "username" in rspJson:
        rspJson["username"] = rspJson["username"]
    else:
        rspJson["username"] = username
    if not password:
        rspJson["password"] = ""
    else:
        rspJson["password"] = "*" * len(password)

    http_conn.close()
    http_conn = None

    return rspJson
