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
 
import json
from .httpconn import HttpConn
from .config import Config
  
def getServerInfo( endpoint=None, username=None, password=None, **kwds):
     
    cfg = None
    if endpoint is None or username is None or password is None:
        # unless we'r given all the connect info, create a config object that
        # pulls in state from a .hscfg file (if found).
        cfg = Config()
 
    if endpoint is None and "hs_endpoint" in cfg:
        endpoint = cfg["hs_endpoint"]

    if username is None and "hs_username" in cfg:
        username = cfg["hs_username"]
              
    if password is None and "hs_password" in cfg:
        password = cfg["hs_password"]

    # http_conn without a domain
    http_conn = HttpConn(None, endpoint=endpoint, username=username, password=password)
          
    rsp = http_conn.GET("/about")
    if rsp.status_code == 400:
        # h5serv uses info for status
        rsp = http_conn.GET("/info")
        
    if rsp.status_code != 200:
        raise IOError(rsp.status_code, rsp.reason)
        
    rspJson = json.loads(rsp.text)
    
    # mix in client connect info
    rspJson["endpoint"] = endpoint
    rspJson["username"] = username
    if not password:
        rspJson["password"] = ''
    else:
        rspJson["password"] = '*'*len(password)

    http_conn.close()
    http_conn = None

    return rspJson

         

      
