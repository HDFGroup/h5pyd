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
import config
import h5pyd

if config.get("use_h5py"):
    sys.exit("use_h5py")

if "H5PYD_TEST_FOLDER" not in os.environ:
    sys.exit("set H5PYD_TEST_FOLDER environment not set")
folder_path = os.environ["H5PYD_TEST_FOLDER"]
if len(folder_path) <= 1:
    # should be more than just /...
    sys.exit("invalid path")
if not folder_path.startswith("/"):
    # HSDS expects folder paths to start with a slash (as opposed to DNS format)
    sys.exit("not HSDS path")
if folder_path[-1] != "/":
    folder_path += "/"
try:
    h5pyd.Folder(folder_path)  # will trigger error with h5serv
except Exception:
    sys.exit("Server doesn't support Folder objects")
    




