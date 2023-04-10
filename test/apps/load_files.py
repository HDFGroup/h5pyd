#!/usr/bin/env python
##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including       s   #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

import os
import sys
import config
import h5pyd

#
# Main
#

print("load_file.py")


if "H5PYD_TEST_FOLDER" not in os.environ:
    print("set H5PYD_TEST_FOLDER environment not set")
    sys.exit(1)

test_folder = os.environ["H5PYD_TEST_FOLDER"]

if test_folder[-1] != "/":
    test_folder += "/"

data_dir = "data"
out_dir = "out"
test_file_http_path = config.get("test_file_http_path")

parent =  h5pyd.Folder(test_folder)
filenames = config.get_test_filenames()

if not os.path.exists(data_dir):
    # make data directory for test HDF5 files
    os.mkdir(data_dir)

if not os.path.exists(out_dir):
    # make data directory for files downloaded from server
    os.mkdir(out_dir)

for filename in filenames:
    domain_path = os.path.join(test_folder, filename)
    hdf5_path = os.path.join(data_dir, filename)
    if filename not in data_dir:
        # check to see if the file has already been downloaded
        if not os.path.isfile(hdf5_path):
            # wget from S3
            http_path = test_file_http_path + filename
            print("downloading:", http_path)
            rc = os.system(f"wget -q https://s3.amazonaws.com/hdfgroup/data/hdf5test/{filename} -P {data_dir}")
            if rc != 0:
                sys.exit("Failed to retreive test data file")
    # run hsload for each file
    print(f"running hsload for {hdf5_path} to {test_folder}")
    rc = os.system(f"python ../../h5pyd/_apps/hsload.py {hdf5_path} {test_folder}")
    if rc != 0:
        sys.exit(f"Failed to hsload {filename}")
    print(f"running hsget for {test_folder}{filename} to {out_dir}")
    rc = os.system(f"python ../../h5pyd/_apps/hsget.py {test_folder}{filename} {out_dir}/{filename}")
    if rc != 0:
        sys.exit(f"Failed to hsget {filename}")
print("load_files done")
    