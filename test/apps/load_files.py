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

s3_http_path = "https://s3.amazonaws.com/hdfgroup/data/hdf5test/"

parent =  h5pyd.Folder(test_folder)
filenames = config.get_test_filenames()

if not os.path.exists(data_dir):
    # make data directory for downloaded HDF5 files
    os.mkdir(data_dir)

for filename in filenames:
    print(filename)
    domain_path = os.path.join(test_folder, filename)
    print(domain_path)
    if filename in parent:
        print("found")
        continue
    # check to see if the file has already been downloaded
    hdf5_path = os.path.join(data_dir, filename)
    if not os.path.isfile(hdf5_path):
        # wget from S3
        s3path = s3_http_path + filename
        print("downloading", s3path)
        rc = os.system("wget -q https://s3.amazonaws.com/hdfgroup/data/hdf5test/{} -P {}".format(filename, data_dir))
        if rc != 0:
            sys.exit("Failed to retreive test data file")
    # run hsload for each file
    print("running hsload for {}".format(hdf5_path))
    rc = os.system("python ../../h5pyd/_apps/hsload.py {} {}".format(hdf5_path, test_folder))
    if rc != 0:
        sys.exit("Failed to load {}".format(filename))
print("load_files done")
    