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
import h5pyd
import numpy as np

cube_side = 256
USER_NAME = "test_user1"
USER_PASSWD = "test"
ENDPOINT = "http://192.168.99.100:5101"

if len(sys.argv) > 1:
    cube_side = int(sys.argv[1])

filename = "cube_" + str(cube_side) + "_" + str(cube_side) + "_" + str(cube_side) + "_gz"
filename += ".h5pyd_test.hdfgroup.org"
#filename += ".h5"
print("filename:", filename)

f = h5pyd.File(filename, "w", username=USER_NAME, password=USER_PASSWD, endpoint=ENDPOINT)
# f = h5pyd.File(filename, "w")

print("create dataset")

dset = f.create_dataset('dset', (cube_side, cube_side, cube_side), compression="gzip", dtype='int8')

print("name:", dset.name)
print("shape:", dset.shape)
print("dset.type:", dset.dtype)
print("dset.maxshape:", dset.maxshape)

print("writing data...")

for i in range(cube_side):
    arr = np.zeros((cube_side, cube_side), dtype=dset.dtype)
    mid = cube_side // 2
    arr[mid,:] = 22
    arr[:,mid] = 44
    dset[i,:,:] = arr
print("done!")

f.close()


