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
    if sys.argv[1] in ("-h", "--help"):
        print("Usage: python readdatacube [side] [-z]")
        sys.exit(1)
    cube_side = int(sys.argv[1])

filename = "cube_" + str(cube_side) + "_" + str(cube_side) + "_" + str(cube_side)


if len(sys.argv) > 2:
    if sys.argv[2] == "-z":
        filename += "_gz"


filename += ".h5pyd_test.hdfgroup.org"


print("filename:", filename)

f = h5pyd.File(filename, "r", username=USER_NAME, password=USER_PASSWD, endpoint=ENDPOINT)

print("name:", f.name)
print("uuid:", f.id.uuid)

print("get dataset")

dset = f['dset']

print("name:", dset.name)
print("uuid:", dset.id.uuid)
print("shape:", dset.shape)
print("dset.type:", dset.dtype)
print("dset.maxshape:", dset.maxshape)

print("reading data...")

stats = []
for i in range(cube_side):
    arr = dset[i,:,:]
    stats.append( (i, np.min(arr), np.max(arr), np.mean(arr),
                np.median(arr), np.std(arr) ) )

print("done!")

print("slice     min  max       mean     medien    stddev")
print("--------------------------------------------------")
for item in stats:
    print(item)

f.close()


