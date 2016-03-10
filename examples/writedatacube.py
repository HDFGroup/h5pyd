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

endpoint="http://127.0.0.1:5000"

cube_side = 100
filename = "cube_" + str(cube_side) + "_" + str(cube_side) + "_" + str(cube_side) + ".client_test.hdfgroup.org"
 
f = h5pyd.File(filename, "w", endpoint=endpoint)

print("filename,", f.filename)
print("name:", f.name)
print("uuid:", f.id.uuid)
 
print("create dataset")
 
dset = f.create_dataset('dset', (cube_side, cube_side, cube_side), dtype='f4')

print("name:", dset.name)
print("uuid:", dset.id.uuid)
print("shape:", dset.shape)
print("dset.type:", dset.dtype)
print("dset.maxshape:", dset.maxshape)

print("writing data...")

for i in range(cube_side):
    arr = np.random.rand(cube_side, cube_side)
    dset[i,:,:] = arr
print("done!")

f.close() 

 
