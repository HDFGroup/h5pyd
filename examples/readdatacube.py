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

cube_side = 64
if len(sys.argv) > 1:
    cube_side = int(sys.argv[1])
    
filename = "cube_" + str(cube_side) + "_" + str(cube_side) + "_" + str(cube_side) + ".client_test.hdfgroup.org"
 
f = h5pyd.File(filename, "r", endpoint=endpoint)

print("filename,", f.filename)
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

 
