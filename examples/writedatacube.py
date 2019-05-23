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
import time
import logging
import numpy as np

USE_H5PY=0
if USE_H5PY:
    import h5py
else:
    import h5pyd as h5py

loglevel = logging.DEBUG
logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

dims = [16, 16, 16]

if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print("usage: python writedatacube.py <hspath> [h] [w] [d]")
    print("example: python writedatacube.py /home/joebob 16")
    sys.exit(1)

folder = sys.argv[1]

if len(sys.argv) > 2:
    dims[0] = int(sys.argv[2])
    dims[1] = dims[2] = dims[0]

if len(sys.argv) > 3:
    dims[1] = int(sys.argv[3])
    dims[2] = dims[0]

if len(sys.argv) > 4:
    dims[2] = int(sys.argv[4])

filename = os.path.join(folder, "cube")
for s in dims:
    filename += '_' + str(s)
filename += ".h5"
print("filename:", filename)


f = h5py.File(filename, "w")   

print("create dataset")

dset = f.create_dataset('dset', dims,  dtype='int8')

print("name:", dset.name)
print("shape:", dset.shape)
print("chunks:", dset.chunks)
print("dset.type:", dset.dtype)
print("dset.maxshape:", dset.maxshape)
print("bytes per slice: ", dims[0]*dims[1])
if isinstance(f.id.id, str) and f.limits:
    print("max_request_size:", f.limits["max_request_size"])

print("writing data...")

for i in range(dims[2]):
    now = time.time()
    arr = np.zeros((dims[0], dims[1]), dtype=dset.dtype)
    mid_x = dims[0] // 2
    mid_y = dims[1] // 2
    arr[mid_x,:] = 22
    arr[:,mid_y] = 44
    dset[:,:,i] = arr
    elapse = time.time() - now
    print("{:.4f}s".format(elapse))

print("closing file...")
f.close()

print("done!")
