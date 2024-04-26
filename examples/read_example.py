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

import h5pyd as h5py

from pathlib import Path
import subprocess
import sys

DOMAIN_PATH = "/home/test_user1/test/tall.h5"


def load_file():
    """ Load the HDF5 file from the S3 sample bucket to HSDS """

    path = Path(DOMAIN_PATH)
    parent_path = str(path.parent) + "/"  # HSDS folders must end with a '/'
    try:
        h5py.Folder(parent_path, mode="x")  # 'x' mode will create the folder if not present
    except IOError as ioe:
        print("ioe:", ioe)
        sys.exit(1)

    # run hsload
    s3_uri = "s3://hdf5.sample/data/hdf5test/tall.h5"
    run_cmd = ["hsload", s3_uri, parent_path]
    print("running command:", " ".join(run_cmd))
    result = subprocess.run(run_cmd)
    if result.returncode != 0:
        print(f"unable able to hsload {s3_uri}, error: {run_cmd.returncode}")
        sys.exit(1)
    print("hsload complete")
    # now we should be able to open the domain
    f = h5py.File(DOMAIN_PATH)
    return f


def visit_item(name):
    print(f"    {name}")
    return None


def find_g1_2(name):
    print(f"    {name}")
    if name.endswith("g1.2"):
        return True  # stop iteration


def visit_item_obj(name, obj):
    print(f"    {name:20s}  id: {obj.id.id}")
    return None


# print the h5py version
print("h5pyd version:", h5py.version.version)

print("opening domain:", DOMAIN_PATH)

try:
    f = h5py.File(DOMAIN_PATH, "r")
except IOError as ioe:
    if ioe.errno in (404, 410):
        # file hasn't been loaded into HSDS, get it now
        f = load_file()
    else:
        print("unexpected error opening: {DOMAIN_PATH}: {ioe}")
        sys.exit(1)

print("name:", f.name)
print("id:", f.id.id)

g2 = f['g2']

print("g2 uuid:", g2.id.id)
print("g2 name:", g2.name)
print("g2 num elements:", len(g2))
print("g2: iter..")
for x in g2:
    print(x)

print("xyz in g2", ('xyz' in g2))
print("dset2.1 in g2", ('dset2.1' in g2))

dset21 = g2['dset2.1']
print("dset21 uuid:", dset21.id.id)
print("dset21 name:", dset21.name)
print("dset21 dims:", dset21.shape)
print("dset21 type:", dset21.dtype)
arr = dset21[...]
print("dset21 values:", arr)

dset111 = f['/g1/g1.1/dset1.1.1']
print("dset111 uuid:", dset111.id.id)
print("dset111 name:", dset111.name)
print("dset111 dims:", dset111.shape)
print("dset111 type:", dset111.dtype)
print("dset111 len:", len(dset111))
arr = dset111[...]
print("dset111 values:", arr)

attr1 = dset111.attrs['attr1']
print("attr1:", attr1)
print("num attrs of dset1.1.1:", len(dset111.attrs))
print("dset1.1.1 attributes:")

for k in dset111.attrs:
    v = dset111.attrs[k]
    print(f"    {k}: {v}")

print("\nvisit...")
f.visit(visit_item)

print("\nvisititems...")
f.visititems(visit_item_obj)

print("\nsearch g1.2:")
f.visit(find_g1_2)
