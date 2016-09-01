import h5pyd as h5py
import numpy as np
import sys

#
# Print objects in a domain in the style of the h5ls utilitiy
#
recursive = False
verbose = False
endpoint = "http://127.0.0.1:5000"
username = None
password = None

def getShapeText(dset):
    shape_text = "Scalar"
    shape = dset.shape
    if shape is not None:
        shape_text = "{"
        rank = len(shape)
        for dim in range(rank):
            if dim != 0:
                shape_text += ", "
            shape_text += str(shape[dim])
        shape_text += "}"
    return shape_text


def dump(name, obj):
    class_name = obj.__class__.__name__
    desc = None
    obj_id = None

    if class_name == "Dataset":
        desc = getShapeText(obj)
        obj_id = obj.id.id
    elif class_name == "Group":
        obj_id = obj.id.id
    elif class_name == "Datatype":
        obj_id = obj.id.id
    elif class_name == "SoftLink":
        desc =  '{' + obj.path + '}'
    elif class_name == "ExternalLink":
        desc = '{' + obj.filename + '//' + obj.path + '}'
    if desc is None:
        print("{0:24} {1}".format(name, class_name))
    else:
        print("{0:24} {1} {2}".format(name, class_name, desc))
    if verbose and obj_id is not None:
        print("    id: {0}".format(obj_id))

#
# Get Group based on URL
#
def getGroupFromUrl(url):
    try:
        f = h5py.File(url, 'r', endpoint=endpoint, username=username, password=password)
        return f['/']
    except OSError as err:
        print("OSError: {0}".format(err))

        sys.exit()

#
# Usage
#
def printUsage():
    print("usage: python h5ls.py [-r] [-a] [-e endpoint] [-u username] [-p password] urls")
    print("example: python h5ls.py -r -e http://data.hdfgroup.org:7253 tall.test.data.hdfgroup.org")
    sys.exit()

#
# Main
#

urls = []
argn = 1

while argn < len(sys.argv):
    arg = sys.argv[argn]
    if arg in ("-r", "--recursive"):
         recursive = True
         argn += 1
    elif arg in ("-v", "--verbose"):
         verbose = True
         argn += 1
    elif arg in ("-h", "--help"):
         printUsage()
    elif arg in ("-e", "--endpoint"):
         endpoint = sys.argv[argn+1]
         argn += 2
    elif arg in ("-u", "--username"):
         username = sys.argv[argn+1]
         argn += 2
    elif arg in ("-p", "--password"):
         password = sys.argv[argn+1]
         argn += 2
    elif arg[0] == '-':
         printUsage()
    else:
         urls.append(arg)
         argn += 1

if len(urls) == 0:
    # add a generic url
    urls.append("hdfgroup.org")

for url in urls:
    grp = getGroupFromUrl(url)
    dump('/', grp)
    if recursive:
        grp.visititems(dump)
    else:
        for k in grp:
            item = grp.get(k, getlink=True)
            if item.__class__.__name__ == "HardLink":
                # follow hardlinks
                item = grp.get(k)
            dump(k, item)
    grp.file.close()


