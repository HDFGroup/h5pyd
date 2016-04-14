import h5pyd as h5py
import numpy as np
import sys

#
# Print objects in a domain in the style of the h5ls utilitiy
#
recursive = False
verbose = False
endpoint = "http://127.0.0.1:5000"
f = None

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
        print("{0:25}{1}".format(name, class_name))
    else:
        print("{0:25}{1} {2}".format(name, class_name, desc))
    if verbose and obj_id is not None:
        print("    id: {0}".format(obj_id))
          

#
# Usage
#
def printUsage():
    print("usage: h5ls [-r] [-a] urls")
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
         endpoint = sys.argv[arn+1]
         argn += 2
    elif arg[0] == '-':
         printUsage()         
    else:
         urls.append(arg)
         argn += 1 
            
if len(urls) == 0:
    printUsage()
        
for url in urls:
    f = h5py.File(url, 'r', endpoint=endpoint)
    if recursive:
        f.visititems(dump)
    else:
        for k in f:
            dump(k, f[k])
    f.close()         

 
