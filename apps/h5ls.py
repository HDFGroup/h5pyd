import h5pyd as h5py
import numpy as np
import sys
import os.path as op
import os
from datetime import datetime

#
# Print objects in a domain in the style of the h5ls utilitiy
#

verbose = False
showacls = False
endpoint = None
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

def visititems(name, grp, visited):
    for k in grp:
        item = grp.get(k, getlink=True)
        if item.__class__.__name__ == "HardLink":
            # follow hardlinks
            item = grp.get(k)
            item_name = op.join(name, k)
            dump(item_name, item, visited=visited)


def dump(name, obj, visited=None):
    class_name = obj.__class__.__name__
    desc = None
    obj_id = None
    if class_name in ("Dataset", "Group", "Datatype"):
        obj_id = obj.id.id
        if visited and obj_id in visited:
            same_as = visited[obj_id]
            print("{0:24} {1}, same as {}".format(name, class_name, same_as))
            return

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
    if visited is not None and obj_id is not None:
        visited[obj_id] = name 
    if class_name == "Group" and visited is not None:
        visititems(name, obj, visited)

def dumpACL(acl):
    perms = ""
    if acl["create"]:
        perms += 'c'
    else:
        perms += '-'
    if acl["read"]:
        perms += 'r'
    else:
        perms += '-'
    if acl["update"]:
        perms += 'u'
    else:
        perms += '-'
    if acl["delete"]:
        perms += 'd'
    else:
        perms += '-'
    if acl["readACL"]:
        perms += 'e'
    else:
        perms += '-'
    if acl["updateACL"]:
        perms += 'p'
    else:
        perms += '-'
    print("    {0:24} {1}".format(acl["userName"], perms))

def dumpAcls(obj):
    try:
        default_acl = obj.getACL("default")
        print("acls:")
        dumpACL(default_acl)
    except OSError:
        print("read ACLs is not permitted")
        return

    acls = obj.getACLs()
     
    for acl in acls:
        if acl["userName"] == "default":
            continue
        dumpACL(acl)

def visitDomains(url, recursive=False):
    #print("recursive:", recursive)
    #print("url:", url)
    
    count = 0
    if url.endswith('/'):
        got_folder = False
        try:
            dir = h5py.Folder(url)
            if len(dir) > 0:
                got_folder = True
                owner = dir.owner
                timestamp = datetime.fromtimestamp(int(dir.modified))
                print("{:24} {} {}".format(owner, timestamp, url))
                for name in dir:
                    # recurse for items in folder
                    n = visitDomains(url + name, recursive=recursive)
                    count += n
 
                    
        except OSError:
            pass # not a valid folder either!
    else:
        got_domain = False
        # see if this is a domain
        try:
            f = h5py.File(url, 'r', endpoint=endpoint, username=username, password=password)
            owner = f.owner
            timestamp = datetime.fromtimestamp(int(f.modified))
            print("{:24} {} {}".format(owner, timestamp, url))
            f.close()
            got_domain = True
            count = 1
        except OSError:
            pass  # ignore if the url doesn't point to a valid domain
        
        if not got_domain or recursive:
            # see if this is a folder url
            count += visitDomains(url+'/', recursive=recursive)

    return count
       

            
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
    print("usage: python h5ls.py [-r] [-a] [-showacls] [-e endpoint] [-u username] [-p password] urls")
    print("example: python h5ls.py -r -e http://data.hdfgroup.org:7253 tall.test.data.hdfgroup.org")
    sys.exit()

#
# Main
#

urls = []
argn = 1
recursive = False

while argn < len(sys.argv):
    arg = sys.argv[argn]
    if arg in ("-r", "--recursive"):
         recursive = True
         argn += 1
    elif arg in ("-v", "--verbose"):
         verbose = True
         argn += 1
    elif arg in ("-showacls", "--showacls"):
         showacls = True
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

if endpoint is None:
    if "H5SERV_ENDPOINT" in os.environ:
        endpoint = os.environ["H5SERV_ENDPOINT"]
    else:
        endpoint = "http://127.0.0.1:5000"

if username is None and "H5SERV_USERNAME" in os.environ:
    username = os.environ["H5SERV_USERNAME"]

if password is None and "H5SERV_PASSWORD" in os.environ:
    password = os.environ["H5SERV_PASSWORD"]
    

if len(urls) == 0:
    # add a generic url
    urls.append("hdfgroup.org")

for url in urls:
    if url.endswith('/'):
        # given a folder path
        count = visitDomains(url, recursive=recursive)
        print("{} items".format(count))
    else:
         
        grp = getGroupFromUrl(url)
        dump('/', grp)
    
        if recursive:
            visited = {} # dict of id to h5path
            visited[grp.id.id] = '/'
            visititems('/', grp, visited)
        else:
            for k in grp:
                item = grp.get(k, getlink=True)
                if item.__class__.__name__ == "HardLink":
                    # follow hardlinks
                    item = grp.get(k)
                dump(k, item)
        if showacls:
            dumpAcls(grp)
        grp.file.close()


