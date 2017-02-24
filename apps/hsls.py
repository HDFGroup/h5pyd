import h5pyd as h5py
import numpy as np
import sys
import os.path as op
import os
from datetime import datetime
from config import Config

#
# Print objects in a domain in the style of the hsls utilitiy
#

verbose = False
showacls = False
 
cfg = Config()

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
    print("    acl: {0:24} {1}".format(acl["userName"], perms))

def dumpAcls(obj):
    try:
        default_acl = obj.getACL("default")
        dumpACL(default_acl)
    except OSError:
        print("read ACLs is not permitted")
        return

    try:
        acls = obj.getACLs()
     
        for acl in acls:
            if acl["userName"] == "default":
                continue
            dumpACL(acl)
    except OSError:
        # if requesting user does not permission to read non-default acl,
        # just ignore
        pass 

def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    dir = h5py.Folder(domain, endpoint=endpoint, username=username, password=password)
    return dir

def getFile(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    fh = h5py.File(domain, mode='r', endpoint=endpoint, username=username, password=password)
    return fh

def visitDomains(domain, recursive=False):
    #print("recursive:", recursive)
    #print("domain:", domain)
    count = 0
    if domain.endswith('/'):
        got_folder = False
        try:
            dir = getFolder(domain)
            if len(dir) > 0:
                got_folder = True
                owner = dir.owner
                timestamp = datetime.fromtimestamp(int(dir.modified))
                print("{:24} {} {}".format(owner, timestamp, domain))
                count += 1
                if showacls:
                    dumpAcls(dir)
                for name in dir:
                    # recurse for items in folder
                    n = visitDomains(domain + name, recursive=recursive)
                    count += n
                    
        except OSError as oe:
            sys.exit(str(oe))
             
         
    else:
        got_domain = False
        # see if this is a domain
        try:
            f = getFile(domain) 
            owner = f.owner
            timestamp = datetime.fromtimestamp(int(f.modified))
            print("{:24} {} {}".format(owner, timestamp, domain))
            f.close()
            got_domain = True
            count = 1
        except OSError:
            pass  # ignore if the domain is not valid 
        
        if not got_domain or recursive:
            # see if this is a folder 
            count += visitDomains(domain+'/', recursive=recursive)

    return count
            
#
# Get Group based on domain path
#
def getGroupFromDomain(domain):
    try:
        f = getFile(domain)
        return f['/']
    except OSError as err:
        return None

#
# Usage
#
def printUsage():
    print("usage: python hsls.py [-r] [-a] [-showacls] [-e endpoint] [-u username] [-p password] domains")
    print("example: python hsls.py -r -e http://data.hdfgroup.org:7253 /hdfgroup/data/test/tall.h5")
    sys.exit()

#
# Main
#

domains = []
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
        cfg["hs_endpoint"] = sys.argv[argn+1]
        argn += 2
    elif arg in ("-u", "--username"):
        cfg["hs_username"] = sys.argv[argn+1]
        argn += 2
    elif arg in ("-p", "--password"):
         cfg["hs_password"] = sys.argv[argn+1]
         argn += 2
    elif arg[0] == '-':
         printUsage()
    else:
         domains.append(arg)
         argn += 1
 
if len(domains) == 0:
    # add a generic url
    domains.append("hdfgroup.org")

for domain in domains:
    if domain.endswith('/'):
        # given a folder path
        count = visitDomains(domain, recursive=recursive)
        print("{} items".format(count))
    else:
         
        grp = getGroupFromDomain(domain)
        if grp is None:
            print("No group associated with this domain")
            continue
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


