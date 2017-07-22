
import sys
import os.path as op
import logging
from datetime import datetime
import h5pyd as h5py
import numpy as np
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Print objects in a domain in the style of the hsls utilitiy
#
 
cfg = Config()

def intToStr(n):
    if cfg["human_readable"]:
        s = "{:,}".format(n)
    else:
        s = "{}".format(n)
    return s

def format_size(n):
    if n is None or n == ' ':
        return ' '*8
    symbol = ' '
    if not cfg["human_readable"]:
        return str(n)
    # convert to common storage unit    
    for s in ('B', 'K', 'M', 'G', 'T'):
        if n < 1024:
            symbol = s
            break
        n /= 1024
    if symbol == 'B':
        return "{:7}B".format(n)
    else:
        return "{:7.1f}{}".format(n, symbol)

def getShapeText(dset):
    shape_text = "Scalar"
    shape = dset.shape
    if shape is not None:
        shape_text = "{"
        rank = len(shape)
        if rank == 0:
            shape_text += "SCALAR"
        else:
            for dim in range(rank):
                if dim != 0:
                    shape_text += ", "
                shape_text += str(shape[dim])
                if cfg["verbose"]:
                    # get unlimited dimension
                    max_extent = dset.maxshape[dim]
                    shape_text += '/'
                    if max_extent is None:
                        shape_text += "Inf"
                    else:
                        shape_text += str(max_extent)
        shape_text += "}"
    return shape_text

def visititems(name, grp, visited):
    for k in grp:
        item = grp.get(k, getlink=True)
        class_name = item.__class__.__name__
        item_name = op.join(name, k)
        if class_name == "HardLink":
            # follow hardlinks    
            try:
                item = grp.get(k)         
                dump(item_name, item, visited=visited)
            except IOError:
                # object deleted but hardlink left?
                desc = "{Missing hardlink object}"
                print("{0:24} {1} {2}".format(item_name, class_name, desc))
            
        elif class_name == "SoftLink":
            desc = '{' + item.path + '}'
            print("{0:24} {1} {2}".format(item_name, class_name, desc))
        elif class_name == "ExternalLink":
            desc = '{' + item.path + '//' + item.filename + '}'
            print("{0:24} {1} {2}".format(item_name, class_name, desc))
        else:
            desc = '{Unknown Link Type}'
            print("{0:24} {1} {2}".format(item_name, class_name, desc))


def dump(name, obj, visited=None):
    class_name = obj.__class__.__name__
    desc = None
    obj_id = None
    if class_name in ("Dataset", "Group", "Datatype"):
        obj_id = obj.id.id
        if visited and obj_id in visited:
            same_as = visited[obj_id]
            print("{0:24} {1}, same as {2}".format(name, class_name, same_as))
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
    if cfg["verbose"] and obj_id is not None:
        print("    {0:>12}: {1}".format("UUID", obj_id))
    if cfg["verbose"] and class_name == "Dataset" and obj.shape is not None and obj.chunks is not None:
        chunk_size = obj.dtype.itemsize
        for chunk_dim in obj.chunks:
            chunk_size *= chunk_dim
        dset_size = obj.dtype.itemsize
        for dim_extent in obj.shape:
            dset_size *= dim_extent
        
        num_chunks = obj.num_chunks
        allocated_size = obj.allocated_size
        if num_chunks is not None and allocated_size is not None:    
            fstr = "    {0:>12}: {1} {2} bytes, {3} allocated chunks"
            print(fstr.format("Chunks", obj.chunks, intToStr(chunk_size), intToStr(num_chunks)))
            if allocated_size > 0:
                utilization = dset_size / allocated_size   
                fstr = "    {0:>12}: {1} logical bytes, {2} allocated bytes, {3:.2f}% utilization"
                print(fstr.format("Storage", intToStr(dset_size), intToStr(allocated_size), utilization*100.0))
            else:
                fstr = "    {0:>12}: {1} logical bytes, {2} allocated bytes"
                print(fstr.format("Storage", intToStr(dset_size), intToStr(allocated_size)))
            
        else:
            # verbose info not available, just show the chunk layout
            fstr = "    {0:>12}: {1} {2} bytes"
            print(fstr.format("Chunks", obj.chunks, intToStr(chunk_size)))
    
        

    if cfg["showattrs"] and class_name in ("Dataset", "Group", "Datatype"):
        # dump attributes for the object
        for attr_name in obj.attrs:
            attr = obj.attrs[attr_name]
            el = "..."  # show this if the attribute is too large
            if isinstance(attr, np.ndarray):
                rank = len(attr.shape)
            else:
                rank = 0  # scalar data
            if rank > 1:
                val = "["*rank + el + "]"*rank
                print("   attr: {0:24} {1}".format(attr_name, val))
            elif rank == 1 and attr.shape[0] > 1:
                val = "[{},{}]".format(attr[0], el)
                print("   attr: {0:24} {1}".format(attr_name, val))
            else:
                print("   attr: {0:24} {1}".format(attr_name, attr))

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
    fh = h5py.File(domain, mode='r', endpoint=endpoint, username=username, password=password, use_cache=True)
    return fh


def visitDomains(domain, depth=1):
    if depth == 0:
        return 0
     
    count = 0
    if domain[-1] == '/':
        domain = domain[:-1]  # strip off trailing slash
    
    try:
        dir = getFolder(domain + '/')
        dir_class = "domain"
        display_name = domain
        num_bytes = ' '
        if dir.is_folder:
            dir_class = "folder"
            display_name += '/'
        elif cfg["verbose"]:
            # get the number of allocated bytes
            f = getFile(domain)
            num_bytes = f.allocated_bytes
            f.close()
        
        owner = dir.owner
        if owner is None:
            owner = ""
        if dir.modified is None:
            timestamp = ""
        else:
            timestamp = datetime.fromtimestamp(int(dir.modified))
 
        print("{:15} {:15} {:8} {} {}".format(owner, format_size(num_bytes), dir_class, timestamp, display_name))
         
        count += 1
        if cfg["showacls"]:
            dumpAcls(dir)
        if dir.is_folder:
            for name in dir:
                # recurse for items in folder
                n = visitDomains(domain + '/' + name, depth=(depth-1))
                count += n
                    
    except OSError as oe:
        if oe.errno in (404, 410):
            # TBD: recently creating domains may not be immediately visible to the service
            # Once the flush operation is implemented, this should be an issue for h5pyd apps
            pass
        else:
            print("error getting domain:", domain)
            sys.exit(str(oe))
             

    return count
            
#
# Get Group based on domain path
#
def getGroupFromDomain(domain):
    try:
        f = getFile(domain)
        return f['/']
    except OSError:
        return None

#
# Usage
#
def printUsage():
    print("usage: python hsls.py [-r] [-v] [-h] [--showacls] [--showattrs] [--loglevel debug|info|warning|error] [--logfile <logfile>] [-e endpoint] [-u username] [-p password] domains")
    print("example: python hsls.py -r -e http://data.hdfgroup.org:7253 /hdfgroup/data/test/tall.h5")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -H | --human-readable :: with -v, print human readable sizes (e.g. 123M)")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://example.com:8080")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --showacls :: print domain ACLs")
    print("     --showattrs :: print attributes")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     -h | --help    :: This message.")
    sys.exit()

#
# Main
#
def main():
    domains = []
    argn = 1
    depth = 2
    loglevel = logging.ERROR
    logfname=None
    cfg["verbose"] = False
    cfg["showacls"] = False
    cfg["showattrs"] = False
    cfg["human_readable"] = False

    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
        if arg in ("-r", "--recursive"):
            depth = -1
            argn += 1
        elif arg in ("-v", "--verbose"):
            cfg["verbose"] = True
            argn += 1
        elif arg in ("-H", "--human-readable"):
            cfg["human_readable"] = True
            argn += 1
        elif arg == "--loglevel":
            val = val.upper()
            if val == "DEBUG":
                loglevel = logging.DEBUG
            elif val == "INFO":
                loglevel = logging.INFO
            elif val in ("WARN", "WARNING"):
                loglevel = logging.WARNING
            elif val == "ERROR":
                loglevel = logging.ERROR
            else:
                printUsage()  
            argn += 2
        elif arg == '--logfile':
            logfname = val
            argn += 2
        elif arg in ("-showacls", "--showacls"):
            cfg["showacls"] = True
            argn += 1
        elif arg in ("-showattrs", "--showattrs"):
            cfg["showattrs"] = True
            argn += 1
        elif arg in ("-h", "--help"):
            printUsage()
        elif arg in ("-e", "--endpoint"):
            cfg["hs_endpoint"] = val
            argn += 2
        elif arg in ("-u", "--username"):
            cfg["hs_username"] = val
            argn += 2
        elif arg in ("-p", "--password"):
            cfg["hs_password"] = val
            argn += 2
        elif arg[0] == '-':
            printUsage()
        else:
            domains.append(arg)
            argn += 1

    # setup logging
    logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))
 
 
    if len(domains) == 0:
        # add top-level domain
        domains.append("/")

    for domain in domains:
        if domain.endswith('/'):
            # given a folder path
            count = visitDomains(domain, depth=depth)
            print("{} items".format(count))
        else:
         
            grp = getGroupFromDomain(domain)
            if grp is None:
                print("{}: No such domain".format(domain))
                continue
            dump('/', grp)
    
            if depth < 0:
                # recursive
                visited = {} # dict of id to h5path
                visited[grp.id.id] = '/'
                visititems('/', grp, visited)
            else:
                for k in grp:
                    item = grp.get(k, getlink=True)
                    if item.__class__.__name__ == "HardLink":
                        # follow hardlinks
                        try:
                            item = grp.get(k)
                        except IOError:
                            # object deleted?  Just dump link info
                            pass
                    dump(k, item)
            if cfg["showacls"]:
                dumpAcls(grp)
            grp.file.close()

if __name__ == "__main__":
    main()
