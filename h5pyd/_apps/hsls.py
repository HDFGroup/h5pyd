
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
        return ' ' * 8
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


def getTypeStr(dt):
    # TBD: convert dt to more HDF5-like representation
    return str(dt)


def dump(name, obj, visited=None):
    class_name = obj.__class__.__name__
    desc = None
    obj_id = None
    if class_name in ("Dataset", "Group", "Datatype", "Table"):
        obj_id = obj.id.id
        if visited and obj_id in visited:
            same_as = visited[obj_id]
            print("{0:24} {1}, same as {2}".format(name, class_name, same_as))
            return
    elif class_name in ("ExternalLink", "SoftLink"):
        pass
    else:
        raise TypeError("unexpected classname: {}".format(class_name))

    is_dataset = False
    if class_name in ("Dataset", "Table"):
        is_dataset = True


    if is_dataset:
        desc = getShapeText(obj)
        obj_id = obj.id.id
    elif class_name == "Group":
        obj_id = obj.id.id
    elif class_name == "Datatype":
        obj_id = obj.id.id
    elif class_name == "SoftLink":
        desc = '{' + obj.path + '}'
    elif class_name == "ExternalLink":
        desc = '{' + obj.filename + '//' + obj.path + '}'

    if desc is None:
        print("{0} {1}".format(name, class_name))
    else:
        print("{0} {1} {2}".format(name, class_name, desc))

    if cfg["verbose"] and obj_id is not None:
        print("    {0:>32}: {1}".format("UUID", obj_id))

    if cfg["verbose"] and is_dataset and obj.shape is not None \
            and obj.chunks is not None:
        chunk_size = obj.dtype.itemsize
        if isinstance(obj.chunks, dict):
            # H5D_CHUNKED_REF layout
            chunk_dims = obj.chunks["dims"]
            storage_desc = "Storage " + obj.chunks["class"]
        else:
            chunk_dims = obj.chunks
            storage_desc = "Storage H5D_CHUNKED"
        for chunk_dim in chunk_dims:
            chunk_size *= chunk_dim
        dset_size = obj.dtype.itemsize
        for dim_extent in obj.shape:
            dset_size *= dim_extent

        num_chunks = obj.num_chunks
        allocated_size = obj.allocated_size
        if num_chunks is not None and allocated_size is not None:
            fstr = "    {0:>32}: {1} {2} bytes, {3} allocated chunks"
            print(fstr.format("Chunks", chunk_dims, intToStr(chunk_size),
                              intToStr(num_chunks)))
            if dset_size > 0:
                utilization = allocated_size / dset_size
                fstr = "    {0:>32}: {1} logical bytes, {2} allocated bytes, {3:.2f}% utilization"
                print(fstr.format(storage_desc, intToStr(dset_size),
                                  intToStr(allocated_size),
                                  utilization * 100.0))
            else:
                fstr = "    {0:>32}: {1} logical bytes, {2} allocated bytes"
                print(fstr.format(storage_desc, intToStr(dset_size),
                                  intToStr(allocated_size)))

        else:
            # verbose info not available, just show the chunk layout
            fstr = "    {0:>32}: {1} {2} bytes"
            print(fstr.format("Chunks", chunk_dims, intToStr(chunk_size)))

        fstr = "    {0:>32}: {1}"
        print(fstr.format("Type", getTypeStr(obj.dtype)))  # dump out type info

    if cfg["showattrs"] and class_name in ("Dataset", "Table", "Group", "Datatype"):
        # dump attributes for the object
        for attr_name in obj.attrs:
            attr = obj.attrs[attr_name]
            el = "..."  # show this if the attribute is too large
            if isinstance(attr, np.ndarray):
                rank = len(attr.shape)
            else:
                rank = 0  # scalar data
            if rank > 1:
                val = "[" * rank + el + "]" * rank
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
        acls = obj.getACLs()
    except IOError:
        print("read ACLs is not permitted")
        return

    for acl in acls:
        dumpACL(acl)


def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket   = cfg["hs_bucket"]
    pattern = cfg["pattern"]
    query = cfg["query"]
    batch_size = 100  # use smaller batchsize for interactively listing of large collections
    dir = h5py.Folder(domain, endpoint=endpoint, username=username,
                      password=password, bucket=bucket, pattern=pattern, query=query, batch_size=batch_size)
    return dir


def getFile(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    fh = h5py.File(domain, mode='r', endpoint=endpoint, username=username,
                   password=password, bucket=bucket, use_cache=True)
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
            num_bytes = f.total_size
            f.close()

        owner = dir.owner
        if owner is None:
            owner = ""
        if dir.modified is None:
            timestamp = ""
        else:
            timestamp = datetime.fromtimestamp(int(dir.modified))

        print("{:15} {:15} {:8} {} {}".format(owner, format_size(num_bytes),
                                              dir_class, timestamp,
                                              display_name))
        count += 1
        if cfg["showacls"]:
            dumpAcls(dir)
        for name in dir:
            item = dir[name]
            owner = item["owner"]
            full_path = domain + '/' + name

            num_bytes = " "
            if cfg["verbose"] and "total_size" in item:
                num_bytes = item["total_size"]
            else:
                 num_bytes = " "
            dir_class = item["class"]
            if item["lastModified"] is None:
                timestamp = ""
            else:
                timestamp = datetime.fromtimestamp(int(item["lastModified"]))

            print("{:15} {:15} {:8} {} {}".format(owner, format_size(num_bytes),
                                              dir_class, timestamp,
                                              full_path))
            count += 1

            if dir_class == "folder":
                # recurse for folders
                n = visitDomains(domain + '/' + name, depth=(depth - 1))
                count += n

    except IOError as oe:
        if oe.errno in (403, 404, 410):
            # TBD: recently created domains may not be immediately visible to
            # the service Once the flush operation is implemented, this should
            # be an issue for h5pyd apps
            #
            # Also, ignore domains for which we don't have permsssions (403)
            pass
        else:
            print("error getting domain:", domain)
            sys.exit(str(oe))

    return count



#
# Usage
#
def printUsage():
    print("usage: {} [-r] [-v] [-h] [--showacls] [--showattrs] [--loglevel debug|info|warning|error] [--logfile <logfile>] [-e endpoint] [-u username] [-p password] [--bucket bucketname] domains".format(cfg["cmd"]))
    print("example: {} -r -e http://hsdshdflab.hdfgroup.org /shared/tall.h5".format(cfg["cmd"]))
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -H | --human-readable :: with -v, print human readable sizes (e.g. 123M)")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --showacls :: print domain ACLs")
    print("     --showattrs :: print attributes")
    print("     --pattern  :: <regex>  :: list domains that match the given regex")
    print("     --query :: <query> list domains where the attributes of the root group match the given query string")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     -h | --help    :: This message.")
    sys.exit()


#
# Main
#
def main():
    domains = []
    argn = 1
    depth = 1
    loglevel = logging.ERROR
    logfname = None
    cfg["verbose"] = False
    cfg["showacls"] = False
    cfg["showattrs"] = False
    cfg["human_readable"] = False
    cfg["pattern"] = None
    cfg["query"] = None
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]

    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn + 1]
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
        elif arg in ("-b", "--bucket"):
            cfg["hs_bucket"] = val
            argn += 2
        elif arg == "--pattern":
            cfg["pattern"] = val
            argn += 2
        elif arg == "--query":
            cfg["query"] = val
            argn += 2

        elif arg[0] == '-':
            printUsage()
        else:
            domains.append(arg)
            argn += 1

    # setup logging
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s',
                        level=loglevel)
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
            try:
                f = getFile(domain)
            except IOError as ioe:
                if ioe.errno == 401:
                    print("Username/Password missing or invalid")
                    continue
                if ioe.errno == 403:
                    print("No permission to read domain: {}".format(domain))
                    continue
                elif ioe.errno == 404:
                    print("Domain {} not found".format(domain))
                    continue
                elif ioe.errno == 410:
                    print("Domain {} has been removed".format(domain))
                    continue
                else:
                    print("Unexpected error: {}".format(ioe))
                    continue

            grp = f['/']
            if grp is None:
                print("{}: No such domain".format(domain))
                domain += '/'
                count = visitDomains(domain, depth=depth)
                print("{} items".format(count))
                continue
            dump('/', grp)

            if depth < 0:
                # recursive
                visited = {}  # dict of id to h5path
                visited[grp.id.id] = '/'
                visititems('/', grp, visited)
            else:
                for k in grp:
                    item = grp.get(k, getlink=True)
                    if item.__class__.__name__ == "HardLink":
                        # follow hardlinks
                        try:
                            item = grp[k]
                        except IOError:
                            # object deleted?  Just dump link info
                            pass
                    dump(k, item)
            if cfg["showacls"]:
                dumpAcls(grp)
            grp.file.close()


if __name__ == "__main__":
    main()
