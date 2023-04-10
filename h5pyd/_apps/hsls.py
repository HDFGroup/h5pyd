
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
    shape_text = "{NULL}"
    shape = dset.shape
    if shape is not None:
        shape_text = "{"
        rank = len(shape)
        if rank == 0:
            shape_text += "{SCALAR}"
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
        
        if isinstance(obj.id.layout, dict):
            # H5D_CHUNKED_REF layout
            chunk_dims = obj.id.layout["dims"]
            storage_desc = "Storage " + obj.id.layout["class"]
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

        # show filters (if any)
        # currently HSDS only supports the shuffle filter (not fletcher32 or 
        # scaleoffset), so just check for shuffle and whatever compressor may
        # be applied
        filter_number = 0
        fstr = "    {0:>30}-{1}: {2} OPT {{{3}}}"
        if obj.compression_opts:
            compression_opts = obj.compression_opts
        else:
            compression_opts = ""
        if obj.shuffle:
            print(fstr.format("Filter", filter_number, "shuffle", compression_opts))
            filter_number += 1
        if obj.compression:
            print(fstr.format("Filter", filter_number, obj.compression, compression_opts))

        # display type info

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
    if cfg["verbose"]:
        verbose = True
    else:
        verbose = False
    batch_size = 100  # use smaller batchsize for interactively listing of large collections
    d = h5py.Folder(domain, endpoint=endpoint, username=username, verbose=verbose,
                      password=password, bucket=bucket, pattern=pattern, query=query, batch_size=batch_size)
    return d


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
        d = getFolder(domain + '/')
        dir_class = "domain"
        display_name = domain
        num_bytes = ' '
        if d.is_folder:
            dir_class = "folder"
            display_name += '/'
        elif cfg["verbose"]:
            # get the number of allocated bytes
            f = getFile(domain)
            num_bytes = f.total_size
            f.close()

        owner = d.owner
        if owner is None:
            owner = ""
        if d.modified is None:
            timestamp = ""
        else:
            timestamp = datetime.fromtimestamp(int(d.modified))

        print("{:35} {:15} {:8} {} {}".format(owner, format_size(num_bytes),
                                              dir_class, timestamp,
                                              display_name))
        count += 1
        if cfg["showacls"]:
            dumpAcls(d)
        for name in d:
            item = d[name]
            owner = item["owner"]
            full_path = domain + '/' + name

            num_bytes = " "
            if cfg["verbose"] and "total_size" in item:
                num_bytes = item["total_size"]
                if "total_size" not in cfg:
                    cfg["total_size"] = 0
                cfg["total_size"] += item["total_size"]
            else:
                 num_bytes = " "
            dir_class = item["class"]
            if item["lastModified"] is None:
                timestamp = ""
            else:
                timestamp = datetime.fromtimestamp(int(item["lastModified"]))

            print("{:35} {:15} {:8} {} {}".format(owner, format_size(num_bytes),
                                              dir_class, timestamp,
                                              full_path))
            if cfg["showacls"]:
                if dir_class == "folder":
                    with getFolder(domain + '/' + name + '/') as f:
                        dumpAcls(f)
                else:
                    with getFile(domain + '/' + name) as f:
                        dumpAcls(f)
            count += 1

            if dir_class == "folder":
                # recurse for folders
                n = visitDomains(domain + '/' + name, depth=(depth - 1))
                count += n

    except IOError as oe:
        if oe.errno in (403, 404, 410):
            # Ignore domains for which:
            #   * we don't have permsssions (403)
            #   * not found error (404)
            #   * recently deleted (410)
            pass
        else:
            print("error getting domain:", domain)
            sys.exit(str(oe))

    return count



#
# Usage
#
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  domain")
    print(f"    {cmd} [ OPTIONS ]  folder")
    print("")
    print("Description:")
    print("    List contents of a domain or folder")
    print("       domain: HSDS domain (absolute path with or without 'hdf5:// prefix)")
    print("       folder: HSDS folder (path as above ending in '/')")
    print("")
    
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print(f"example: {cmd} -r -e http://hsdshdflab.hdfgroup.org /shared/tall.h5")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()


#
# Main
#
def main():
    domains = []

    # additional options
    cfg.setitem("showacls", False, flags=["--showacls",], help="display domain ACLs")
    cfg.setitem("showattrs", False, flags=["--showattrs",], help="display domain attributes")
    cfg.setitem("pattern", None, flags=["--pattern",], choices=["REGEX",], help="list domains that match the given regex")
    cfg.setitem("query", None, flags=["--query",], choices=["QUERY",], help="list domains where the attributes of the root group match the given query string")
    cfg.setitem("recursive", False, flags=["-r", "--recursive"], help="recursively list sub-folders or sub-groups")
    cfg.setitem("human_readable", False, flags=["-H", "--human-readable"], help="with -v, print human readable sizes (e.g. 123M)")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        domains = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(domains) == 0:
        # need a domain - use root
        domains.append("/")

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    for domain in domains:
        if cfg["recursive"]:
            depth = -1
        else:
            depth = 1

        if domain.endswith('/'):
            # given a folder path
            count = visitDomains(domain, depth=depth)
            print(f"{count} items")

        else:
            try:
                f = getFile(domain)
            except IOError as ioe:
                if ioe.errno == 401:
                    print("Username/Password missing or invalid")
                    continue
                if ioe.errno == 403:
                    print(f"No permission to read domain: {domain}")
                    continue
                elif ioe.errno == 404:
                    print(f"Domain {domain} not found")
                    continue
                elif ioe.errno == 410:
                    print(f"Domain {domain} has been removed")
                    continue
                else:
                    print(f"Unexpected error: {ioe}")
                    continue

            grp = f['/']
            if grp is None:
                print(f"{domain}: No such domain")
                domain += '/'
                count = visitDomains(domain, depth=depth)
                print(f"{count} items")
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
    if "total_size" in cfg and cfg["total_size"] > 0:
        # print aggregate size
        total_size = cfg["total_size"]
        s = format_size(total_size)
        s = s.strip()

        print(f"{s} bytes")


if __name__ == "__main__":
    main()
