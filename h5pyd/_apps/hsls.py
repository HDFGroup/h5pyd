
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
        s = f"{n:,}"
    else:
        s = f"{n}"
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
        return f"{n:7}B"
    else:
        return f"{n:7.1f}{symbol}"


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
                print(f"{item_name:24} {class_name} {desc}")

        elif class_name == "SoftLink":
            desc = '{' + item.path + '}'
            print(f"{item_name:24} {class_name} {desc}")
        elif class_name == "ExternalLink":
            desc = '{' + item.path + '//' + item.filename + '}'
            print(f"{item_name:24} {class_name} {desc}")
        else:
            desc = '{Unknown Link Type}'
            print(f"{item_name:24} {class_name} {desc}")


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
            print(f"{name:24} {class_name}, same as {same_as}")
            return
    elif class_name in ("ExternalLink", "SoftLink"):
        pass
    else:
        raise TypeError(f"unexpected classname: {class_name}")

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
        print(f"{name} {class_name}")
    else:
        print(f"{name} {class_name} {desc}")

    if cfg["verbose"] and obj_id is not None:
        uuid_str = "UUID"
        print(f"    {uuid_str:>32}: {obj_id}")

    if cfg["verbose"] and is_dataset and obj.shape is not None \
            and obj.chunks is not None:
        chunk_size = obj.dtype.itemsize

        if isinstance(obj.id.layout, dict):
            # H5D_CHUNKED_REF layout
            chunk_dims = obj.id.layout["dims"]
            obj_layout = obj.id.layout["class"]
        else:
            chunk_dims = obj.chunks
            obj_layout = "H5D_CHUNKED"
        storage_desc = f"Storage {obj_layout}"
        max_chunk_count = 1
        rank = len(obj.shape)
        for i in range(rank):
            extent = obj.shape[i]
            chunk_dim = chunk_dims[i]
            chunk_size *= chunk_dim
            max_chunk_count *= -(-extent // chunk_dim)
        dset_size = obj.dtype.itemsize
        for dim_extent in obj.shape:
            dset_size *= dim_extent

        if obj_layout == "H5D_CHUNKED_REF_INDIRECT":
            chunk_table_id = obj.id.layout["chunk_table"]
            chunk_table = obj.file[f"datasets/{chunk_table_id}"]
            num_chunks = int(np.prod(chunk_table.shape))
            chunk_table_elements = chunk_table[...].reshape((num_chunks,))
            num_linked_chunks = 0
            allocated_size = 0
            for e in chunk_table_elements:
                chunk_offset = e[0]
                chunk_size = e[1]
                if chunk_offset > 0 and chunk_size > 0:
                    num_linked_chunks += 1
                    allocated_size += chunk_size
            num_chunks = num_linked_chunks
            chunk_type = "linked"

        else:
            num_chunks = obj.num_chunks
            allocated_size = obj.allocated_size
            chunk_type = "allocated"

        if num_chunks is not None and allocated_size is not None:
            fstr = "    {0:>32}: {1} {2} bytes, {3}/{4} {5} chunks"

            s = fstr.format("Chunks", chunk_dims, intToStr(chunk_size), intToStr(num_chunks),
                            intToStr(max_chunk_count), chunk_type)
            print(s)
            if dset_size > 0:
                utilization = allocated_size / dset_size
                fstr = "    {0:>32}: {1} logical bytes, {2} {3} bytes, {4:.2f}% utilization"
                print(fstr.format(storage_desc, intToStr(dset_size),
                                  intToStr(allocated_size),
                                  chunk_type,
                                  utilization * 100.0))
            else:
                fstr = "    {0:>32}: {1} logical bytes, {2} {3} bytes"
                print(fstr.format(storage_desc, intToStr(dset_size),
                                  intToStr(allocated_size), chunk_type))

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
        dumpAttrs(obj)

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
    acl_username = acl["username"]
    print(f"    acl: {acl_username:24} {perms}")


def dumpAcls(obj):
    try:
        acls = obj.getACLs()
    except IOError:
        print("read ACLs is not permitted")
        return

    for acl in acls:
        dumpACL(acl)


def dumpAttrs(obj):
    """ print attributes of the given obj """

    for attr_name in obj.attrs:
        attr = obj.attrs[attr_name]
        el = "..."  # show this if the attribute is too large
        if isinstance(attr, np.ndarray):
            rank = len(attr.shape)
        else:
            rank = 0  # scalar data
        if rank > 1:
            val = "[" * rank + el + "]" * rank
            print(f"   attr: {attr_name:24} {val}")
        elif rank == 1 and attr.shape[0] > 1:
            val = f"[{attr[0]},{el}]"
            print(f"   attr: {attr_name:24} {val}")
        else:
            print(f"   attr: {attr_name:24} {attr}")


def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
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


def isFile(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]

    return h5py.is_hdf5(domain, endpoint=endpoint, username=username,
                        password=password, bucket=bucket)


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
        if cfg["names_only"]:
            parts = display_name.split('/')
            if len(parts) > depth:
                parts = parts[-depth:]
                parts = parts[1:]
                display_name = "/".join(parts)
        num_bytes = ' '
        if d.is_folder:
            dir_class = "folder"
            if not cfg["names_only"]:
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

        if not cfg["names_only"]:
            msg = f"{owner:35} {format_size(num_bytes):15} {dir_class:8} "
            msg += f"{timestamp} {display_name}"
            print(msg)
        count += 1
        if cfg["showacls"]:
            dumpAcls(d)
        for name in d:
            item = d[name]
            owner = item["owner"]
            full_path = display_name + name
            if item["class"] == "folder":
                full_path += "/"
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

            if cfg["names_only"]:
                # just print the name...
                print(full_path)
            else:
                msg = f"{owner:35} {format_size(num_bytes):15} {dir_class:8} "
                msg += f"{timestamp} {full_path}"
                print(msg)
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


def checkDomain(path):
    """ Convenience method to specify a domain + h5path as a single string.
        Walk up the path items, as soon as the parent is a domain or folder return it.
        Supply the other part as h5path.  """

    path_names = path.split("/")
    h5path = ""
    while path_names:
        domain_path = "/".join(path_names)
        if h5py.is_hdf5(domain_path):
            return (h5path, domain_path)
        last = path_names[-1]
        path_names = path_names[:-1]
        h5path = last + "/" + h5path

    return None


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
    cfg.setitem("pattern", None, flags=["--pattern",], choices=["REGEX",],
                help="list domains that match the given regex")
    cfg.setitem("query", None, flags=["--query",], choices=["QUERY",],
                help="list domains where the attributes of the root group match the given query string")
    cfg.setitem("recursive", False, flags=["-r", "--recursive"], help="recursively list sub-folders or sub-groups")
    cfg.setitem("dataset_path", None, flags=["-d", "--dataset"], choices=["H5PATH",], help="display specified dataset")
    cfg.setitem("group_path", None, flags=["-g", "--group"], choices=["H5PATH",], help="display specified group")
    cfg.setitem("datatype_path", None, flags=["-t", "--datatype"], choices=["H5PATH",],
                help="display specified datatype")
    cfg.setitem("names_only", False, flags=["-n", "--names"], help="list just folder names or link titles")
    cfg.setitem("human_readable", False, flags=["-H", "--human-readable"],
                help="with -v, print human readable sizes (e.g. 123M)")
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

        h5path = None

        if domain.endswith('/'):
            # given a folder path
            count = visitDomains(domain, depth=depth)
            if not cfg["names_only"]:
                print(f"{count} items")

        else:
            res = checkDomain(domain)
            if res is None:
                # couldn't find a domain, call getFile anyway so we can
                # report on exactly what went wrong
                pass
            else:
                h5path = res[0]
                domain = res[1]
                logging.debug(f"using h5path: {h5path} domain: {domain}")
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

            if h5path:
                if h5path not in f:
                    print(f"h5path: {h5path} not found in domain: {domain}")
                    continue

                obj = f[h5path]
                class_name = obj.__class__.__name__
                if class_name in ("Table", "Dataset"):
                    dump(h5path, obj)
                    continue
                elif class_name == "Datatype":
                    dump(h5path, obj)
                    continue
                else:
                    grp = obj
                    # we'll just fall through to our normal processin

            if cfg["group_path"]:
                if h5path:
                    print("group_path option can't be used when h5path is specified")
                    continue
                h5path = cfg["group_path"]
                if h5path not in grp:
                    print(f"group_path: {h5path} not found")
                    continue
                else:
                    grp = grp[h5path]
                    class_name = grp.__class__.__name__
                    if class_name != "Group":
                        if class_name in ("Table", "Dataset"):
                            hint = " (use --dataset option to display)"
                        elif class_name == "Datatype":
                            hint = " (use --datatype option to display)"
                        else:
                            hint = ""
                        print(f"group_path: {h5path} points to a {class_name}{hint}")
                        continue
            else:
                h5path = "/"  # start at root

            if cfg["dataset_path"]:
                dataset_path = cfg["dataset_path"]
                if dataset_path[0] == "/" and h5path != "/":
                    print("--group_path can't be used with absolute --dataset_path")
                    continue
                if dataset_path[0] == "/":
                    h5path = dataset_path  # replace h5path
                else:
                    if h5path[-1] != "/":
                        h5path = h5path + "/"
                    h5path = h5path + dataset_path
                    print("using h5path:", h5path)
                if h5path not in grp:
                    print("dataset path: {h5path} not found")
                    continue
                obj = grp[h5path]
                class_name = obj.__class__.__name__
                if class_name not in ("Table", "Dataset"):
                    print(f"was expecting a Dataset object but found: {class_name}")
                    continue

                dump(h5path, obj)
                continue

            if cfg["datatype_path"]:
                datatype_path = cfg["datatype_path"]
                if datatype_path[0] == "/" and h5path != "/":
                    print("--group_path can't be used with absolute --datatype_path")
                    continue
                if datatype_path[0] == "/":
                    h5path = datatype_path  # replace h5path
                else:
                    if h5path[-1] != "/":
                        h5path = h5path + "/"
                    h5path = h5path + datatype_path
                    print("using h5path:", h5path)
                if h5path not in grp:
                    print("datatype path: {h5path} not found")
                    continue
                obj = grp[h5path]
                class_name = obj.__class__.__name__
                if class_name != "Datatype":
                    print(f"was expecting a Datatype object but found: {class_name}")
                    continue
                dump(h5path, obj)
                continue

            if cfg["showattrs"]:
                # dump attributes for root group
                dumpAttrs(grp)

            if depth < 0:
                # recursive
                visited = {}  # dict of id to h5path
                visited[grp.id.id] = '/'
                visititems('/', grp, visited)
            else:
                for k in grp:
                    if cfg["names_only"]:
                        # just print the link name
                        print(k)
                        continue
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
