import sys
import logging
import os.path as op
from datetime import datetime
import h5pyd as h5py
import numpy as np
if __name__ == "__main__":
    from config import Config
else:
    from h5pyd.config import Config

class HSLSCommand():

    """
        HSLS Command Object.  Can be used to set up and execute the HSLS algorithm.
    """

    def __init__(self, endpoint, username, password):
        """Create a new Folders object.

        endpoint
            HSDS Endpoint to access. E.g.: http://192.168.86.198:5101
        username
            The HSDS username to login as.
        password
            The password for the HSDS login.

        """

        self.cfg = Config()
        self.cfg['hs_endpoint'] = endpoint
        self.cfg['hs_username'] = username
        self.cfg['hs_password'] = password

    def intToStr(self, n):
        if self.cfg["human_readable"]:
            s = "{:,}".format(n)
        else:
            s = "{}".format(n)
        return s

    def format_size(self, n):
        if n is None or n == ' ':
            return ' ' * 8
        symbol = ' '
        if not self.cfg["human_readable"]:
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

    def getShapeText(self, dset):
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
                    if self.cfg["verbose"]:
                        # get unlimited dimension
                        max_extent = dset.maxshape[dim]
                        shape_text += '/'
                        if max_extent is None:
                            shape_text += "Inf"
                        else:
                            shape_text += str(max_extent)
            shape_text += "}"
        return shape_text

    def visititems(self, name, grp, visited):
        for k in grp:
            item = grp.get(k, getlink=True)
            class_name = item.__class__.__name__
            item_name = op.join(name, k)
            if class_name == "HardLink":
                # follow hardlinks
                try:
                    item = grp.get(k)
                    self.dump(item_name, item, visited=visited)
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

    def getTypeStr(self, dt):
        # TBD: convert dt to more HDF5-like representation
        return str(dt)


    def dump(self, name, obj, visited=None):
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
            desc = self.getShapeText(obj)
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

        if self.cfg["verbose"] and obj_id is not None:
            print("    {0:>32}: {1}".format("UUID", obj_id))

        if self.cfg["verbose"] and is_dataset and obj.shape is not None \
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
                print(fstr.format("Chunks", chunk_dims, self.intToStr(chunk_size),
                                self.intToStr(num_chunks)))
                if dset_size > 0:
                    utilization = allocated_size / dset_size
                    fstr = "    {0:>32}: {1} logical bytes, {2} allocated bytes, {3:.2f}% utilization"
                    print(fstr.format(storage_desc, self.intToStr(dset_size),
                                    self.intToStr(allocated_size),
                                    utilization * 100.0))
                else:
                    fstr = "    {0:>32}: {1} logical bytes, {2} allocated bytes"
                    print(fstr.format(storage_desc, self.intToStr(dset_size),
                                    self.intToStr(allocated_size)))

            else:
                # verbose info not available, just show the chunk layout
                fstr = "    {0:>32}: {1} {2} bytes"
                print(fstr.format("Chunks", chunk_dims, self.intToStr(chunk_size)))

            fstr = "    {0:>32}: {1}"
            print(fstr.format("Type", self.getTypeStr(obj.dtype)))  # dump out type info

        if self.cfg["showattrs"] and class_name in ("Dataset", "Table", "Group", "Datatype"):
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
            self.visititems(name, obj, visited)

    def dumpACL(self, acl):
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

    def dumpAcls(self, obj):
        try:
            acls = obj.getACLs()
        except IOError:
            print("read ACLs is not permitted")
            return

        for acl in acls:
            self.dumpACL(acl)

    def getFolder(self, domain):
        username = self.cfg["hs_username"]
        password = self.cfg["hs_password"]
        endpoint = self.cfg["hs_endpoint"]
        bucket   = self.cfg["hs_bucket"]
        pattern = self.cfg["pattern"]
        query = self.cfg["query"]
        batch_size = 100  # use smaller batchsize for interactively listing of large collections
        dir = h5py.Folder(domain, endpoint=endpoint, username=username,
                        password=password, bucket=bucket, pattern=pattern, query=query, batch_size=batch_size)
        return dir

    def getFile(self, domain):
        username = self.cfg["hs_username"]
        password = self.cfg["hs_password"]
        endpoint = self.cfg["hs_endpoint"]
        bucket = self.cfg["hs_bucket"]
        fh = h5py.File(domain, mode='r', endpoint=endpoint, username=username,
                    password=password, bucket=bucket, use_cache=True)
        return fh

    def visitDomains(self, domain, depth=1):
        if depth == 0:
            return 0

        count = 0
        if domain[-1] == '/':
            domain = domain[:-1]  # strip off trailing slash

        try:
            dir = self.getFolder(domain + '/')
            dir_class = "domain"
            display_name = domain
            num_bytes = ' '
            if dir.is_folder:
                dir_class = "folder"
                display_name += '/'
            elif self.cfg["verbose"]:
                # get the number of allocated bytes
                f = self.getFile(domain)
                num_bytes = f.total_size
                f.close()

            owner = dir.owner
            if owner is None:
                owner = ""
            if dir.modified is None:
                timestamp = ""
            else:
                timestamp = datetime.fromtimestamp(int(dir.modified))

            print("{:15} {:15} {:8} {} {}".format(owner, self.format_size(num_bytes),
                                                dir_class, timestamp,
                                                display_name))
            count += 1
            if self.cfg["showacls"]:
                self.dumpAcls(dir)
            for name in dir:
                item = dir[name]
                owner = item["owner"]
                full_path = domain + '/' + name

                num_bytes = " "
                if self.cfg["verbose"] and "total_size" in item:
                    num_bytes = item["total_size"]
                else:
                    num_bytes = " "
                dir_class = item["class"]
                if item["lastModified"] is None:
                    timestamp = ""
                else:
                    timestamp = datetime.fromtimestamp(int(item["lastModified"]))

                print("{:15} {:15} {:8} {} {}".format(owner, self.format_size(num_bytes),
                                                dir_class, timestamp,
                                                full_path))
                count += 1

                if dir_class == "folder":
                    # recurse for folders
                    n = self.visitDomains(domain + '/' + name, depth=(depth - 1))
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

    def execute(self, domain, verbose=False, showacls=False, showattrs=False, human_readable=False, pattern=None, 
    query=None, cmd=None, recursive=None, loglevel=logging.ERROR, logfile=None, endpoint=None, username=None, password=None, bucket=None):
        domains = [domain]
        self.execute(domains, verbose=verbose, showacls=showacls, showattrs=showattrs, human_readable=human_readable, pattern=pattern, query=query, 
        cmd=cmd, recursive=recursive, loglevel=loglevel, logfile=logfile, endpoint=endpoint, username=username, password=password, bucket=bucket)

    def execute(self, domains, verbose=False, showacls=False, showattrs=False, human_readable=False, pattern=None, 
    query=None, cmd=None, recursive=None, loglevel=logging.ERROR, logfile=None, endpoint=None, username=None, password=None, bucket=None):
        depth = 1
        self.cfg["verbose"] = False
        self.cfg["showacls"] = False
        self.cfg["showattrs"] = False
        self.cfg["human_readable"] = False
        self.cfg["pattern"] = None
        self.cfg["query"] = None
        self.cfg["cmd"] = None
        self.cfg['hs_bucket'] = None
        
        if verbose is not None:
            self.cfg['verbose'] = verbose
        if showacls is not None:
            self.cfg['showacls'] = showacls
        if showattrs is not None:
            self.cfg['showattrs'] = showattrs
        if human_readable is not None:
            self.cfg['human_readable'] = human_readable
        if pattern is not None:
            self.cfg['pattern'] = pattern
        if query is not None:
            self.cfg['query'] = query
        if cmd is not None:
            self.cfg['cmd'] = cmd
        if recursive is True:
            depth = -1
        if endpoint is not None:
            self.cfg['hs_endpoint'] = endpoint
        if username is not None:
            self.cfg['hs_username'] = username
        if password is not None:
            self.cfg['hs_password'] = password
        if bucket is not None:
            self.cfg['hs_bucket'] = bucket

        # setup logging
        logging.basicConfig(filename=logfile, format='%(levelname)s %(asctime)s %(message)s',
                            level=loglevel)
        logging.debug("set log_level to {}".format(loglevel))

        if len(domains) == 0:
            # add top-level domain
            domains.append("/")

        for domain in domains:
            if domain.endswith('/'):
                # given a folder path
                count = self.visitDomains(domain, depth=depth)
                print("{} items".format(count))

            else:
                try:
                    f = self.getFile(domain)
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
                    count = self.visitDomains(domain, depth=depth)
                    print("{} items".format(count))
                    continue
                self.dump('/', grp)

                if depth < 0:
                    # recursive
                    visited = {}  # dict of id to h5path
                    visited[grp.id.id] = '/'
                    self.visititems('/', grp, visited)
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
                        self.dump(k, item)
                if self.cfg["showacls"]:
                    self.dumpAcls(grp)
                grp.file.close()