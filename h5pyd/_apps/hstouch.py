import sys
import os.path as op
import logging
import h5pyd as h5py
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Create domain or update timestamp if domain already exist
#

cfg = Config()

def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    logging.debug(f"getFolder({domain})")
    dir = h5py.Folder(domain, endpoint=endpoint, username=username, password=password, bucket=bucket)
    return dir

def createFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    owner = None
    if "hs_owner" in cfg:
        owner=cfg["hs_owner"]
    logging.debug(f"createFolder({domain})")
    dir = h5py.Folder(domain, mode='x', endpoint=endpoint, username=username, password=password, bucket=bucket, owner=owner)
    return dir

def getFile(domain, mode="a"):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    logging.debug(f"getFile(domain={domain}, mode={mode})")
    fh = h5py.File(domain, mode=mode, endpoint=endpoint, username=username, password=password, bucket=bucket)
    return fh

def createFile(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    owner = None
    if "hs_owner" in cfg:
        owner=cfg["hs_owner"]
    logging.debug(f"createFile({domain})")
    fh = h5py.File(domain, mode='x', endpoint=endpoint, username=username, password=password, bucket=bucket, owner=owner)
    return fh

def getParentDomain(domain):
    if domain[-1] == '/':
        if len(domain) > 1:
            domain = domain[:-1]
    parent_domain = op.dirname(domain)
    if not parent_domain.endswith("/"):
        parent_domain += "/"
    return parent_domain

def touchDomain(domain):
    # get handle to parent folder
    parent_domain = getParentDomain(domain)

    if parent_domain == "/":
        if not domain.endswith("/"):
            msg = "Only folders can be created as a top-level domain"
            logging.error(msg)
            sys.exit(msg)
        if len(domain) < 4:
            msg = "Top-level folders must be at least three characters"
            logging.error(msg)
            sys.exit(msg)

    else:
        try:
            getFolder(parent_domain)
        except IOError as oe:
            #print("errno:", oe.errno)
            if oe.errno in (404, 410):   # Not Found
                sys.exit(f"Parent domain: {parent_domain} not found")
            elif oe.errno == 401:  # Unauthorized
                sys.exit("Authorization failure")
            elif oe.errno == 403:  # Forbidden
                sys.exit("Not allowed")
            else:
                sys.exit(f"Unexpected error: {oe}")

    hdomain = None
    try:
        if domain.endswith("/"):
            hdomain = getFolder(domain)
        else:
            hdomain = getFile(domain, mode="r")
    except IOError as oe:
        if oe.errno in (404, 410):   # Not Found
            pass  # domain not found
        else:
            sys.exit(f"Unexpected error: {oe}")

    if hdomain is not None:
        logging.debug(f"domain: {domain} exists")
        if domain.endswith("/"):
            sys.exit("Can not update timestamp of folder object")
        else:
            try:
                # get domain for updating
                hdomain = getFile(domain, mode="a")
                r = hdomain['/']
                # create/update attribute to update lastModified timestamp of domain
                r.attrs["hstouch"] = 1
                cfg.print(f"updated timestamp for domain: {domain}")
                hdomain.close()
            except IOError as oe:
                msg = f"Got error updating domain: {oe}"
                logging.error(msg)
                sys.exit(msg)
    else:
        # create domain
        if not domain.endswith("/"):
            try:
                fh = createFile(domain)
                cfg.print(f"domain created: {domain}, root id: {fh.id.id}")
                fh.close()
            except IOError as oe:
                sys.exit(f"Got error updating domain: {oe}")
        else:
            # make folder
            try:
                fh = createFolder(domain)
                cfg.print(f"folder created {domain}")
                fh.close()
            except IOError as oe:
                sys.exit(f"Got error updating domain: {oe}")

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
    print("    Create a new domain or folder")
    print("       domain: HSDS domain (absolute path with or without 'hdf5:// prefix)")
    print("       folder: HSDS folder (path as above ending in '/')")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print(f"Example: {cmd}  hdf5://home/myfolder/emptydomain.h5")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()

#
# Main
#
def main():
    domains = []
    # additional options
    cfg.setitem("hs_owner", None, flags=["-o", "--owner"], choices=["OWNER",], help="set owner (must be run as an admin user)")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        domains = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(domains) == 0:
        # need a domain
        usage()

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    for domain in domains:
        touchDomain(domain)

if __name__ == "__main__":
    main()
