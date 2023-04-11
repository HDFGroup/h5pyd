##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

import sys
import logging
import os.path as op
import h5pyd

if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

cfg = Config()

#----------------------------------------------------------------------------------
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  source_domain  des_domain")
    print(f"    {cmd} [ OPTIONS ]  source_domain  folder")
    print("")
    print("Description:")
    print("    Move domain from one location to another")
    print("       source_domain: domain to be moved ")
    print("       des_domain: destination domain")
    print("       folder: destination folder (Unix style ending in '/')")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("Examples:")
    print(f"     {cmd} /home/myfolder/file1.h5  /home/myfolder/file2.h5")
    print(f"     {cmd} /home/myfolder/file2.h5  /home/anotherfolder/")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit(-1)
#end print_usage

#----------------------------------------------------------------------------------
def print_config_example():
    print("# default")
    print("hs_username = <username>")
    print("hs_password = <passwd>")
    print("hs_endpoint = http://hsdshdflab.hdfgroup.org")
#print_config_example

#----------------------------------------------------------------------------------

def getFolder(domain, mode="r"):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    dir = h5pyd.Folder(domain, endpoint=endpoint, username=username,
                      password=password, bucket=bucket, mode=mode)
    return dir


def getFile(domain, mode="r"):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    fh = h5pyd.File(domain, mode=mode, endpoint=endpoint, username=username,
                   password=password, bucket=bucket, use_cache=True)
    return fh

def createFile(domain, linked_domain=None, no_clobber=False):
    #print("createFile", domain)
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    owner = None
    if no_clobber:
        mode= "x"
    else:
        mode="w"
    if "hs_owner" in cfg:
        owner=cfg["hs_owner"]
    fh = h5pyd.File(domain, mode=mode, endpoint=endpoint, username=username, password=password, bucket=bucket, owner=owner, linked_domain=linked_domain)
    return fh


def deleteDomain(domain, keep_root=False):

    # get handle to parent folder
    if domain.endswith('/'):
        path = domain[:-1]
    else:
        path = domain
    parent_domain = op.dirname(path)
    base_name = op.basename(path)

    if len(parent_domain) < 2:
        sys.exit("can't get parent domain")

    if not parent_domain.endswith('/'):
        parent_domain += '/'
    try:
        hparent = getFolder(parent_domain, mode='a')
    except IOError as oe:
        if oe.errno == 404:   # Not Found
            sys.exit(f"Parent domain: {parent_domain} not found")
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        else:
            sys.exit(f"Unexpected error: {oe}")

    if base_name not in hparent:
        # note - this may happen if the domain was recently created and not
        # yet synced to S3
        sys.exit(f"domain: {domain} not found")

    # delete the domain
    hparent.delete_item(base_name, keep_root=keep_root)
    if cfg["verbose"]:
        if domain.endswith('/'):
            print(f"Folder: {domain} deleted")
        else:
            print(f"Domain: {domain} deleted")

def main():

    cfg.setitem("no_clobber", False, flags=["-n", "--no-clobber"],  help="do not overwrite any domains")
    cfg.setitem("hs_owner", None, flags=["-o", "--owner"], choices=["OWNER",], help="set owner (must be run as an admin user)")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        domains = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(domains) < 2:
        # need at least source and target
        usage()

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    src_domain = domains[0]
    des_domain = domains[1]

    if src_domain[0] != '/' or des_domain[0] != '/':
        print("absolute paths must be used")
        usage()

    if src_domain[-1] == '/':
        print("folder can not be used for src domain")

    if des_domain[-1] == '/':
        # add on the filename from source to folder path
        des_domain += op.basename(src_domain)

    logging.info(f"source domain: {src_domain}")
    logging.info(f"target domain: {des_domain}")

    # get root id of source file
    try:
        fin = getFile(src_domain)
    except IOError as oe:
        # this will fail if we try to open a folder
        msg = f"Error: {oe.errno} getting domain: {src_domain}"
        logging.error(msg)
        sys.exit(str(oe))

    logging.info(f"src root id: {fin.id.id}")
    fin.close()

    # create a new file using the src domain for the root group
    no_clobber = cfg["no_clobber"]
    try:
        fout = createFile(des_domain, linked_domain=src_domain, no_clobber=no_clobber)
    except IOError as oe:
        msg = f"Error: {oe.errno} creating domain: {des_domain}"
        logging.error(msg)
        sys.exit(str(oe))

    cfg.print(f"{src_domain} copied to {des_domain}")
    logging.info(f"des root id: {fout.id.id}")

    try:
        deleteDomain(src_domain, keep_root=True)
        logging.info(f"domain: {src_domain} removed")
    except IOError as oe:
        msg = f"Error: {oe.errno} removing source domain: {src_domain}"
        logging.error(msg)
        sys.exit(str(oe))

# __main__
if __name__ == "__main__":
    main()
