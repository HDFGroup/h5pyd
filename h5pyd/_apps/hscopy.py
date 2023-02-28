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
import h5pyd

if __name__ == "__main__":
    from config import Config
    from utillib import load_file
else:
    from .config import Config
    from .utillib import load_file

cfg = Config()

# ----------------------------------------------------------------------------------
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  SOURCE DEST")
    print("")
    print("Description:")
    print("    Copy HSDS domain to target")
    print("       SOURCE: HSDS domain (absolute path with or without 'hdf5:// prefix)")
    print("       DEST: HSDS domain or folder (path as above ending in '/')")
    print("")
    
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("examples:")
    print(f"   {cmd} /myfolder/orig.h5 /myfolder/copy.h5")
    print(f"   {cmd} /myfolder/orig.h5 /anotherfolder/")
    print(f"   {cmd} -z 5 /myfolder/uncompressed.h5 /myfolder/compressed.h5")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()


def getFile(domain):
    username = cfg["src_username"]
    if not username:
        username = cfg["hs_username"]
    password = cfg["src_password"]
    if not password:
        password = cfg["hs_password"]
    endpoint = cfg["src_endpoint"]
    if not endpoint:
        endpoint = cfg["hs_endpoint"]
    bucket = cfg["src_bucket"]
    if not bucket:
        bucket = cfg["hs_bucket"]

    fh = h5pyd.File(domain, 
        mode='r', 
        endpoint=endpoint, 
        username=username,
        password=password, 
        bucket=bucket)

    return fh

def createFile(domain, linked_domain=None, no_clobber=False):
    #print("createFile", domain)
    username = cfg["des_username"]
    if not username:
        username = cfg["hs_username"]
    password = cfg["des_password"]
    if not password:
        password = cfg["hs_password"]
    endpoint = cfg["des_endpoint"]
    if not endpoint:
        endpoint = cfg["hs_endpoint"]
    bucket = cfg["des_bucket"]
    if not bucket:
        bucket = cfg["hs_bucket"]
    if cfg["no_clobber"]:
        mode= "x"
    else:
        mode="w"

    fh = h5pyd.File(domain, 
        mode=mode, 
        endpoint=endpoint, 
        username=username, 
        password=password, 
        bucket=bucket)
        
    return fh


# end print_usage

# ----------------------------------------------------------------------------------
def main():

    cfg.setitem("no_clobber", False, flags=["-n", "--no-clobber"],  help="do not overwrite any domains")
    cfg.setitem("src_endpoint", None, flags=["--src-endpoint"],  choices=["ENDPOINT",], help="server endpoint for source domain")
    cfg.setitem("src_username", False, flags=["--src-user"],  choices=["USERNAME",], help="user name credential for source domain")
    cfg.setitem("src_password", False, flags=["--src-password"], choices=["PASSWORD",], help="password credential for source domain")
    cfg.setitem("src_bucket", False, flags=["--src-bucket"],  choices=["BUCKET"], help="storage bucket for source domain")
    cfg.setitem("des_endpoint", None, flags=["--des-endpoint"],  choices=["ENDPOINT",], help="server endpoint for dest domain")
    cfg.setitem("des_username", False, flags=["--des-user"],  choices=["USERNAME",], help="user name credential for dest domain")
    cfg.setitem("des_password", False, flags=["--des-password"], choices=["PASSWORD",], help="password credential for dest domain")
    cfg.setitem("des_bucket", False, flags=["--des-bucket"], choices=["BUCKET"], help="storage bucket for dest domain")
    cfg.setitem("compress", 0, flags=["-z",], choices=["LEVEL",], help="compression level from 0 (no compression) to 9 (highest)")
    cfg.setitem("nodata", False, flags=["--nodata",], help="do not copy dataset data")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        domains = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(domains) < 2:
        usage()
    
    src_domain = domains[0]
    des_domain = domains[1]

    if cfg["nodata"]:
        dataload = None
    else:
        dataload = "ingest"

    compressLevel = None
    if cfg["compress"]:
        try:
            compressLevel = int(cfg["compress"])
        except ValueError:
            msg = "Compression Level must be int between 0 and 9"
            logging.error(msg)
            sys.exit(msg)
    
    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    logging.info(f"source domain: {src_domain}")
    logging.info(f"target domain: {des_domain}")

    if src_domain.startswith("/") or src_domain.startswith("hdf5://"):
        logging.debug("source domain path is absolute")
    else:
        msg = "source domain must be an absolute path"
        logging.error(msg)
        sys.exit(msg)

    if src_domain[-1] == "/":
        msg = "source domain can't be a folder"
        logging.error(msg)
        sys.exit(msg)

    if des_domain.startswith("/") or des_domain.startswith("hdf5://"):
        logging.debug("target domain path is absolute")
    else:
        msg = "target domain must be an absolute path"
        logging.error(msg)
        sys.exit(msg)

    if des_domain[-1] == "/":
        # pull out the basename of src and add it to the 
        # end of des_domain
        fields = src_domain.split("/")
        des_domain += fields[-1]
        cfg.print(f"using {des_domain} for destination")
         
    # get a handle to input file
    try:
        fin = getFile(src_domain)
    except IOError as ioe:
        msg = f"Error opening file {src_domain}: {ioe.errno}"
        logging.error(msg)
        sys.exit(msg)


    try:
        fout = createFile(des_domain)
    except IOError as ioe:
        if ioe.errno == 403:
            msg = f"No write access to domain: {des_domain}"
        elif ioe.errno == 409 and cfg["no_clobber"]:
            msg = f"DEST domain: {des_domain} exists, aborting copy"
        else:
            msg = f"Error creating file {des_domain}: {ioe.errno}"
        logging.error(msg)
        sys.exit(msg)

    if compressLevel:
        compress_filter = "deflate"  # TBD - add option for other compressors
    else:
        compress_filter = None

    print("load_file, compressLevel:", compressLevel)

    try:
        # do the actual load
        load_file(
            fin,
            fout,
            verbose=cfg["verbose"],
            ignore_error=cfg["ignore"],
            dataload=dataload,
            compression=compress_filter,
            compression_opts=compressLevel,
        )

        msg = f"File {src_domain} uploaded to domain: {des_domain}"
        cfg.print(msg)

    except KeyboardInterrupt:
        logging.error("Aborted by user via keyboard interrupt.")
        sys.exit(1)


# __main__
if __name__ == "__main__":
    main()
