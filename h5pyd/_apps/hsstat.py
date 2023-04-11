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
from datetime import datetime
import h5pyd

if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Print objects in a domain in the style of the hsls utilitiy
#

cfg = Config()

#
# log error and abort app
#
def abort(msg):
    logging.error(msg)
    if cfg["logfile"]:
        # write to stderr if we are output logs to a file
        sys.stderr.write(msg + "\n")
    logging.error("exiting program with return code -1")
    sys.exit(-1)

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
    print("    Get domain stats for domain or folder")
    print("       domain: HSDS domain (absolute path with or without 'hdf5:// prefix)")
    print("       folder: HSDS folder (path as above ending in '/')")
    print("")
    
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("examples:")
    print(f"   {cmd} -e http://hsdshdflab.hdfgroup.org")
    print(f"   {cmd} -e http://hsdshdflab.hdfgroup.org /shared/tall.h5")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()


def format_size(n):
    if n is None or n == " ":
        return " " * 8
    symbol = " "
    if not cfg["human_readable"]:
        return str(n)
    # convert to common storage unit
    for s in ("B", "K", "M", "G", "T"):
        if n < 1024:
            symbol = s
            break
        n /= 1024
    if symbol == "B":
        return "{:}B".format(n)
    else:
        return "{:.1f}{}".format(n, symbol)


def getDomainInfo(domain, cfg):
    """get info about the domain and print"""
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    if "rescan" in cfg and cfg["rescan"]:
        mode = "r+"  # need write intent
    else:
        mode = "r"

    if domain.endswith("/"):
        is_folder = True
    else:
        is_folder = False

    try:
        if is_folder:
            f = h5pyd.Folder(
                domain,
                mode=mode,
                endpoint=endpoint,
                username=username,
                password=password,
                bucket=bucket,
                use_cache=True,
            )
        else:
            f = h5pyd.File(
                domain,
                mode=mode,
                endpoint=endpoint,
                username=username,
                password=password,
                bucket=bucket,
                use_cache=False,
            )
    except IOError as oe:
        if oe.errno in (404, 410):  # Not Found
            abort(f"domain: {domain} not found")
        elif oe.errno == 401:  # Unauthorized
            abort("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            abort("Not allowed")
        else:
            abort(f"Unexpected error: {oe}")

    timestamp = datetime.fromtimestamp(int(f.modified))
    if not is_folder and f.last_scan:
        last_scan = datetime.fromtimestamp(int(f.last_scan))
    else:
        last_scan = None

    if is_folder:
        print(f"folder: {domain}")
        print(f"    owner:           {f.owner}")
        print(f"    last modified:   {timestamp}")
    else:
        if "rescan" in cfg and cfg["rescan"]:
            f.run_scan()

        # report HDF objects (groups, datasets, and named datatypes) vs. allocated chunks
        num_objects = f.num_groups + f.num_datatypes + f.num_datasets
        if f.num_chunks > 0:
            num_chunks = f.num_chunks
        else:
            # older storeinfo format doesn't have num_chunks, so calculate
            num_chunks = f.num_objects - num_objects

        print(f"domain: {domain}")
        print(f"    owner:           {f.owner}")
        print(f"    id:              {f.id.id}")
        print(f"    last modified:   {timestamp}")
        if last_scan:
            print(f"    last scan:       {last_scan}")
        if f.md5_sum:
            print(f"    md5 sum:         {f.md5_sum}")
        print(f"    total_size:      {format_size(f.total_size)}")
        print(f"    allocated_bytes: {format_size(f.allocated_bytes)}")
        if f.metadata_bytes:
            print(f"    metadata_bytes:  {format_size(f.metadata_bytes)}")
        if f.linked_bytes:
            print(f"    linked_bytes:    {format_size(f.linked_bytes)}")
        print(f"    num objects:     {num_objects}")
        print(f"    num chunks:      {num_chunks}")
        if f.num_linked_chunks:
            print(f"    linked chunks:   {f.num_linked_chunks}")

    f.close()
#
# Main
#
def main():
    domains = []

    cfg.setitem("human_readable", False, flags=["-H", "--human-readable"], help="print human readable sizes (e.g. 123M)")
    cfg.setitem("rescan", False, flags=["--rescan",], help="refresh domain stats (for use when domain is provided)")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        domains = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    if not domains:
        abort("no domain provided!")

    for domain in domains:
        getDomainInfo(domain, cfg)


if __name__ == "__main__":
    main()
