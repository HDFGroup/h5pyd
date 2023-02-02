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
import time
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
# Usage
#
def printUsage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  domain")
    print(f"    {cmd} [ OPTIONS ]  folder")
    print("")
    print("Description:")
    print("    Get status information from server, or domain stats if domain is provided")
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
    sys.exit()

#
#
#
def getUpTime(start_time):
    now = int(time.time())
    sec = now - start_time
    days = sec // (24 * 60 * 60)
    sec -= 24 * 60 * 60 * days
    hrs = sec // (60 * 60)
    sec -= 60 * 60 * hrs
    mins = sec // 60
    sec -= 60 * mins
    if days:
        ret_str = "{} days, {} hours {} min {} sec".format(days, hrs, mins, sec)
    elif hrs:
        ret_str = "{} hours {} min {} sec".format(hrs, mins, sec)
    elif mins:
        ret_str = "{} min {} sec".format(mins, sec)
    else:
        ret_str = "{} sec".format(sec)

    return ret_str


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


def getServerInfo(cfg):
    """get server state and print"""
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    try:
        info = h5pyd.getServerInfo(
            username=username, password=password, endpoint=endpoint
        )
        print("server name: {}".format(info["name"]))
        if "state" in info:
            print("server state: {}".format(info["state"]))
        print(f"endpoint: {endpoint}")
        if "isadmin" in info and info["isadmin"]:
            admin_tag = "(admin)"
        else:
            admin_tag = ""

        print("username: {} {}".format(info["username"], admin_tag))
        print("password: {}".format(info["password"]))
        if info["state"] == "READY":
            try:
                home_folder = getHomeFolder()
                if home_folder:
                    print(f"home: {home_folder}")
            except IOError:
                print("home: NO ACCESS")

        if "hsds_version" in info:
            print("server version: {}".format(info["hsds_version"]))
        if "node_count" in info:
            print("node count: {}".format(info["node_count"]))
        elif "h5serv_version" in info:
            print("server version: {}".format(info["h5serv_version"]))
        if "start_time" in info:
            uptime = getUpTime(info["start_time"])
            print(f"up: {uptime}")
        print("h5pyd version: {}".format(h5pyd.version.version))

    except IOError as ioe:
        if ioe.errno == 401:
            if username and password:
                print(f"username/password not valid for username: {username}")
            else:
                # authentication error with openid or app token
                print("authentication failure")
        else:
            print(f"Error: {ioe}")


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
            sys.exit("domain: {} not found".format(domain))
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        else:
            sys.exit("Unexpected error: {}".format(oe))

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
# Get folder in /home/ that is owned by given user
#
def getHomeFolder():
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    if not username:
        return None
    dir = h5pyd.Folder(
        "/home/", username=username, password=password, endpoint=endpoint
    )  # get folder object for root
    homefolder = None
    for name in dir:
        # we should come across the given domain
        if username.startswith(name):
            # check any folders where the name matches at least part of the username
            # e.g. folder: "/home/bob/" for username "bob@acme.com"
            path = "/home/" + name + "/"
            try:
                f = h5pyd.Folder(
                    path, username=username, password=password, endpoint=endpoint
                )
            except IOError as ioe:
                logging.info("find home folder - got ioe: {}".format(ioe))
                continue
            except Exception as e:
                logging.warn("find home folder - got exception: {}".format(e))
                continue
            if f.owner == username:
                homefolder = path
            f.close()
            if homefolder:
                break

    dir.close()
    return homefolder


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
        printUsage()

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    if not domains:
        if not cfg["hs_endpoint"]:
            logging.error("endpoint not set")
            printUsage()
        getServerInfo(cfg)
    else:
        for domain in domains:
            getDomainInfo(domain, cfg)


if __name__ == "__main__":
    main()
