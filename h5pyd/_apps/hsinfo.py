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
import h5pyd

if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Get server status info
#

cfg = Config()


#
# Usage
#
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]")
    print("")
    print("Description:")
    print("    Get status information from server")
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


#
# getUpTime
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
        ret_str = f"{days} days, {hrs} hours {mins} min {sec} sec"
    elif hrs:
        ret_str = f"{hrs} hours {mins} min {sec} sec"
    elif mins:
        ret_str = f"{mins} min {sec} sec"
    else:
        ret_str = f"{sec} sec"

    return ret_str


def getServerInfo(cfg):
    """get server state and print"""
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    try:
        info = h5pyd.getServerInfo(
            username=username, password=password, endpoint=endpoint
        )
        info_name = info["name"]
        print(f"server name: {info_name}")
        if "state" in info:
            info_state = info["state"]
            print(f"server state: {info_state}")
        print(f"endpoint: {endpoint}")
        if "isadmin" in info and info["isadmin"]:
            admin_tag = "(admin)"
        else:
            admin_tag = ""

        info_username = info["username"]
        print(f"username: {info_username} {admin_tag}")
        info_password = info["password"]
        print(f"password: {info_password}")
        if info["state"] == "READY":
            try:
                home_folder = getHomeFolder()
                if home_folder:
                    print(f"home: {home_folder}")
            except IOError:
                print("home: NO ACCESS")

        if "hsds_version" in info:
            info_hsds_version = info["hsds_version"]
            print(f"server version: {info_hsds_version}")
        if "node_count" in info:
            info_node_count = info["node_count"]
            print(f"node count: {info_node_count}")
        if "start_time" in info:
            uptime = getUpTime(info["start_time"])
            print(f"up: {uptime}")
        print(f"h5pyd version: {h5pyd.version.version}")

    except IOError as ioe:
        if ioe.errno == 401:
            if username and password:
                print(f"username/password not valid for username: {username}")
            else:
                # authentication error with openid or app token
                print("authentication failure")
        else:
            print(f"Error: {ioe}")


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
                logging.info(f"find home folder - got ioe: {ioe}")
                continue
            except Exception as e:
                logging.warning(f"find home folder - got exception: {e}")
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

    if domains:
        sys.exit("Use the hsstat command to get information about about a folder or domain ")

    if not cfg["hs_endpoint"]:
        logging.error("endpoint not set")
        usage()

    getServerInfo(cfg)


if __name__ == "__main__":
    main()
