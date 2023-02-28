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
