##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
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
else:
    from .config import Config

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
# get given ACL, return None if not found
#
def getACL(f, username="default"):
    try:
       acl = f.getACL(username)
    except IOError as ioe:
        if ioe.errno == 403:
            print("No permission to read ACL for this domain")
            sys.exit(1)
        elif ioe.errno == 401:
            print("username/password needs to be provided")
            sys.exit(1)
        elif ioe.errno == 404 or not ioe.errno:
            return None
        else:
            print("unexpected error: {}".format(ioe))
            sys.exit(1)
    if acl and "domain" in acl:
        # remove the domain key
        del acl["domain"]
    return acl

#
# Usage
#
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:")
    print("")
    print(f"    {cmd} [options] domain [+crudep] [-crudep] [userid1 userid2 ...]")
    print("")
    print("Description:")
    print("    Display, add, or change ACLs for a domain or folder")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")

    print("Arguments:")
    print("    domain :: Domain or Folder to be updated")
    print("    +/- :: add or remove permissions")
    print("    crudep :: permission flags: Create, Read, Update, Delete, rEadacl, uPdateacl")
    msg = "    userid1, userid2, etc.: list of usernames, group names (group names distinguished "
    msg += "by 'g:' prefix) or 'default' to set permissions for those not otherwise listed"
    print(msg)
    print("")
    print("Examples:")
    print(f"    list acls: {cmd} /home/jill/myfile.h5")
    print(f"    list ted's acl (if any): {cmd} /home/jill/myfile.h5  ted")
    print(f"    add/update acl to give ted read & update permissions: {cmd} /home/jill/myfile.h5 +ru ted")
    print(f"    remove all permissions except read for jill: {cmd} /home/jill/myfile.h5 -cudep jill")
    print(f"    enable create, update, and read ACL for devs group: {cmd} /shared/datafile.h5 +cup g:devs")
    print(f"    enable domain and ACLs to be read by anyone: {cmd} /home/jill/myfile.h5 +re default")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()



def main():
    perm_abvr = {'c':'create', 'r': 'read', 'u': 'update', 'd': 'delete', 'e': 'readACL', 'p':'updateACL'}
    fields = ('username', 'create', 'read', 'update', 'delete', 'readACL', 'updateACL')
    domain = None
    perm = None
    usernames = []
    add_list = set()
    remove_list = set()

    # additional options
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    
    try:
        cmdline_args = cfg.set_cmd_flags(sys.argv[1:], allow_post_flags=True)
    except ValueError as ve:
        print(ve)
        usage()
    
    if len(cmdline_args) == 0:
        # need a domain
        usage()

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    for arg in cmdline_args:
        if domain is None:
            domain = arg
        elif arg[0] == '+':
            if len(usernames) > 0:
                abort("no usernames given!")
            add_list = set(arg[1:])
        elif arg[0] == '-':
            if len(usernames) > 0:
                abort("remove flags must be placed before usernames!")
            remove_list = set(arg[1:])
        else:
            if arg.find('/') >= 0:
                abort(f"invalid username: {arg}")
            usernames.append(arg)

    logging.info(f"domain: {domain}")
    logging.info(f"add_list: {add_list}")
    logging.info(f"remove_list: {remove_list}")
    logging.info(f"usernames: {usernames}")

    if len(usernames) == 0 and (add_list or remove_list):
        abort("at least one username must be given to add/remove permissions")

    if domain is None:
        abort("no domain specified")

    conflicts = list(add_list & remove_list)

    if len(conflicts) > 0:
        abort(f"permission: {conflicts[0]} flag set for both add and remove")

    mode = 'r'
    if add_list or remove_list:
        mode = 'a'  # we'll be updating the domain
        perm = {}
        for x in add_list:
            if x not in perm_abvr:
                abort("Permission flag: {x} is not valid - must be one of 'crudep'")
            perm_name = perm_abvr[x]
            perm[perm_name] = True
        for x in remove_list:
            if x not in perm_abvr:
                abort(f"Permission flag: {x} is not valid - must be one of 'crudep'")
            perm_name = perm_abvr[x]
            perm[perm_name] = False
        logging.info("perm:", perm)

    # open the domain or folder
    try:
        if domain[-1] == '/':
            f = h5pyd.Folder(domain, mode=mode, endpoint=cfg["hs_endpoint"], username=cfg["hs_username"], password=cfg["hs_password"], bucket=cfg["hs_bucket"])
        else:
            f = h5pyd.File(domain, mode=mode, endpoint=cfg["hs_endpoint"], username=cfg["hs_username"], password=cfg["hs_password"], bucket=cfg["hs_bucket"])
    except IOError as ioe:
        if ioe.errno in (404, 410):
            abort("domain not found")
        elif ioe.errno in (401, 403):
            abort("access is not authorized")
        else:
            abort(f"Unexpected error: {ioe}")

    # update/add ACL if permission flags have been set
    if perm:
        default_acl = {'updateACL': False,
                       'delete': False,
                       'create': False,
                       'read': False,
                       'update': False,
                       'readACL': False,
                       'userName': 'default'
                       }
        # note: list.copy not supported in py2.7, copy by hand for now
        # update_names = usernames.copy()
        update_names = []
        for username in usernames:
            update_names.append(username)

        if not update_names:
            update_names.append("default")

        for username in update_names:
            # get user's ACL if it exist
            acl = getACL(f, username=username)
            if acl is None:
                acl = default_acl.copy()
            acl["userName"] = username
            logging.info(f"updating acl to: {acl}")
            # mix in any permission changes
            for k in perm:
                acl[k] = perm[k]
            try:
                f.putACL(acl)
            except IOError as ioe:
                if ioe.errno in (401, 403):
                    abort("access is not authorized")
                else:
                    abort("Unexpected error:", ioe)
    #
    # read the acls
    #
    if len(usernames) == 0:
        # no usernames, dump all ACLs
        try:
            acls = f.getACLs()
        except IOError as ioe:
            if ioe.errno == 403:
                username = cfg["hs_username"]
                abort(f"User: {username} does not have permission to read ACL for this domain")
            elif ioe.errno == 401:
                abort("username/password needs to be provided")
            else:
                abort(f"Unexpected error: {ioe}")
        print("%015s   %08s  %08s  %08s  %08s  %08s  %08s " % fields)
        print("-"*80)
        for acl in acls:
            vals = (acl["userName"], acl["create"], acl["read"], acl["update"], acl["delete"], acl["readACL"], acl["updateACL"])
            print("%015s   %08s  %08s  %08s  %08s  %08s  %08s " % vals)
    else:
        header_printed = False  # don't print header until we have at least one ACL
        for username in usernames:
            try:
                acl = f.getACL(username)
                if not header_printed:
                    print("%015s   %08s  %08s  %08s  %08s  %08s  %08s " % fields)
                    print("-"*80)
                    header_printed = True
                vals = (acl["userName"], acl["create"], acl["read"], acl["update"], acl["delete"], acl["readACL"], acl["updateACL"])
                print("%015s   %08s  %08s  %08s  %08s  %08s  %08s " % vals)
            except IOError as ioe:
                if ioe.errno == 403:
                    this_user = cfg["hs_username"]
                    abort(f"User {this_user} does not have permission to read ACL for this domain")
                elif ioe.errno == 401:
                    abort("username/password needs to be provided")
                elif ioe.errno == 404:
                    abort(f"{username} not found")
                else:
                    abort(f"Unexpected error: {ioe}")

    f.close()

if __name__ == "__main__":
    main()
