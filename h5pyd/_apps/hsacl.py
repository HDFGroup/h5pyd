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
        elif ioe.errno == 404:
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
def printUsage():
    print("")
    print("Usage: python hsacl.py [options] domain [+crudep] [-crudep] [userid1 userid2 ...]")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://example.com:8080")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     -h | --help    :: This message.")
    print("Arguments:")
    print(" domain :: Domain or Folder to be updated")
    print(" +/- :: add or remove permissions")
    print(" crudep :: permission flags: Create, Read, Update, Delete, rEadacl, uPdateacl")
    print("")
    print("examples...")
    print("list acls: python hsacl.py  /home/jill/myfile.h5")
    print("list ted's acl (if any): python hsacl.py  /home/jill/myfile.h5  ted")
    print("add/update acl to give ted read & update permissions: python hsacl +ru /home/jill/myfile.h5 ted")
    print("remove all permissions except read for jill: python hsacl -cudep /home/jill/myfile.h5 jill ")
    print("")
    sys.exit()

 
                  
def main():
    perm_abvr = {'c':'create', 'r': 'read', 'u': 'update', 'd': 'delete', 'e': 'readACL', 'p':'updateACL'} 
    fields = ('username', 'create', 'read', 'update', 'delete', 'readACL', 'updateACL')
    domain = None
    perm = None
    loglevel = logging.ERROR
    logfname = None
    usernames = []
    add_list = set()
    remove_list = set()
    if len(sys.argv) == 1 or sys.argv[1] == "-h":
        printUsage()

    # setup logging
    logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))

    cfg = Config()
         
    argn = 1 
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
        logging.debug("arg:", arg, "val:", val)
        
        if domain is None and arg in ("-v", "--verbose"):
            verbose = True
            argn += 1
        elif domain is None and arg == "--loglevel":
            val = val.upper()
            if val == "DEBUG":
                loglevel = logging.DEBUG
            elif val == "INFO":
                loglevel = logging.INFO
            elif val in ("WARN", "WARNING"):
                loglevel = logging.WARNING
            elif val == "ERROR":
                loglevel = logging.ERROR
            else:
                printUsage()  
            argn += 2
        elif domain is None and arg == '--logfile':
            logfname = val
            argn += 2  
        elif domain is None and arg in ("-h", "--help"):
            printUsage()
        elif domain is None and arg in ("-e", "--endpoint"):
            cfg["hs_endpoint"] = val
            argn += 2
        elif domain is None and arg in ("-u", "--username"):
            cfg["hs_username"] = val
            argn += 2
        elif domain is None and arg in ("-p", "--password"):
            cfg["hs_password"] = val
            argn += 2
        elif domain is None and arg[0] in ('-', '+'):
            print("No domain given")
            printUsage()
        elif domain is None:
            logging.debug("get domain")
            domain = arg
            if domain[0] != '/':
                print("Domain must start with '/'")
                printUsage()
            logging.debug("domain:", domain)
            argn += 1
        elif arg[0] == '+':
            logging.debug("got plus")
            if len(usernames) > 0:
                logging.debug("usernames:", usernames)
                printUsage()
            add_list = set(arg[1:])
            logging.info("add_list:", add_list)
            argn += 1

        elif arg[0] == '-':
            logging.debug("got minus")
            if len(usernames) > 0:
                printUsage()
            remove_list = set(arg[1:])
            logging.info("remove_list:", remove_list)
            argn += 1
        else:
            logging.info("got username:", arg)
            if arg.find('/') >= 0:
                print("Invalid username:", arg)
                printUsage()
            usernames.append(arg)
            argn += 1
          
    logging.info("domain:", domain)
    logging.info("add_list:", add_list)
    logging.info("remove_list:", remove_list)
    logging.info("usernames:", usernames)

    if len(usernames) == 0 and (add_list or remove_list):
        print("At least one username must be given to add/remove permissions")
        printUsage()
    
    if domain is None:
        print("no domain specified")
        sys.exit(1)    
    
    
    conflicts = list(add_list & remove_list)
    
    if len(conflicts) > 0:
        print("permission: ", conflicts[0], " permission flag set for both add and remove")
        sys.exit(1)

    mode = 'r'
    if add_list or remove_list:
        mode = 'a'  # we'll be updating the domain
        perm = {}
        for x in add_list:
            if x not in perm_abvr:
                print("Permission flag: {} is not valid - must be one of 'crudep;".format(x))
                sys.exit(1)
            perm_name = perm_abvr[x]
            perm[perm_name] = True
        for x in remove_list:
            if x not in perm_abvr:
                print("Permission flag: {} is not valid - must be one of 'crudep;".format(x))
                sys.exit(1)
            perm_name = perm_abvr[x]
            perm[perm_name] = False
        logging.info("perm:", perm)
            
    # open the domain or folder
    try:
        if domain[-1] == '/':
            f = h5pyd.Folder(domain, mode=mode, endpoint=cfg["hs_endpoint"], username=cfg["hs_username"], password=cfg["hs_password"])
        else:
            f = h5pyd.File(domain, mode=mode, endpoint=cfg["hs_endpoint"], username=cfg["hs_username"], password=cfg["hs_password"])
    except IOError as ioe:
        if ioe.errno in (404, 410):
            print("domain not found")
            sys.exit(1)
        elif ioe.errno in (401, 403):
            print("access is not authorized")
            sys.exit(1)
        else:
            print("Unexpected error:", ioe)
            sys.exit(1)

    # update/add ACL if permission flags have been set
    if perm:
        default_acl = getACL(f)  
        logging.info("default acl:", default_acl)
        update_names = usernames.copy()
        if not update_names:
            update_names.append("default")
         
        for username in update_names:
            # get user's ACL if it exist
            acl = getACL(f, username=username)
            if acl is None:
                acl = default_acl.copy()
            acl["userName"] = username
            logging.info("updating acl to: {}".format(acl))
            # mix in any permission changes
            for k in perm:
                acl[k] = perm[k]    
            try:
                f.putACL(acl)
            except IOError as ioe:
                if ioe.errno in (401, 403):
                    print("access is not authorized")
                else:
                    print("Unexpected error:", ioe)
                sys.exit(1)  
    #
    # read the acls
    # 
    if len(usernames) == 0:
        # no usernames, dump all ACLs
        try:
            acls = f.getACLs()
        except IOError as ioe:
            if ioe.errno == 403:
                print("User {} does not have permission to read ACL for this domain".format(cfg["hs_username"]))
                sys.exit(1)
            elif ioe.errno == 401:
                print("username/password needs to be provided")
                sys.exit(1)
            else:
                print("Unexpected error: {}".format(ioe))
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
                    print("User {} does not have permission to read ACL for this domain".format(cfg["hs_username"]))
                    sys.exit(1)
                elif ioe.errno == 401:
                    print("username/password needs to be provided")
                    sys.exit(1)
                elif ioe.errno == 404:
                    print(username, "<NONE>")
                else:
                    print("Unexpected error:", ioe)
                    sys.exit(1)

    f.close()  

if __name__ == "__main__":
    main()

    
	
