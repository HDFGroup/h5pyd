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

try:
    import h5pyd
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e))
    sys.exit(1)


if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

cfg = Config()



#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    {} [ OPTIONS ]  source_domain  des_domain".format(cfg["cmd"])))
    print(("    {} [ OPTIONS ]  source_domain  folder".format(cfg["cmd"])))
    print("")
    print("Description:")
    print("    Move domain from one location to another")
    print("       source_domain: domain to be moved ")
    print("       des_domain: desttnation domain")
    print("       folder: destination folder (Unix style ending in '/')")
    print("")
    print("Example:")
    print("     hsmv /home/myfolder/file1.h5  /home/myfolder/file2.h5")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     -h | --help    :: This message.")
    print("")
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
    fh = h5pyd.File(domain, mode='r', endpoint=endpoint, username=username,
                   password=password, bucket=bucket, use_cache=True)
    return fh

def createFile(domain, linked_domain=None):
    #print("createFile", domain)
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    owner = None
    if "hs_owner" in cfg:
        owner=cfg["hs_owner"]
    fh = h5pyd.File(domain, mode='x', endpoint=endpoint, username=username, password=password, bucket=bucket, owner=owner, linked_domain=linked_domain)
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
            sys.exit("Parent domain: {} not found".format(parent_domain))
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        else:
            sys.exit("Unexpected error: {}".format(oe))

    if base_name not in hparent:
        # note - this may happen if the domain was recently created and not
        # yet synced to S3
        sys.exit("domain: {} not found".format(domain))

    # delete the domain
    hparent.delete_item(base_name, keep_root=keep_root)
    if cfg["verbose"]:
        if domain.endswith('/'):
            print("Folder: {} deleted".format(domain))
        else:
            print("Domain: {} deleted".format(domain))

def main():

    loglevel = logging.ERROR
    verbose = False
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["logfname"] = None
    logfname=None

    src_files = []
    argn = 1
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None

        if arg[0] == '-' and len(src_files) > 0:
            # options must be placed before filenames
            print("options must precead source files")
            usage()
            sys.exit(-1)
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
        if arg in ("-v", "--verbose"):
            verbose = True
            argn += 1
        elif arg == "--loglevel":
            if val == "debug":
                loglevel = logging.DEBUG
            elif val == "info":
                loglevel = logging.INFO
            elif val == "warning":
                loglevel = logging.WARNING
            elif val == "error":
                loglevel = logging.ERROR
            else:
                print("unknown loglevel")
                usage()
                sys.exit(-1)
            argn += 2
        elif arg == '--logfile':
            logfname = val
            argn += 2
        elif arg in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif arg in ("-e", "--endpoint"):
            cfg["hs_endpoint"] = val
            argn += 2
        elif arg in ("-u", "--username"):
            cfg["hs_username"] = val
            argn += 2
        elif arg in ("-p", "--password"):
            cfg["hs_password"] = val
            argn += 2
        elif arg in ("-b", "--bucket"):
            cfg["hs_bucket"] = val
            argn += 2
        elif arg == '--cnf-eg':
            print_config_example()
            sys.exit(0)
        elif arg[0] == '-':
            usage()
            sys.exit(-1)
        else:
            src_files.append(arg)
            argn += 1

    # setup logging
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(filename)s:%(lineno)d %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))

    # end arg parsing
    logging.info("username: {}".format(cfg["hs_username"]))
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))
    logging.info("verbose: {}".format(verbose))

    if len(src_files) < 2:
        # need at least a src and destination
        usage()
        sys.exit(-1)
    src_domain = src_files[0]
    des_domain = src_files[1]

    if src_domain[0] != '/' or des_domain[0] != '/':
        print("absolute paths must be used")
        usage()
        sys.exit(-1)

    if src_domain[-1] == '/':
        print("folder can not be used for src domain")

    if des_domain[-1] == '/':
        # add on the filename from source to folder path
        des_domain += op.basename(src_domain)


    logging.info("source domain: {}".format(src_domain))
    logging.info("target domain: {}".format(des_domain))


    if cfg["hs_endpoint"] is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))

    # get root id of source file
    try:
        fin = getFile(src_domain, "r")
    except IOError as oe:
        # this will fail if we try to open a folder
        msg = "Error: {} getting domain: {}".format(oe.errno, src_domain)
        logging.error(msg)
        print(msg)
        sys.exit(str(oe))

    logging.info("src root id: {}".format(fin.id.id))
    fin.close()

    # create a new file using the src domain for the root group
    try:
        fout = createFile(des_domain, linked_domain=src_domain)
    except IOError as oe:
        msg = "Error: {} creating domain: {}".format(oe.errno, des_domain)
        logging.error(msg)
        print(msg)
        sys.exit(str(oe))

    logging.info("des root id: {}".format(fout.id.id))

    try:
        deleteDomain(src_domain, keep_root=True)
        logging.info("domain: {} removed".format(src_domain))
    except IOError as oe:
        msg = "Error: {} removing source domain: {}".format(oe.errno, src_domain)
        logging.error(msg)
        print(msg)
        sys.exit(str(oe))


# __main__
if __name__ == "__main__":
    main()
