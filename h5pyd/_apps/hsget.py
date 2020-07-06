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

try:
    import h5py
    import h5pyd
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e))
    sys.exit(1)

if __name__ == "__main__":
    from config import Config
    from utillib import load_file
else:
    from .config import Config
    from .utillib import load_file

cfg = Config()  #  config object

#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    {} [ OPTIONS ]  domain filepath".format(cfg["cmd"])))
    print("")
    print("Description:")
    print("    Copy server domain to local HDF5 file")
    print("       domain: HDF Server domain (Unix or DNS style)")
    print("       filepath: HDF5 file to be created ")
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
    print("     --nodata :: Do not download dataset data")
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
def main():

    loglevel = logging.ERROR
    verbose = False
    dataload = "ingest"  # or None

    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["verbose"] = False

    endpoint=cfg["hs_endpoint"]
    username=cfg["hs_username"]
    password=cfg["hs_password"]
    bucket = cfg["hs_bucket"]
    logfname=None

    des_file = None
    src_domain = None
    argn = 1
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None

        if arg[0] == '-' and src_domain is not None:
            # options must be placed before filenames
            print("options must precead source files")
            usage()
            sys.exit(-1)
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
        if arg in ("-v", "--verbose"):
            verbose = True
            argn += 1
        elif arg == "--nodata":
            dataload = None
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
        elif arg in ("-b", "--bucket"):
            bucket = val
            argn += 2
        elif arg in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif arg in ("-e", "--endpoint"):
            endpoint = val
            argn += 2
        elif arg in ("-u", "--username"):
            username = val
            argn += 2
        elif arg in ("-p", "--password"):
            password = val
            argn += 2
        elif arg == '--cnf-eg':
            print_config_example()
            sys.exit(0)
        elif arg[0] == '-':
            usage()
            sys.exit(-1)
        elif src_domain is None:
            src_domain = arg
            argn += 1
        elif des_file is None:
            des_file = arg
            argn += 1
        else:
            usage()
            sys.exit(-1)

    # setup logging
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))

    # end arg parsing
    logging.info("username: {}".format(username))
    logging.info("password: {}".format(password))
    logging.info("endpoint: {}".format(endpoint))
    logging.info("verbose: {}".format(verbose))

    if src_domain is None or des_file is None:
        # need at least a src and destination
        usage()
        sys.exit(-1)

    logging.info("source domain: {}".format(src_domain))
    logging.info("target file: {}".format(des_file))


    if endpoint is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(endpoint))

    # get a handle to input domain
    try:
        fin = h5pyd.File(src_domain, mode='r', endpoint=endpoint, username=username, password=password, bucket=bucket, use_cache=True)
    except IOError as ioe:
        if ioe.errno == 403:
            logging.error("No read access to domain: {}".format(src_domain))
        elif ioe.errno == 404:
            logging.error("Domain: {} not found".format(src_domain))
        elif ioe.errno == 410:
            logging.error("Domain: {} has been recently deleted".format(src_domain))
        else:
            logging.error("Error opening domain {}: {}".format(src_domain, ioe))
        sys.exit(1)

    # create the output HDF5 file
    try:
        fout = h5py.File(des_file, 'w')
    except IOError as ioe:
        logging.error("Error creating file {}: {}".format(des_file, ioe))
        sys.exit(1)

    try:
        load_file(fin, fout, verbose=verbose, dataload=dataload)
        msg = "Domain {} downloaded to file: {}".format(src_domain, des_file)
        logging.info(msg)
        if verbose:
            print(msg)
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
#__main__
if __name__ == "__main__":
    main()
