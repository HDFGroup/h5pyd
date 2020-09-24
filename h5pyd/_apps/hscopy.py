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
    import h5pyd
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e))
    sys.exit(1)

try:
    import pycurl as PYCRUL
except ImportError:
    PYCRUL = None

if __name__ == "__main__":
    from config import Config
    from utillib import load_file
else:
    from .config import Config
    from .utillib import load_file


cfg = Config()


#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    {} [ OPTIONS ]  source  destination".format(cfg["cmd"])))
    print("")
    print("Description:")
    print("    Copy domain")
    print("       source: domain to be copied ")
    print("       destination: target domain")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     -z[n] :: apply compression filter to any non-compressed datasets, n: [0-9]")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     --nodata :: Do not upload dataset data")
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
    dataload  = "ingest"
    deflate = None
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
        elif arg.startswith("-z"):
            compressLevel = 4
            if len(arg) > 2:
                try:
                    compressLevel = int(arg[2:])
                except ValueError:
                    print("Compression Level must be int between 0 and 9")
                    sys.exit(-1)
            deflate = compressLevel
            argn += 1
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

    logging.info("source domain: {}".format(src_domain))
    logging.info("target domain: {}".format(des_domain))
    if src_domain[0] != '/' or src_domain[-1] == '/':
        print("source domain must be an absolute path, non-folder domain")
        usage()
        sys.exit(-1)

    if des_domain[0] != '/' or des_domain[-1] == '/':
        print("source domain must be an absolute path, non-folder domain")
        usage()
        sys.exit(-1)


    if cfg["hs_endpoint"] is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))

    try:
        username = cfg["hs_username"]
        password = cfg["hs_password"]
        endpoint = cfg["hs_endpoint"]
        bucket = cfg["hs_bucket"]
        # get a handle to input file
        try:
            fin = h5pyd.File(src_domain, mode='r', endpoint=endpoint, username=username, password=password, bucket=bucket)
        except IOError as ioe:
            logging.error("Error opening file {}: {}".format(src_domain, ioe))
            sys.exit(1)

        # create the output domain
        try:

            fout = h5pyd.File(des_domain, 'x', endpoint=endpoint, username=username, password=password, bucket=bucket)
        except IOError as ioe:
            if ioe.errno == 403:
                logging.error("No write access to domain: {}".format(des_domain))
            else:
                logging.error("Error creating file {}: {}".format(des_domain, ioe))
            sys.exit(1)


        # do the actual load
        load_file(fin, fout, verbose=verbose, dataload=dataload, deflate=deflate)

        msg = "File {} uploaded to domain: {}".format(src_domain, des_domain)
        logging.info(msg)
        if verbose:
            print(msg)

    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)


# __main__
if __name__ == "__main__":
    main()
