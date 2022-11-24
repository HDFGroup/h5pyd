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


# ----------------------------------------------------------------------------------
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
    print(
        "     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org"
    )
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     --src_endpoint <domain> :: The HDF Server endpoint for src file")
    print("     --src_user <username>   :: User name credential for src file")
    print("     --src_password <password> :: Password credential for src file")
    print("     --des_endpoint <domain> :: The HDF Server endpoint for des file")
    print("     --des_user <username>   :: User name credential for des file")
    print("     --des_password <password> :: Password credential for des file")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print(
        "     -z[n] :: apply compression filter to any non-compressed datasets, n: [0-9]"
    )
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     --src_bucket <bucket_name> :: Storage bucket for src file")
    print("     --des_bucket <bucket_name> :: Storage bucket for des file")
    print("     --nodata :: Do not upload dataset data")
    print("     --ignore :: Don't exit on error")
    print("     -h | --help    :: This message.")
    print("")


# end print_usage

# ----------------------------------------------------------------------------------
def print_config_example():
    print("# default")
    print("hs_username = <username>")
    print("hs_password = <passwd>")
    print("hs_endpoint = http://hsdshdflab.hdfgroup.org")


# print_config_example

# ----------------------------------------------------------------------------------
def main():

    loglevel = logging.ERROR
    verbose = False
    ignore_error = False
    dataload = "ingest"
    compressLevel = None
    cfg["cmd"] = sys.argv[0].split("/")[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["logfname"] = None
    logfname = None

    src_files = []
    argn = 1
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None

        if arg[0] == "-" and len(src_files) > 0:
            # options must be placed before filenames
            print("options must precead source files")
            usage()
            sys.exit(-1)
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn + 1]
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
        elif arg == "--logfile":
            logfname = val
            argn += 2
        elif arg in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif arg in ("-e", "--endpoint"):
            cfg["hs_endpoint"] = val
            argn += 2
        elif arg == "--src_endpoint":
            cfg["src_hs_endpoint"] = val
            argn += 2
        elif arg == "--des_endpoint":
            cfg["des_hs_endpoint"] = val
            argn += 2
        elif arg in ("-u", "--username"):
            cfg["hs_username"] = val
            argn += 2
        elif arg == "--src_username":
            cfg["src_hs_username"] = val
            argn += 2
        elif arg == "--des_username":
            cfg["des_hs_username"] = val
            argn += 2
        elif arg in ("-p", "--password"):
            cfg["hs_password"] = val
            argn += 2
        elif arg == "--src_password":
            cfg["src_hs_password"] = val
            argn += 2
        elif arg == "--des_password":
            cfg["des_hs_password"] = val
            argn += 2
        elif arg in ("-b", "--bucket"):
            cfg["hs_bucket"] = val
            argn += 2
        elif arg == "--src_bucket":
            cfg["src_hs_bucket"] = val
            argn += 2
        elif arg == "--des_bucket":
            cfg["des_hs_bucket"] = val
            argn += 2
        elif arg == "--cnf-eg":
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
            argn += 1
        elif arg == "--ignore":
            ignore_error = True
            argn += 1
        elif arg[0] == "-":
            print("got unknown arg:", arg)
            usage()
            sys.exit(-1)
        else:
            src_files.append(arg)
            argn += 1

    # setup logging
    logging.basicConfig(
        filename=logfname,
        format="%(levelname)s %(asctime)s %(filename)s:%(lineno)d %(message)s",
        level=loglevel,
    )
    logging.debug("set log_level to {}".format(loglevel))

    # end arg parsing
    logging.info("verbose: {}".format(verbose))

    if len(src_files) < 2:
        # need at least a src and destination
        usage()
        sys.exit(-1)
    src_domain = src_files[0]
    des_domain = src_files[1]

    logging.info("source domain: {}".format(src_domain))
    logging.info("target domain: {}".format(des_domain))

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
        msg = "target domain can't be a folder"
        logging.error(msg)
        sys.exit(msg)

    if cfg["hs_endpoint"]:
        logging.info("endpoint: {}".format(cfg["hs_endpoint"]))
    elif cfg["src_hs_endpoint"] and cfg["des_hs_endpoint"]:
        logging.info("src_endpoint: {}".format(cfg["src_hs_endpoint"]))
        logging.info("des_endpoint: {}".format(cfg["des_hs_endpoint"]))
    else:
        logging.error("No endpoint given, try -h for help\n")
        sys.exit(1)

    try:
        if cfg["src_hs_username"]:
            username = cfg["src_hs_username"]
        else:
            username = cfg["hs_username"]
        if cfg["src_hs_password"]:
            password = cfg["src_hs_password"]
        else:
            password = cfg["hs_password"]
        if cfg["src_hs_endpoint"]:
            endpoint = cfg["src_hs_endpoint"]
        else:
            endpoint = cfg["hs_endpoint"]
        if cfg["src_hs_bucket"]:
            bucket = cfg["src_hs_bucket"]
        else:
            bucket = cfg["hs_bucket"]
        # get a handle to input file
        try:
            fin = h5pyd.File(
                src_domain,
                mode="r",
                endpoint=endpoint,
                username=username,
                password=password,
                bucket=bucket,
            )
        except IOError as ioe:
            logging.error("Error opening file {}: {}".format(src_domain, ioe))
            sys.exit(1)

        # create the output domain
        if cfg["des_hs_username"]:
            username = cfg["des_hs_username"]
        else:
            username = cfg["hs_username"]
        if cfg["des_hs_password"]:
            password = cfg["des_hs_password"]
        else:
            password = cfg["hs_password"]
        if cfg["des_hs_endpoint"]:
            endpoint = cfg["des_hs_endpoint"]
        else:
            endpoint = cfg["hs_endpoint"]
        if cfg["des_hs_bucket"]:
            bucket = cfg["des_hs_bucket"]
        else:
            bucket = cfg["hs_bucket"]
        try:
            fout = h5pyd.File(
                des_domain,
                "x",
                endpoint=endpoint,
                username=username,
                password=password,
                bucket=bucket,
            )
        except IOError as ioe:
            if ioe.errno == 403:
                logging.error("No write access to domain: {}".format(des_domain))
            else:
                logging.error("Error creating file {}: {}".format(des_domain, ioe))
            sys.exit(1)

        if compressLevel is not None:
            compress_filter = "deflate"  # TBD - add option for other compressors
        else:
            compress_filter = None

        # do the actual load
        load_file(
            fin,
            fout,
            verbose=verbose,
            ignore_error=ignore_error,
            dataload=dataload,
            compression=compress_filter,
            compression_opts=compressLevel,
        )

        msg = "File {} uploaded to domain: {}".format(src_domain, des_domain)
        logging.info(msg)
        if verbose:
            print(msg)

    except KeyboardInterrupt:
        logging.error("Aborted by user via keyboard interrupt.")
        sys.exit(1)


# __main__
if __name__ == "__main__":
    main()
