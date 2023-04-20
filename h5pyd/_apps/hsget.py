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
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  domain [ filepath ]")
    print("")
    print("Description:")
    print("    Copy server domain to local HDF5 file")
    print("       domain: domain to be copied")
    print("       filepath: HDF5 file to be created ")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("Examples:")
    print(f"     {cmd} /shared/tall.h5 tall.h5")
    print(f"     {cmd} hdf5://shared/tall.h5 tall.h5")
    print(f"     {cmd} hdf5://shared/tall.h5  # creates local file 'tall.h5'")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()

#end print_usage


#----------------------------------------------------------------------------------
def main():
    
    cfg.setitem("no_clobber", False, flags=["-n", "--no-clobber"],  help="do not overwrite target")
    cfg.setitem("nodata", False, flags=["--nodata",], help="do not copy dataset data")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        cmdline_values = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(cmdline_values) < 1:
        usage()

    src_domain = cmdline_values[0]
    if len(cmdline_values) > 1:
        des_file = cmdline_values[1]
    else:
        # use domain base name as file
        parts = src_domain.split('/')
        des_file = parts[-1]

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")     

    logging.info(f"source domain: {src_domain}")
    logging.info(f"target file: {des_file}")

    # get a handle to input domain
    kwargs = {}
    kwargs["endpoint"] = cfg["hs_endpoint"]
    kwargs["username"] = cfg["hs_username"]
    kwargs["password"] = cfg["hs_password"]
    kwargs["bucket"] = cfg["hs_bucket"]
    try:
        fin = h5pyd.File(src_domain, mode='r', **kwargs)
    except IOError as ioe:
        if ioe.errno == 403:
            logging.error(f"No read access to domain: {src_domain}")
        elif ioe.errno == 404:
            logging.error(f"Domain: {src_domain} not found")
        elif ioe.errno == 410:
            logging.error(f"Domain: {src_domain} has been recently deleted")
        else:
            logging.error(f"Error opening domain {src_domain}: {ioe}")
        sys.exit(1)

    # create the output HDF5 file
    mode = "x" if cfg["no_clobber"] else "w"
    try:
        fout = h5py.File(des_file, mode)
    except IOError as ioe:
        logging.error(f"Error creating file {des_file}: {ioe}")
        sys.exit(1)

    try:
        kwargs = {}
        kwargs["verbose"] = cfg["verbose"]
        kwargs["ignore_error"] = cfg["ignore_error"]
        kwargs["dataload"] = None if cfg["nodata"] else "ingest"
        load_file(fin, fout, **kwargs)
        cfg.print(f"Domain {src_domain} downloaded to file: {des_file}")
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
#__main__
if __name__ == "__main__":
    main()
