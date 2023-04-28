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

import os
import sys
import logging
import os.path as op

try:
    import h5py
    import h5pyd
except ImportError as e:
    sys.stderr.write(f"ERROR : {e} : install it to use this utility...\n")
    sys.exit(1)

try:
    import s3fs

    S3FS_IMPORT = True
except ImportError:
    S3FS_IMPORT = False

if __name__ == "__main__":
    from config import Config
    from utillib import load_file
else:
    from .config import Config
    from .utillib import load_file

from urllib.parse import urlparse

cfg = Config()

# ----------------------------------------------------------------------------------
def abort(msg):
    logging.error(msg)
    if cfg["logfile"]:
        sys.stderr.write(msg + "\n")
    logging.error("exiting program with return code -1")
    sys.exit(-1)

# ----------------------------------------------------------------------------------
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  sourcefile  domain")
    print(f"    {cmd} [ OPTIONS ]  sourcefile_1, sourcefile_2,...  folder")
    print("")
    print("Description:")
    print("    Copy HDF5 file to domain or multiple files to a domain folder")
    print("       sourcefile: HDF5 file to be copied ")
    print("       domain: HSDS domain (absolute path with or without hdf5:// prefix)")
    print("       folder: HSDS folder (path as above ending in '/')")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("Note about --link option:")
    print("    --link enables just the source HDF5 metadata to be ingested while the dataset data")
    print("     is left in the original file and fetched as needed.")
    print("     When used with files stored in AWS S3, the source file can be specified using the S3")
    print("     path: 's3://<bucket_name>/<s3_path>'.  Preferably, the bucket should be in the same")
    print("     region as the HSDS service")
    print("     For Posix or Azure deployments, the source file needs to be copied to a regular file,")
    print("     and the --linkpath option should be used to specifiy the Azure container name and path, or ")
    print("     (for HSDS deployed with POSIX) the file path relative to the server ROOT_DIR")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit(-1)
     


# end print_usage


# ----------------------------------------------------------------------------------
def main():

    COMPRESSION_FILTERS = ("blosclz", "lz4", "lz4hc", "snappy", "gzip", "zstd")

    s3 = None # S3FS instance

    cfg.setitem("append", False, flags=["-a", "--append"], help="append to existing domain")
    cfg.setitem("extend_dim", None, flags=["--extend",], choices=["DIMSCALE",], help="extend along given dimensionscale")
    cfg.setitem("extend_offset", None, flags=["--extend-offset"], choices=["N",], help="write data at index n along extended dimension")
    cfg.setitem("no_clobber", False, flags=["-n", "--no-clobber"],  help="do not overwrite target")
    cfg.setitem("nodata", False, flags=["--nodata",], help="do not copy dataset data")
    cfg.setitem("z", None, flags=["-z",], choices=["N",], help="apply compression filter to any non-compressed datasets, n: [0-9]")
    cfg.setitem("link", None, flags=["--link",],  help="Link to dataset data (sourcefile given as <bucket>/<path>) or s3uri")
    cfg.setitem("linkpath", None, flags=["--linkpath",], choices=["PATH_URI",], help="Use the given URI for the link references rather than the src path")
    cfg.setitem("compression", None, flags=["--compression",], choices=COMPRESSION_FILTERS, help="use the given compression algorithm for -z option (lz4 is default)")
    cfg.setitem("ignorefilters", False, flags=["--ignore-filters"], help="ignore any filters used by source dataset")
    cfg.setitem("retries", 3, flags=["--retries",], choices=["N",], help="Set number of server retry attempts")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        cmdline_values = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(cmdline_values) < 2:
        usage()

    domain = cmdline_values[-1]
    src_files = cmdline_values[:-1]

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}") 

    if cfg["linkpath"] and not cfg["link"]:
        abort("--linkpath option can only be used with --link")

    if cfg["extend_offset"] and cfg["extend_dim"] is None:
        abort("--extend-offset option can only be used with --link")

    if cfg["extend_dim"] and cfg["link"]:
        abort("--extend option can't be used with --link")

    if cfg["nodata"] and cfg["link"]:
        abort("--nodata option can't  be used with --link")

    if cfg["link"]:
        dataload = "link"
    elif cfg["nodata"]:
        dataload = None
    else:
        dataload = "ingest"

    
    logging.info(f"source files: {src_files}")
    logging.info(f"target domain: {domain}")
    if len(src_files) > 1 and domain[-1] != "/":
        abort("target must be a folder if multiple source files are provided")

    # check we have min HDF5 lib version for link option
    if cfg["link"]:
        logging.info("checking libversion")

        if (
            h5py.version.version_tuple.major == 2
            and h5py.version.version_tuple.minor < 10
        ):
            abort("link option requires h5py version 2.10 or higher")
            
        if h5py.version.hdf5_version_tuple < (1, 10, 6):
            abort("link option requires h5py version 2.10 or higher")
        

    try:

        for src_file in src_files:
            # check if this is a non local file, if it is remote (http, etc...) stage it first then insert it into hsds
            src_file_chk = urlparse(src_file)
            logging.debug(src_file_chk)

            tgt = domain
            if tgt[-1] == "/":
                # folder destination
                tgt = tgt + op.basename(src_file)

            # get a handle to input file
            if src_file.startswith("s3://"):
                s3path = src_file
                if not S3FS_IMPORT:
                    abort("Install S3FS package to load s3 files")
                    sys.exit(1)

                if not s3:
                    key = os.environ.get("AWS_ACCESS_KEY_ID")
                    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
                    aws_s3_gateway = os.environ.get("AWS_GATEWAY")
                    client_kwargs = {}
                    if aws_s3_gateway:
                        client_kwargs["endpoint_url"] = aws_s3_gateway

                    s3 = s3fs.S3FileSystem(use_ssl=False, key=key, secret=secret, client_kwargs=client_kwargs)
                try:
                    fin = h5py.File(s3.open(src_file, "rb"), moe="r")
                except IOError as ioe:
                    abort(f"Error opening file {src_file}: {ioe}")
                    
            else:
                if cfg["link"]:
                    if op.isabs(src_file) and not cfg["linkpath"]:
                        msg = "source file must be s3path (for HSDS using S3 storage) or relative path from server "
                        msg += "root directory (for HSDS using posix storage)"
                        abort(msg)
                    s3path = src_file
                else:
                    s3path = None
                try:
                    fin = h5py.File(src_file, mode="r")
                except IOError as ioe:
                    abort(f"Error opening file {src_file}: {ioe}")

            # create the output domain
            try:
                if cfg["append"]:
                    mode = "a"
                elif cfg["no_clobber"]:
                    mode = "x"
                else:
                    mode = "w"
                kwargs = {
                    "username": cfg["hs_username"],
                    "password": cfg["hs_password"],
                    "endpoint": cfg["hs_endpoint"],
                    "bucket": cfg["hs_bucket"],
                    "mode": mode,
                    "retries": cfg["retries"],
                }

                fout = h5pyd.File(tgt, **kwargs)
            except IOError as ioe:
                if ioe.errno == 404:
                    abort(f"Domain: {tgt} not found")
                elif ioe.errno == 403:
                    abort(f"No write access to domain: {tgt}")
                else:
                    abort(f"Error creating file {tgt}: {ioe}")

            if cfg["linkpath"]:
                # now that we have a handle to the source file,
                # repurpose s3path to the s3uri that will actually get stored 
                # in the target domain
                s3path = cfg["linkpath"]

            if cfg["no_clobber"]:
                if cfg["append"]: 
                    # no need to check for clobber if not in append mode
                    no_clobber = True
                else:
                    no_clobber = False
            else:
                no_clobber = False

            if cfg["compression"]:
                compression = cfg["compression"]
            else:
                compression = None

            if cfg["z"]:
                try:
                    compression_opts = int(cfg["z"])
                    if compression is None:
                        # if no other comressor is specified, just use gzip
                        compression = "gzip"

                except ValueError:
                    # not a numeric option?  Just pass the string
                    compression_opts = cfg["z"]
            else:
                compression_opts = None


            # do the actual load
            kwargs = {
                "verbose": cfg["verbose"],
                "dataload": dataload,
                "s3path": s3path,
                "compression": compression,
                "compression_opts": compression_opts,
                "ignorefilters": cfg["ignorefilters"],
                "append": cfg["append"],
                "extend_dim": cfg["extend_dim"],
                "extend_offset": cfg["extend_offset"],
                "ignore_error": cfg["ignore_error"],
                "no_clobber": no_clobber
                            }
            load_file(fin, fout, **kwargs)

            msg = f"File {src_file} uploaded to domain: {tgt}"
            logging.info(msg)
            if cfg["verbose"]:
                print(msg)

    except KeyboardInterrupt:
        abort("Aborted by user via keyboard interrupt.")


# __main__
if __name__ == "__main__":
    main()
