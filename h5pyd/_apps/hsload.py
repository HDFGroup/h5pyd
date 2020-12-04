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
    import h5py
    import h5pyd
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e))
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

if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

cfg = Config()


#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    {} [ OPTIONS ]  sourcefile  domain".format(cfg["cmd"])))
    print(("    {} [ OPTIONS ]  sourcefile  folder".format(cfg["cmd"])))
    print("")
    print("Description:")
    print("    Copy HDF5 file to Domain or multiple files to a Domain folder")
    print("       sourcefile: HDF5 file to be copied ")
    print("       domain: HDF Server domain (Unix or DNS style)")
    print("       folder: HDF Server folder (Unix style ending in '/')")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -a | --append <mode>  :: Flag to append to an existing HDF Server domain")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     -z[n] :: apply compression filter to any non-compressed datasets, n: [0-9]")
    print("     --compression blosclz|lz4|lz4hc|snappy|gzip|zstd :: use the given compression algorithm for -z option (lz4 is default)")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     --nodata :: Do not upload dataset data")
    print("     --link :: Link to dataset data (sourcefile given as <bucket>/<path>)")
    print("     -h | --help    :: This message.")
    print("")
    print("Note about --link option:")
    print("    --link enables just the source HDF5 metadata to be ingested while the dataset data")
    print("     is left in the original file and fetched as needed.")
    print("     When used with files stored in AWS S3, the source file can be specified using the S3")
    print("     path: 's3://<bucket_name>/<s3_path>'.  Preferably, the bucket should be in the same")
    print("     region as the HSDS service")
    print("     For Posix or Azure deployments, the source file needs to be copied to a regular file")
    print("     system and hsload run from a directory that mirrors the bucket layout.  E.g. if")
    print("     consider a Posix deployment where the ROOT_DIR is '/mnt/data' and the HSDS default")
    print("     bucket is 'hsdsdata' (so ingested data will be stored in '/mnt/data/hsdsdata'), the")
    print("     source HDF5 files could be stored in '/mnt/data/hdf5/' and the file 'myhdf5.h5'")
    print("     would be imported as: 'hsload --link data/hdf5/myhdf5.h5 <folder>'")
    print("")
    print("     Also, the --link option requires hdf5lib 1.10.6 or higher and h5py 2.10 or higher.")
    print("     The docker image: 'hdfgroup/hdf5lib:1.10.6' includes these versions as well as h5pyd.")
    print("     E.g.: 'docker run --rm -v ~/.hscfg:/root/.hscfg  -v ~/data:/data -it hdfgroup/hdf5lib:1.10.6 bash'")
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

    COMPRESSION_FILTERS = ('blosclz', 'lz4', 'lz4hc', 'snappy', 'gzip', 'zstd')
    loglevel = logging.ERROR
    verbose = False
    compression = None
    compression_opts = None
    s3path = None
    dataload = "ingest"  # or None, or "link"
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["logfname"] = None
    logfname = None
    s3 = None  # s3fs instance
    mode = 'w'

    src_files = []
    argn = 1
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None

        if arg[0] == '-' and len(src_files) > 0:
            # options must be placed before filenames
            sys.stderr.write("options must precede source files")
            usage()
            sys.exit(-1)

        if len(sys.argv) > argn + 1:
            val = sys.argv[argn + 1]

        if arg in ("-v", "--verbose"):
            verbose = True
            argn += 1
        elif arg == "--link":
            if dataload != "ingest":
                sys.stderr.write("--nodata flag can't be used with link flag")
                sys.exit(1)
            dataload = "link"
            argn += 1
        elif arg == "--nodata":
            if dataload != "ingest":
                sys.stderr.write("--nodata flag can't be used with link flag")
                sys.exit(1)
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
                sys.stderr.write("unknown loglevel")
                usage()
                sys.exit(-1)
            argn += 2
        elif arg == '--logfile':
            logfname = val
            argn += 2
        elif arg in ("-b", "--bucket"):
            cfg["hs_bucket"] = val
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
        elif arg in ("-a", "--append"):
            mode = 'a'
            argn += 1
        elif arg == '--cnf-eg':
            print_config_example()
            sys.exit(0)
        elif arg.startswith("-z"):
            compression_opts = 4
            if len(arg) > 2:
                try:
                    compression_opts = int(arg[2:])
                except ValueError:
                    sys.stderr.write("Compression Level must be int between 0 and 9")
                    sys.exit(-1)
            if not compression:
                compression = 'lz4'
            argn += 1
        elif arg in ("-c", "--compression"):
            if val not in COMPRESSION_FILTERS:
                sys.stderr.write("unknown compression filter")
                usage()
                sys.exit(-1)
            compression = val
            argn += 2
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
    domain = src_files[-1]
    src_files = src_files[:-1]

    logging.info("source files: {}".format(src_files))
    logging.info("target domain: {}".format(domain))
    if len(src_files) > 1 and (domain[0] != '/' or domain[-1] != '/'):
        sys.stderr.write("target must be a folder if multiple source files are provided")
        usage()
        sys.exit(-1)

    if cfg["hs_endpoint"] is None:
        sys.stderr.write('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))

    # check we have min HDF5 lib version for chunk query
    if dataload == "link":
        logging.info("checking libversion")
        if h5py.version.version_tuple.major == 2 and h5py.version.version_tuple.minor < 10:
            sys.stderr.write("link option requires h5py version 2.10 or higher")
            sys.exit(1)
        if h5py.version.hdf5_version_tuple[0] != 1 or h5py.version.hdf5_version_tuple[1] != 10 or h5py.version.hdf5_version_tuple[2] < 6:
            sys.stderr.write("link option requires hdf5 lib version 1.10.6 or higher")
            sys.exit(1)

    try:

        for src_file in src_files:
            # check if this is a non local file, if it is remote (http, etc...) stage it first then insert it into hsds
            src_file_chk = urlparse(src_file)
            logging.debug(src_file_chk)

            tgt = domain
            if tgt[-1] == '/':
                # folder destination
                tgt = tgt + op.basename(src_file)

            # get a handle to input file
            if src_file.startswith("s3://"):
                s3path = src_file
                if not S3FS_IMPORT:
                    sys.stderr.write("Install S3FS package to load s3 files")
                    sys.exit(1)

                if not s3:
                    s3 = s3fs.S3FileSystem(use_ssl=False)
                try:
                    fin = h5py.File(s3.open(src_file, "rb"), moe="r")
                except IOError as ioe:
                    logging.error("Error opening file {}: {}".format(src_file, ioe))
                    sys.exit(1)
            else:
                if dataload == "link":
                    if op.isabs(src_file):
                        sys.stderr.write("source file must be s3path (for HSDS using S3 storage) or relative path from server root directory (for HSDS using posix storage)")
                        sys.exit(1)
                    s3path = src_file
                else:
                    s3path = None
                try:
                    fin = h5py.File(src_file, mode='r')
                except IOError as ioe:
                    logging.error("Error opening file {}: {}".format(src_file, ioe))
                    sys.exit(1)

            # create the output domain
            try:
                username = cfg["hs_username"]
                password = cfg["hs_password"]
                endpoint = cfg["hs_endpoint"]
                bucket = cfg["hs_bucket"]

                fout = h5pyd.File(tgt, mode, endpoint=endpoint, username=username, password=password, bucket=bucket)
            except IOError as ioe:
                if ioe.errno == 404:
                    logging.error("Domain: {} not found".format(tgt))
                elif ioe.errno == 403:
                    logging.error("No write access to domain: {}".format(tgt))
                else:
                    logging.error("Error creating file {}: {}".format(tgt, ioe))
                sys.exit(1)

            # do the actual load
            load_file(fin, fout, verbose=verbose, dataload=dataload, s3path=s3path, compression=compression, compression_opts=compression_opts)

            msg = "File {} uploaded to domain: {}".format(src_file, tgt)
            logging.info(msg)
            if verbose:
                print(msg)

    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)


# __main__
if __name__ == "__main__":
    main()
