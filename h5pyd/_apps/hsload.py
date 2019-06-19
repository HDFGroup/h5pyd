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
import json
import logging
import os
import os.path as op
import tempfile

try:
    import h5py
    import h5pyd
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e))
    sys.exit(1)

try:
    import pycurl as PYCRUL
except ImportError as e:
    PYCRUL = None

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
def stage_file(uri, netfam=None, sslv=True):
    if PYCRUL == None:
        logging.warn("pycurl not available for inline staging of input %s, see pip search pycurl." % uri)
        return None
    try:
        fout = tempfile.NamedTemporaryFile(prefix='hsload.', suffix='.h5', delete=False)
        logging.info("staging %s --> %s" % (uri, fout.name))
        if cfg["verbose"]: print("staging %s" % uri)
        crlc = PYCRUL.Curl()
        crlc.setopt(crlc.URL, uri)
        if sslv == True:
            crlc.setopt(crlc.SSL_VERIFYPEER, sslv)

        if netfam == 4:
            crlc.setopt(crlc.IPRESOLVE, crlc.IPRESOLVE_V4)
        elif netfam == 6:
            crlc.setopt(crlc.IPRESOLVE, crlc.IPRESOLVE_V6)

        if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            crlc.setopt(crlc.VERBOSE, True)
        crlc.setopt(crlc.WRITEFUNCTION, fout.write)
        crlc.perform()
        crlc.close()
        fout.close()
        return fout.name
    except (IOError, PYCRUL.error) as e:
      logging.error("%s : %s" % (uri, str(e)))
      return None
#stage_file

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
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     -z[n] :: apply compression filter to any non-compressed datasets, n: [0-9]")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")

    print("     --bucket <bucket_name> :: Storage bucket")
    print("     --nodata :: Do not upload dataset data")
    print("     --s3path :: S3Path that holds a copy of sourcefile")
    print("     --storeinfo :: JSON file containing output of store_info utilitity")
    print("     -4 :: Force ipv4 for any file staging (doesn\'t set hsds loading net)")
    print("     -6 :: Force ipv6 (see -4)")
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
    nodata = False
    deflate = None
    s3path = None
    storeinfo = None
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["logfname"] = None
    logfname=None
    ipvfam=None

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
        elif arg == "--s3path":
            if not val.startswith("s3://"):
                print("invalid s3path")
                usage()
                sys.exit(-1)
            s3path = val
            argn += 2
        elif arg == "--storeinfo":
            storeinfo = None
            # val should be a valid filename
            if not op.isfile(val):
                print("Can't open storeinfo file")
                usage()
                sys.exit(-1)
            # try reading the JSON contents
            storeinfo = None
            with open(val) as f:
                storeinfo = json.load(f)
            argn += 2
  
        elif arg == "--nodata":
            nodata = True
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
            cfg["hs_bucket"] = val
            argn += 2
        elif arg == '-4':
            ipvfam = 4
        elif arg == '-6':
            ipvfam = 6
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
    logging.basicConfig(filename=logfname, format='%(asctime)s %(filename)s:%(lineno)d %(message)s', level=loglevel)
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
        print("target must be a folder if multiple source files are provided")
        usage()
        sys.exit(-1)

    if cfg["hs_endpoint"] is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))

    try:

        for src_file in src_files:
            # check if this is a non local file, if it is remote (http, etc...) stage it first then insert it into hsds
            src_file_chk  = urlparse(src_file)
            logging.debug(src_file_chk)

            if src_file_chk.scheme == 'http' or src_file_chk.scheme == 'https' or src_file_chk.scheme == 'ftp':
                src_file = stage_file(src_file, netfam=ipvfam)
                if src_file == None:
                    continue
                istmp = True
                logging.info('temp source data: '+str(src_file))
            else:
                istmp = False

            if storeinfo and src_file not in storeinfo:
                logging.error("Can't find {} in storeinfo json".format(src_file))
                sys.exit(1)

            tgt = domain
            if tgt[-1] == '/':
                # folder destination
                tgt = tgt + op.basename(src_file)

           
            # get a handle to input file
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
                
                fout = h5pyd.File(tgt, 'x', endpoint=endpoint, username=username, password=password, bucket=bucket)
            except IOError as ioe:
                if ioe.errno == 404:
                    logging.error("Domain: {} not found".format(tgt))
                elif ioe.errno == 403:
                    logging.error("No write access to domain: {}".format(tgt))
                else:
                    logging.error("Error creating file {}: {}".format(tgt, ioe))
                sys.exit(1)

            
            # do the actual load
            storeinfo_file = None
            if storeinfo and src_file in storeinfo:
                storeinfo_file = storeinfo[src_file]
            load_file(fin, fout, verbose=verbose, nodata=nodata, deflate=deflate, s3path=s3path, storeinfo=storeinfo_file)

            # cleanup if needed
            if istmp:
                try:
                    os.unlink(src_file)
                except IOError as e:
                    logging.warn("failed to delete %s : %s" % (src_file, str(e)))

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
