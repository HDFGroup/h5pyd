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

__version__ = '0.0.1'

UTILNAME = 'hsload'
 
if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
else:
    from urlparse import urlparse
 
 
    
#----------------------------------------------------------------------------------
def stage_file(uri, netfam=None, sslv=True):
    if PYCRUL == None:
        logging.warn("pycurl not available for inline staging of input %s, see pip search pycurl." % uri)
        return None
    try:
        fout = tempfile.NamedTemporaryFile(prefix='hsload.', suffix='.h5', delete=False)
        logging.info("staging %s --> %s" % (uri, fout.name))
        if verbose: print("staging %s" % uri)
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
    print(("    %s [ OPTIONS ]  SOURCE  DOMAIN" % UTILNAME))
    print(("    %s [ OPTIONS ]  SOURCE  FOLDER" % UTILNAME))
    print("")
    print("Description:")
    print("    Copy HDF5 file to Domain or multiple files to a Domain folder")
    print("       SOURCE: HDF5 file or multiple files if copying to folder ")
    print("       DOMAIN: HDF Server domain (Unix or DNS style)")
    print("       FOLDER: HDF Server folder (Unix style ending in '/')")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://example.com:8080")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --nodata :: Do not upload dataset data")
    print("     -4 :: Force ipv4 for any file staging (doesn\'t set hsds loading net)")
    print("     -6 :: Force ipv6 (see -4)")
    print("     -h | --help    :: This message.")
    print("")
    print(("%s version %s\n" % (UTILNAME, __version__)))
#end print_usage

#----------------------------------------------------------------------------------
def print_config_example():
    print("# default")
    print("hs_username = <username>")
    print("hs_password = <passwd>")
    print("hs_endpoint = https://hdfgroup.org:7258")
#print_config_example

#----------------------------------------------------------------------------------
def main():
     
    loglevel = logging.ERROR
    verbose = False
    nodata = False
    cfg = Config()  #  config object
    endpoint=cfg["hs_endpoint"]
    username=cfg["hs_username"]
    password=cfg["hs_password"]
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
        elif arg == '-4':
            ipvfam = 4
        elif arg == '-6':
            ipvfam = 6
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
        else:
            src_files.append(arg)
            argn += 1

    # setup logging
    logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))
    
    # end arg parsing
    logging.info("username: {}".format(username))
    logging.info("password: {}".format(password))
    logging.info("endpoint: {}".format(endpoint))
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
        
    if endpoint is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(endpoint))

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

            tgt = domain
            if tgt[-1] == '/':
                # folder destination
                tgt = tgt + op.basename

            # get a handle to input file
            try:
                fin = h5py.File(src_file, mode='r')
            except IOError as ioe:
                logging.error("Error opening file {}: {}".format(src_domain, ioe))
                sys.exit(1)

            # create the output domain
            try:
                fout = h5pyd.File(tgt, 'w', endpoint=endpoint, username=username, password=password)
            except IOError as ioe:
                if ioe.errno == 404:
                    logging.error("Domain: {} not found".format(src_domain))
                elif ioe.errno == 403:
                    logging.error("No write access to domain: {}".format(src_domain))
                else:
                    logging.error("Error creating file {}: {}".format(des_file, ioe))
                sys.exit(1)


            # do the actual load
            r = load_file(fin, fout, verbose=verbose, nodata=nodata)

            # cleanup if needed
            if istmp:
                try:    
                    os.unlink(src_file)
                except OSError as e:
                    logging.warn("failed to delete %s : %s" % (src_file, str(e)))

            msg = "File {} uploaded to domain: {}".format(src_file, tgt)
            logging.info(msg)
            if verbose:
                print(msg)  
        
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
#__main__

if __name__ == "__main__":
    main()

