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
import numpy as np
    
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

from config import Config
from chunkiter import ChunkIterator

__version__ = '0.0.1'

UTILNAME = 'hsload'
verbose = False
nodata = False
 
if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

#----------------------------------------------------------------------------------
def copy_attribute(desobj, name, srcobj):
    msg = "creating attribute {} in {}".format(name, srcobj.name)
    logging.debug(msg)
    if verbose:
        print(msg)
    try:
        # TBD: we are potentially losing some fidelity here by accessing 
        # the attribute as a numpy array and then passing that to the 
        # the create method.  Better would be to use the h5py low-level
        # API to ensure we get the exact type of the attribute.
        desobj.attrs.create(name, srcobj.attrs[name])
    except (IOError, TypeError) as e:
        msg = "ERROR: failed to create attribute {} of object {} -- {}".format(name, desobj.name, str(e))
        logging.error(msg)
        print(msg)
# copy_attribute
      
#----------------------------------------------------------------------------------
def create_dataset(fd, dobj):
    """ create a dataset using the properties of the passed in h5py dataset.
        If successful, proceed to copy attributes and data.
    """
    msg = "creating dataset {}, shape: {}, type: {}".format(dobj.name, dobj.shape, dobj.dtype)
    logging.info(msg)
    if verbose:
        print(msg) 
       
    fillvalue = None
    try:    
        # can trigger a runtime error if fillvalue is undefined
        fillvalue = dobj.fillvalue
    except RuntimeError:
        pass  # ignore
    
    try:
        dset = fd.create_dataset( dobj.name, shape=dobj.shape, dtype=dobj.dtype, chunks=dobj.chunks, \
                compression=dobj.compression, shuffle=dobj.shuffle, \
                fletcher32=dobj.fletcher32, maxshape=dobj.maxshape, \
                compression_opts=dobj.compression_opts, fillvalue=fillvalue, \
                scaleoffset=dobj.scaleoffset)
        msg = "dataset created, uuid: {}, chunk_size: {}".format(dset.id.id, str(dset.chunks))  
        logging.info(msg)
        if verbose:
            print(msg)
    except (IOError, TypeError) as e:
        msg = "ERROR: failed to create dataset: {}".format(str(e))
        logging.error(msg)
        print(msg)
        return
    # create attributes
    for da in dobj.attrs:
        copy_attribute(dset, da, dobj)

    if nodata:
        msg = "skipping data load"
        logging.info(msg)
        if verbose:
            print(msg)
        return

    msg = "iterating over chunks for {}".format(dobj.name)
    logging.info(msg)
    if verbose:
        print(msg)
    try:
        it = ChunkIterator(dset)

        logging.debug("src dtype: {}".format(dobj.dtype))
        logging.debug("des dtype: {}".format(dset.dtype))
        
        for s in it:
            msg = "writing dataset data for slice: {}".format(s)
            logging.info(msg)
            if verbose:
                print(msg)
            arr = dobj[s]
            dset[s] = arr
    except (IOError, TypeError) as e:
        msg = "ERROR : failed to copy dataset data : {}".format(str(e))
        logging.error(msg)
        print(msg)
    msg = "done with dataload for {}".format(dobj.name)
    logging.info(msg)
    if verbose:
        print(msg)
# create_dataset

#----------------------------------------------------------------------------------
def create_group(fd, gobj):
    msg = "creating group {}".format(gobj.name)
    logging.info(msg)
    if verbose:
        print(msg)
    grp = fd.create_group(gobj.name)

    # create attributes
    for ga in gobj.attrs:
        copy_attribute(grp, ga, gobj)

    # create any soft/external links
    for title in gobj:
        lnk = gobj.get(title, getlink=True)
        if isinstance(lnk, h5py.HardLink):
            logging.debug("Got hardlink: {}".format(title))
            # TBD: handle the case where multiple hardlinks point to same object
        elif isinstance(lnk, h5py.SoftLink):
            msg = "creating SoftLink({}) with title: {}".format(lnk.path, title)
            if verbose:
                print(msg)
            logging.info(msg)
            soft_link = h5pyd.SoftLink(lnk.path)
            grp[title] = soft_link
        elif isinstance(lnk, h5py.ExternalLink):
            msg = "creating ExternalLink({}, {}) with title: {}".format(lnk.filename, lnk.path, title)
            if verbose:
                print(msg)
            logging.info(msg)
            ext_link = h5pyd.ExternalLink(lnk.filename, lnk.path)
            grp[title] = ext_link
        else:
            msg = "Unexpected link type: {}".format(lnk.__class__.__name__)
            logging.warning(msg)
            if verbose:
                print(msg)
# create_group

#----------------------------------------------------------------------------------
def create_datatype(fd, obj):
    msg = "creating datatype {}".format(obj.name)
    logging.info(msg)
    if verbose:
        print(msg)
    fd[obj.name] = obj.dtype
    ctype = fd[obj.name]
    # create attributes
    for ga in obj.attrs:
        copy_attribute(ctype, ga, obj)
# create_datatype
      
#----------------------------------------------------------------------------------
def load_file(filename, domain, endpoint=None, username=None, password=None):
    logging.info("input file: {}".format(filename))   
    finfd = h5py.File(filename, "r")
    logging.info("output domain: {}".format(domain))
    foutfd = h5pyd.File(domain, "w", endpoint=endpoint, username=username, password=password)

    # create any root attributes
    for ga in finfd.attrs:
        copy_attribute(foutfd, ga, finfd)

    def object_create_helper(name, obj):
        if isinstance(obj, h5py.Dataset):
            create_dataset(foutfd, obj)
        elif isinstance(obj, h5py.Group):
            create_group(foutfd, obj)
        elif isinstance(obj, h5py.Datatype):
            create_datatype(foutfd, obj)
        else:
            logging.error("no handler for object class: {}".format(type(obj)))

    # build a rough map of the file using the internal function above
    finfd.visititems(object_create_helper)
        
    # Fully flush the h5pyd handle. The core of the source hdf5 file 
    # has been created on the hsds service up to now.
    foutfd.close() 
      
    # close up the source file, see reason(s) for this below
    finfd.close() 
    msg = "File {} uploaded to domain: {}".format(filename, domain)
    logging.info(msg)
    if verbose:
        print(msg)

    return 0
# load_file
    
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
if __name__ == "__main__":
     
    loglevel = logging.ERROR
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

            r = load_file(src_file, tgt, endpoint=endpoint, username=username, password=password)

            # cleanup if needed
            if istmp:
                try:    
                    os.unlink(src_file)
                except OSError as e:
                    logging.warn("failed to delete %s : %s" % (src_file, sr(e)))
        
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
#__main__

