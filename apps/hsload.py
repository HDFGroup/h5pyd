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
from config import Config
from chunkiter import ChunkIterator

__version__ = '0.0.1'

UTILNAME = 'hsload'
verbose = False
 
#get_sizes

#----------------------------------------------------------------------------------
def copy_attribute(obj, name, attrobj):
    msg = "creating attribute {} in {}".format(name, obj.name)
    logging.debug(msg)
    if verbose:
        print(msg)
    try:
        obj.attrs.create(name, attrobj)
    except (IOError, TypeError) as e:
        msg = "ERROR: failed to create attribute {} of dataset {} -- {}".format(name, obj.name, str(e))
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
    except IOError as ioe:
        msg = "ERROR: failed to create dataset: {}".format(str(ioe))
        logging.error(msg)
        print(msg)
        return
    # create attributes
    for da in dobj.attrs:
        copy_attribute(dset, da, dobj.attrs[da])
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
    except IOError as ioe:
        msg = "ERROR : failed to copy dataset data : {}".format(str(ioe))
        logging.error(msg)
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
        copy_attribute(grp, ga, gobj.attrs[ga])

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

# create_datatype

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
        copy_attribute(ctype, ga, obj.attrs[ga])
# create_group
      
#----------------------------------------------------------------------------------
def load_file(filename, domain, endpoint=None, username=None, password=None):
    logging.info("input file: {}".format(filename))   
    finfd = h5py.File(filename, "r")
    logging.info("output domain: {}".format(domain))
    foutfd = h5pyd.File(domain, "w", endpoint=endpoint, username=username, password=password)

    # create any root attributes
    for ga in finfd.attrs:
        copy_attribute(foutfd, ga, finfd.attrs[ga])

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
    
# hsds_basic_load

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
            tgt = domain
            if tgt[-1] == '/':
                # folder destination
                tgt = tgt + op.basename

            r = load_file(src_file, tgt, endpoint=endpoint, username=username, password=password)
          
        
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
#__main__

