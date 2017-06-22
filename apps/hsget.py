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
 

from config import Config
from chunkiter import ChunkIterator

__version__ = '0.0.1'

UTILNAME = 'hsget'
verbose = False
nodata = False
 
def is_h5py(objref):
    # Return True if objref is a h5py reference and False is not 
    if objref and isinstance(objref.id.id, int):
        return True
    else:
        return False

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
    chunks=None
    if dobj.chunks:
        chunks = tuple(dobj.chunks)
    try:
        dset = fd.create_dataset( dobj.name, shape=dobj.shape, dtype=dobj.dtype, chunks=chunks, \
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
        link_classname = lnk.__class__.__name__
        if link_classname == "HardLink":
            logging.debug("Got hardlink: {}".format(title))
            # TBD: handle the case where multiple hardlinks point to same object
        elif link_classname == "SoftLink":
            msg = "creating SoftLink({}) with title: {}".format(lnk.path, title)
            if verbose:
                print(msg)
            logging.info(msg)
            if is_h5py(fd):
                soft_link = h5py.SoftLink(lnk.path)
            else:
                soft_link = h5pyd.SoftLink(lnk.path)
            grp[title] = soft_link
        elif link_classname == "ExternalLink":
            msg = "creating ExternalLink({}, {}) with title: {}".format(lnk.filename, lnk.path, title)
            if verbose:
                print(msg)
            logging.info(msg)
            if is_h5py(fd):
                ext_link = h5py.ExternalLink(lnk.filename, lnk.path)
            else:
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
def load_domain(fin, fout):
    logging.info("input domain: {}".format(fin.filename))   
    logging.info("output file: {}".format(fout.filename))
     
    # create any root attributes
    for ga in fin.attrs:
        copy_attribute(fout, ga, fin)

    def object_create_helper(name, obj):
        class_name = obj.__class__.__name__
        if class_name == "Dataset":
            create_dataset(fout, obj)
        elif class_name == "Group":
            create_group(fout, obj)
        elif class_name == "Datatype":
            create_datatype(fout, obj)
        else:
            logging.error("no handler for object class: {}".format(type(obj)))

    # build a rough map of the file using the internal function above
    fin.visititems(object_create_helper)
        
    # Fully flush the h5py handle.  
    fout.close() 
      
    # close up the source domain, see reason(s) for this below
    fin.close() 
    
    return 0
# load_file
    
 

#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    %s [ OPTIONS ]  DOMAIN SOURCE" % UTILNAME))
    print("")
    print("Description:")
    print("    Copy server domain to local HDF5 file")
    print("       DOMAIN: HDF Server domain (Unix or DNS style)")
    print("       SOURCE: HDF5 file ")
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
    print("     --nodata :: Do not download dataset data")
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
        print("arg:", arg, "val:", val)
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
    logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
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
        fin = h5pyd.File(src_domain, mode='r', endpoint=endpoint, username=username, password=password, use_cache=True)
    except IOError as ioe:
        if ioe.errno == 404:
            logging.error("Domain: {} not found".format(src_domain))
        elif ioe.errno == 403:
            logging.error("No read access to domain: {}".format(src_domain))
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
        load_domain(fin, fout)  
        msg = "Domain {} downloaded to file: {}".format(src_domain, des_file)
        logging.info(msg)
        if verbose:
            print(msg)    
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
    print("done")
#__main__

