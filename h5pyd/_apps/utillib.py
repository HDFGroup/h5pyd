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
 
if __name__ == "utillib":
    from chunkiter import ChunkIterator
else:
    from .chunkiter import ChunkIterator
  
def is_h5py(objref):
    # Return True if objref is a h5py reference and False is not 
    if objref and isinstance(objref.id.id, int):
        return True
    else:
        return False

#----------------------------------------------------------------------------------
def copy_attribute(desobj, name, srcobj, verbose=False):
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
def create_dataset(fd, dobj, verbose=False, nodata=False):
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
def create_group(fd, gobj, verbose=False):
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
def create_datatype(fd, obj, verbose=False):
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
def load_file(fin, fout, verbose=False, nodata=False):
    logging.info("input file: {}".format(fin.filename))   
    logging.info("output file: {}".format(fout.filename))
     
    # create any root attributes
    for ga in fin.attrs:
        copy_attribute(fout, ga, fin)

    def object_create_helper(name, obj):
        class_name = obj.__class__.__name__
        if class_name == "Dataset":
            create_dataset(fout, obj, verbose=verbose, nodata=nodata)
        elif class_name == "Group":
            create_group(fout, obj, verbose=verbose)
        elif class_name == "Datatype":
            create_datatype(fout, obj, verbose=verbose)
        else:
            logging.error("no handler for object class: {}".format(type(obj)))

    # build a rough map of the file using the internal function above
    fin.visititems(object_create_helper)
        
    # Fully flush the h5py handle.  
    fout.close() 
      
    # close up the source domain, see reason(s) for this below
    fin.close() 
    msg="load_file complete: {}"
    logging.info(msg)
    if verbose:
        print(msg)
    
    return 0
# load_file
    
  

