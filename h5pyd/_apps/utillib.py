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
    import numpy as np
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e)) 
    sys.exit(1)
 
if __name__ == "utillib":
    from chunkiter import ChunkIterator
else:
    from .chunkiter import ChunkIterator

def dump_dtype(dt):
    if not isinstance(dt, np.dtype):
        raise TypeError("expected np.dtype, but got: {}".format(type(dt)))
    if len(dt) > 0:
        out = "{"
        for name in dt.fields:
            subdt = dt.fields[name][0]
            out += "{}: {} |".format(name, dump_dtype(subdt))
        out = out[:-1] + "}"
    else:
        ref = h5py.check_dtype(ref=dt)
        if ref:
            out = str(ref)
        else:
            vlen = h5py.check_dtype(vlen=dt)
            if vlen:
                out = "VLEN: " + dump_dtype(vlen)
            else:
                out = str(dt)
    return out

  
def is_h5py(obj):
    # Return True if objref is a h5py object and False is not 
    if isinstance(obj, object) and isinstance(obj.id.id, int):
        return True
    else:
        return False

def is_reference(val):
    if isinstance(val, object) and val.__class__.__name__ == "Reference":
        return True 
    elif isinstance(val, type) and val.__name__ == "Reference":
        return True
    else:
        return False

def is_regionreference(val):
    if isinstance(val, object) and val.__class__.__name__ == "RegionReference":
        return True 
    elif isinstance(val, type) and val.__name__ == "RegionReference":
        return True
    else:
        return False

def has_reference(dtype):
    has_ref = False
    if len(dtype) > 0:
        for name in dtype.fields:
            item = dtype.fields[name]
            if has_reference(item[0]):
                has_ref = True
                break
    elif dtype.metadata and 'ref' in dtype.metadata:
        basedt = dtype.metadata['ref']
        has_ref = is_reference(basedt)
    elif dtype.metadata and 'vlen' in dtype.metadata:
        basedt = dtype.metadata['vlen']
        has_ref = has_reference(basedt)
    return has_ref


def convert_dtype(srcdt, ctx):
    """ Return a dtype based on input dtype, converting any Reference types from 
    h5py style to h5pyd and vice-versa.
    """
    
    msg = "convert dtype: {}, type: {}, len: {}".format(srcdt, type(srcdt), len(srcdt))
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
     
    if len(srcdt) > 0:
        fields = []
        for name in srcdt.fields:
            item = srcdt.fields[name]
            # item is a tuple of dtype and integer offset
            field_dt = convert_dtype(item[0], ctx)
            fields.append((name, field_dt))
        tgt_dt = np.dtype(fields)
    else:
        # check if this a "special dtype"
        if srcdt.metadata and 'ref' in srcdt.metadata:
            ref = srcdt.metadata['ref']
            if is_reference(ref):
                if is_h5py(ctx['fout']):
                    tgt_dt = h5py.special_dtype(ref=h5py.Reference)
                else:
                    tgt_dt = h5pyd.special_dtype(ref=h5pyd.Reference)
            elif is_regionreference(ref):
                if is_h5py(ctx['fout']):
                    tgt_dt = h5py.special_dtype(ref=h5py.RegionReference)
                else:
                    tgt_dt = h5py.special_dtype(ref=h5py.RegionReference)
            else:
                msg = "Unexpected ref type: {}".format(srcdt)
                logging.error(msg)
                raise TypeError(msg)
        elif srcdt.metadata and 'vlen' in srcdt.metadata:
            src_vlen = srcdt.metadata['vlen']
            tgt_base = convert_dtype(src_vlen, ctx)
            if is_h5py(ctx['fout']):
                tgt_dt = h5py.special_dtype(vlen=tgt_base)
            else:
                tgt_dt = h5pyd.special_dtype(vlen=tgt_base)
        else:
            tgt_dt = srcdt
    return tgt_dt

#----------------------------------------------------------------------------------
def copy_element(val, src_dt, tgt_dt, ctx):
    logging.debug("copy_element, val: " + str(val) + " val type: " + str(type(val)) + "src_dt: " + dump_dtype(src_dt) + " tgt_dt: " + dump_dtype(tgt_dt))
     
    fin = ctx["fin"]
    fout = ctx["fout"]
    out = None
    if len(src_dt) > 0:
        out_fields = []
        i = 0
        for name in src_dt.fields:
            field_src_dt = src_dt.fields[name][0]
            field_tgt_dt = tgt_dt.fields[name][0]
            field_val = val[i]
            i += 1
            out_field = copy_element(field_val, field_src_dt, field_tgt_dt, ctx)
            out_fields.append(out_field)
            out = tuple(out_fields)
    elif src_dt.metadata and 'ref' in src_dt.metadata:
        if not tgt_dt.metadata or 'ref' not in tgt_dt.metadata:
            raise TypeError("Expected tgt dtype to be ref, but got: {}".format(tgt_dt))
        ref = tgt_dt.metadata['ref']
        if is_reference(ref):
            # initialize out to null ref
            if is_h5py(ctx['fout']):
                out = h5py.Reference()  # null h5py ref
            else:
                out = '' # h5pyd refs are strings
             
            if ref:
                try:
                    fin_obj = fin[val]
                except AttributeError as ae:
                    msg = "Unable able to get obj for ref value: {}".format(ae)
                    logging.error(msg)
                    print(msg)
                    return None

                # TBD - for hsget, the name property is not getting set
                h5path = fin_obj.name
                if not h5path:
                    msg = "No path found for ref object"
                    logging.warn(msg)
                    if ctx["verbose"]:
                        print(msg)
                else:
                    fout_obj = fout[h5path]
                    if is_h5py(ctx['fout']):
                        out = fout_obj.ref
                    else:
                        out = str(fout_obj.ref) # convert to string for JSON serialization
            
            
        elif is_regionreference(ref):
            out = "tbd"
        else:
            raise TypeError("Unexpected ref type: {}".format(type(ref)))
    elif src_dt.metadata and 'vlen' in src_dt.metadata:
        logging.debug("copy_elment, got vlen element, dt: {}".format(src_dt.metadata["vlen"]))
        if not isinstance(val, np.ndarray):
            raise TypeError("Expecting ndarray or vlen element, but got: {}".format(type(val)))
        if not tgt_dt.metadata or 'vlen' not in tgt_dt.metadata:
            raise TypeError("Expected tgt dtype to be vlen, but got: {}".format(tgt_dt))
        src_vlen_dt = src_dt.metadata["vlen"]
        tgt_vlen_dt = tgt_dt.metadata["vlen"]
        if has_reference(src_vlen_dt):
            if len(val.shape) == 0:
                # scalar array
                e = val[()]
                v = copy_element(e, src_vlen_dt, tgt_vlen_dt, ctx)
                out = np.array(v, dtype=tgt_dt)
            else:
                out = np.zeros(val.shape, dtype=tgt_dt)
                for i in range(len(out)):
                    e = val[i]
                    out[i] = copy_element(e, src_vlen_dt, tgt_vlen_dt, ctx)
        else:
            # can just directly copy the array
            out = np.zeros(val.shape, dtype=tgt_dt)
            out[...] = val[...]
    else:
        out = val  # can just copy as is
    return out



#----------------------------------------------------------------------------------
def copy_array(src_arr, ctx):
    """ Copy the numpy array to a new array.
    Convert any reference type to point to item in the target's hierarchy.
    """
    if not isinstance(src_arr, np.ndarray):
        raise TypeError("Expecting ndarray, but got: {}".format(src_arr))
    tgt_dt = convert_dtype(src_arr.dtype, ctx)
    tgt_arr = np.zeros(src_arr.shape, dtype=tgt_dt)

    if has_reference(src_arr.dtype):
        # flatten array to simplify iteration
        count = np.product(src_arr.shape)
        tgt_arr_flat = tgt_arr.reshape((count,))
        src_arr_flat = src_arr.reshape((count,))
        for i in range(count):
            element = copy_element(src_arr_flat[i], src_arr.dtype, tgt_dt, ctx)
            tgt_arr_flat[i] = element
        tgt_arr = tgt_arr_flat.reshape(src_arr.shape)
    else:
        # can just copy the entire array
        tgt_arr[...] = src_arr[...]
    return tgt_arr


#----------------------------------------------------------------------------------
def copy_attribute(desobj, name, srcobj, ctx):
    msg = "creating attribute {} in {}".format(name, srcobj.name)
    logging.debug(msg)
    if ctx["verbose"]:
        print(msg)
    
    try:
        srcarr = srcobj.attrs[name]
        if isinstance(srcarr, np.ndarray):
            tgtarr = copy_array(srcarr, ctx)
            desobj.attrs.create(name, tgtarr)
        else:
            # scalars are just read as the native type
            desobj.attrs.create(name, srcarr)
    except (IOError, TypeError) as e:
        msg = "ERROR: failed to create attribute {} of object {} -- {}".format(name, desobj.name, str(e))
        logging.error(msg)
        print(msg)
    
# copy_attribute
      
#----------------------------------------------------------------------------------
def create_dataset(dobj, ctx):
    """ create a dataset using the properties of the passed in h5py dataset.
        If successful, proceed to copy attributes and data.
    """
    msg = "creating dataset {}, shape: {}, type: {}".format(dobj.name, dobj.shape, dobj.dtype)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg) 
    fout = ctx["fout"]
    deflate = ctx["deflate"]
       
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
        tgt_dtype = convert_dtype(dobj.dtype, ctx)
        compression_filter = dobj.compression
        compression_opts = dobj.compression_opts
        if deflate is not None and compression_filter is None:
            compression_filter = "gzip"
            compression_opts = deflate
            if ctx["verbose"]:
                print("applying gzip filter with level: {}".format(deflate))

        dset = fout.create_dataset( dobj.name, shape=dobj.shape, dtype=tgt_dtype, chunks=chunks, \
                compression=compression_filter, shuffle=dobj.shuffle, \
                fletcher32=dobj.fletcher32, maxshape=dobj.maxshape, \
                compression_opts=compression_opts, fillvalue=fillvalue, \
                scaleoffset=dobj.scaleoffset)
        msg = "dataset created, uuid: {}, chunk_size: {}".format(dset.id.id, str(dset.chunks))  
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
    except (IOError, TypeError, KeyError) as e:
        msg = "ERROR: failed to create dataset: {}".format(str(e))
        logging.error(msg)
        print(msg)
        return
    
# create_dataset

#----------------------------------------------------------------------------------
def write_dataset(src, tgt, ctx):
    """ write values from src dataset to target dataset.
    """
    msg = "write_dataset src: {} to tgt: {}, shape: {}, type: {}".format(src.name, tgt.name, src.shape, src.dtype)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg) 

    if src.shape is None:
        # null space dataset
        msg = "no data for null space dataset: {}".format(src.name)
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        return  # no data 

    if len(src.shape) == 0:
        # scalar dataset
        x = src[()]
        msg = "writing: {} for scalar dataset: {}".format(x, src.name)
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        tgt[()] = x
        return

    msg = "iterating over chunks for {}".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    try:
        it = ChunkIterator(tgt)

        logging.debug("src dtype: {}".format(src.dtype))
        logging.debug("des dtype: {}".format(tgt.dtype))
        
        for s in it:
            msg = "writing dataset data for slice: {}".format(s)
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
            arr = src[s]
            tgt[s] = arr
    except (IOError, TypeError) as e:
        msg = "ERROR : failed to copy dataset data : {}".format(str(e))
        logging.error(msg)
        print(msg)
    msg = "done with dataload for {}".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
# write_dataset



#----------------------------------------------------------------------------------
def create_group(gobj, ctx):
    msg = "creating group {}".format(gobj.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]
    grp = fout.create_group(gobj.name)
 
    # create any soft/external links
    for title in gobj:
        lnk = gobj.get(title, getlink=True)
        link_classname = lnk.__class__.__name__
        if link_classname == "HardLink":
            logging.debug("Got hardlink: {}".format(title))
            # TBD: handle the case where multiple hardlinks point to same object
        elif link_classname == "SoftLink":
            msg = "creating SoftLink({}) with title: {}".format(lnk.path, title)
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if is_h5py(fout):
                soft_link = h5py.SoftLink(lnk.path)
            else:
                soft_link = h5pyd.SoftLink(lnk.path)
            grp[title] = soft_link
        elif link_classname == "ExternalLink":
            msg = "creating ExternalLink({}, {}) with title: {}".format(lnk.filename, lnk.path, title)
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if is_h5py(fout):
                ext_link = h5py.ExternalLink(lnk.filename, lnk.path)
            else:
                ext_link = h5pyd.ExternalLink(lnk.filename, lnk.path)
            grp[title] = ext_link
        else:
            msg = "Unexpected link type: {}".format(lnk.__class__.__name__)
            logging.warning(msg)
            if ctx["verbose"]:
                print(msg)
# create_group

#----------------------------------------------------------------------------------
def create_datatype(obj, ctx):
    msg = "creating datatype {}".format(obj.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]
    fout[obj.name] = obj.dtype

     
# create_datatype
      
#----------------------------------------------------------------------------------
def load_file(fin, fout, verbose=False, nodata=False, deflate=None):
    logging.info("input file: {}".format(fin.filename))   
    logging.info("output file: {}".format(fout.filename))
     
    # it would be nice to make a class out of these functions, but that 
    # makes it heard to use visititems iterator.
    # instead, create a context object to pass arround common state
    ctx = {}
    ctx["fin"] = fin
    ctx["fout"] = fout
    ctx["verbose"] = verbose
    ctx["nodata"] = nodata
    ctx["deflate"] = deflate
    

    # create any root attributes
    for ga in fin.attrs:
        copy_attribute(fout, ga, fin, ctx)

    def object_create_helper(name, obj):
        class_name = obj.__class__.__name__
         
        if class_name == "Dataset":
            create_dataset(obj, ctx)
        elif class_name == "Group":
            create_group(obj, ctx)
        elif class_name == "Datatype":
            create_datatype(obj, ctx)
        else:
            logging.error("no handler for object class: {}".format(type(obj)))
    
    def object_copy_helper(name, obj):
        class_name = obj.__class__.__name__
        if class_name == "Dataset":
            tgt = fout[obj.name]
            write_dataset(obj, tgt, ctx)
        elif class_name == "Group":
            logging.debug("skip copy for group: {}".format(obj.name))
        elif class_name == "Datatype":
            logging.debug("skip copy for datatype: {}".format(obj.name))
        else:
            logging.error("no handler for object class: {}".format(type(obj)))

    def object_attribute_helper(name, obj):
        tgt = fout[obj.name]
        for ga in obj.attrs:
            copy_attribute(tgt, ga, obj, ctx)

    # build a rough map of the file using the internal function above
    fin.visititems(object_create_helper)

    # copy over any attributes
    fin.visititems(object_attribute_helper)

    if not nodata:
        # copy dataset data
        fin.visititems(object_copy_helper)
        
    # Fully flush the h5py handle.  
    fout.close() 
      
    # close up the source domain, see reason(s) for this below
    fin.close() 
    msg="load_file complete"
    logging.info(msg)
    if verbose:
        print(msg)
    
    return 0
# load_file
    
  

