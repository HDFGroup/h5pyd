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
    try:
        if isinstance(val, object) and val.__class__.__name__ == "Reference":
            return True
        elif isinstance(val, type) and val.__name__ == "Reference":
            return True
    except AttributeError as ae:
        msg = "is_reference for {} error: {}".format(val, ae)
        logging.error(msg)

    return False


def is_regionreference(val):
    try:
        if isinstance(val, object) and val.__class__.__name__ == "RegionReference":
            return True
        elif isinstance(val, type) and val.__name__ == "RegionReference":
            return True
    except AttributeError as ae:
        msg = "is_reference for {} error: {}".format(val, ae)
        logging.error(msg)

    return False


def has_reference(dtype):
    has_ref = False
    if not isinstance(dtype, np.dtype):
        return False
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

def is_vlen(dtype):
    if dtype.metadata and 'vlen' in dtype.metadata:
        return True
    else:
        return False

def get_fillvalue(dset):

    fillvalue = None
    if not is_vlen(dset.dtype):
        try:
            # can trigger a runtime error if fillvalue is undefined
            fillvalue = dset.fillvalue
        except RuntimeError:
            pass  # ignore
    return fillvalue

def is_compact(dset):
    if isinstance(dset.id.id, str):
        return False  # compact storage not used with HSDS
    cpl = dset.id.get_create_plist()
    if cpl.get_layout() == h5py.h5d.COMPACT:
        return True
    else:
        return False

def convert_dtype(srcdt, ctx):
    """ Return a dtype based on input dtype, converting any Reference types from
    h5py style to h5pyd and vice-versa.
    """

    msg = "convert dtype: {}, type: {},".format(srcdt, type(srcdt))
    logging.info(msg)

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
            if isinstance(src_vlen, np.dtype):
                tgt_base = convert_dtype(src_vlen, ctx)
            else:
                tgt_base = src_vlen
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
                out = ''  # h5pyd refs are strings

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
                        out = str(fout_obj.ref)  # convert to string for JSON serialization

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
            e = src_arr_flat[i]
            element = copy_element(e, src_arr.dtype, tgt_dt, ctx)
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

    tgtarr = None
    data = srcobj.attrs[name]
    src_dt = None
    try:
        src_dt = data.dtype
    except AttributeError:
        pass  # auto convert to numpy array
    # First, make sure we have a NumPy array.
    srcarr = np.asarray(data, order='C', dtype=src_dt)
    tgtarr = copy_array(srcarr, ctx)
    try:
        desobj.attrs.create(name, tgtarr)

    except (IOError, TypeError) as e:
        msg = "ERROR: failed to create attribute {} of object {} -- {}".format(name, desobj.name, str(e))
        logging.error(msg)
        print(msg)


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

    chunks = None
    dset = None

    if ctx["dataload"] == "link" and not is_vlen(dobj.dtype) and not is_compact(dobj):
        dset_dims = dobj.shape
        logging.debug("dset_dims: {}".format(dset_dims))
        rank = len(dset_dims)
        chunk_dims = dobj.chunks
        logging.debug("chunk_dims: {}".format(chunk_dims))
        num_chunks = 0
        dsetid = dobj.id
        spaceid = dsetid.get_space()
        if chunk_dims:
            num_chunks = dsetid.get_num_chunks(spaceid)

        chunks = {}  # pass a map to create_dataset

        if num_chunks == 0:
            chunks["class"] = 'H5D_CONTIGUOUS_REF'
            chunks["file_uri"] = ctx["s3path"]
            chunks["offset"] = dsetid.get_offset()
            # TBD - check the size is not too large
            chunks["size"] = dsetid.get_storage_size()
            logging.info(f"using chunk layout: {chunks}")

        elif num_chunks < 10:
            # construct map of chunks
            chunk_map = {}
            for i in range(num_chunks):
                chunk_info = dsetid.get_chunk_info(i, spaceid)
                index = chunk_info.chunk_offset
                logging.debug(f"got chunk_info: {chunk_info} for chunk: {i}")
                if not isinstance(index, tuple) or len(index) != rank:
                    msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                    logging.error(msg)
                    raise IOError(msg)
                chunk_key = ""
                for dim in range(rank):
                    chunk_key += str(index[dim] // chunk_dims[dim])
                    if dim < rank - 1:
                        chunk_key += "_"
                logging.debug("adding chunk_key: {}".format(chunk_key))
                chunk_map[chunk_key] = (chunk_info.byte_offset, chunk_info.size)

            chunks["class"] = 'H5D_CHUNKED_REF'
            chunks["file_uri"] = ctx["s3path"]
            chunks["dims"] = dobj.chunks
            chunks["chunks"] = chunk_map
            logging.info("using chunk layout: {}".format(chunks))

        else:
            # create anonymous dataset to hold chunk info
            dt = np.dtype([('offset', np.int64), ('size', np.int32)])

            chunkinfo_arr_dims = []
            for dim in range(rank):
                chunkinfo_arr_dims.append(int(np.ceil(dset_dims[dim] / chunk_dims[dim])))
            chunkinfo_arr_dims = tuple(chunkinfo_arr_dims)
            logging.debug("creating chunkinfo array of shape: {}".format(chunkinfo_arr_dims))
            chunkinfo_arr = np.zeros(np.prod(chunkinfo_arr_dims), dtype=dt)
            for i in range(num_chunks):
                chunk_info = dsetid.get_chunk_info(i, spaceid)
                index = chunk_info.chunk_offset
                if not isinstance(index, tuple) or len(index) != rank:
                    msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                    logging.error(msg)
                    raise IOError(msg)
                offset = 0
                stride = 1
                for i in range(rank):
                    dim = rank - i - 1
                    offset += (index[dim] // chunk_dims[dim]) * stride
                    stride *= chunkinfo_arr_dims[dim]
                chunkinfo_arr[offset] = (chunk_info.byte_offset, chunk_info.size)
            anon_dset = fout.create_dataset(None, shape=chunkinfo_arr_dims, dtype=dt)
            anon_dset[...] = chunkinfo_arr
            logging.debug("anon_dset: {}".format(anon_dset))
            #logging.debug("anon_values: {}".format(anon_dset[...]))
            chunks["class"] = 'H5D_CHUNKED_REF_INDIRECT'
            chunks["file_uri"] = ctx["s3path"]
            chunks["dims"] = dobj.chunks
            chunks["chunk_table"] = anon_dset.id.id
            logging.info("using chunk layout: {}".format(chunks))

    # use the source object layout if we are not using reference mapping
    if chunks is None and dobj.chunks:
        chunks = tuple(dobj.chunks)

    try:
        tgt_dtype = convert_dtype(dobj.dtype, ctx)
        if len(dobj.shape) == 0 or (is_vlen(dobj.dtype) and is_h5py(fout)):
            # don't use compression/chunks for scalar datasets
            # or vlen
            compression = None
            compression_opts = None
            chunks = None
            shuffle = None
            fletcher32 = None
            maxshape = None
            scaleoffset = None
        else:
            compression = dobj.compression
            compression_opts = dobj.compression_opts
            if ctx["default_compression"] is not None and compression is None:
                compression = ctx["default_compression"]
                compression_opts = ctx["default_compression_opts"]
                if compression and ctx["verbose"]:
                    print("applying {} filter with level: {}".format(compression, compression_opts))
            shuffle = dobj.shuffle
            fletcher32 = dobj.fletcher32
            maxshape = dobj.maxshape
            scaleoffset = dobj.scaleoffset

        fillvalue=get_fillvalue(dobj)
        dset = fout.create_dataset(
            dobj.name, shape=dobj.shape, dtype=tgt_dtype, chunks=chunks,
            compression=compression, shuffle=shuffle, maxshape=maxshape,
            fletcher32=fletcher32, compression_opts=compression_opts,
            fillvalue=fillvalue, scaleoffset=scaleoffset)
        msg = "dataset created, uuid: {}, chunk_size: {}".format(dset.id.id, str(dset.chunks))
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        logging.debug("adding dataset id {} to {} in srcid_desobj_map".format(dobj.id.id, dset))
        srcid_desobj_map = ctx["srcid_desobj_map"]
        srcid_desobj_map[dobj.id.__hash__()] = dset
    except (IOError, TypeError, KeyError) as e:
        msg = "ERROR: failed to create dataset: {}".format(str(e))
        logging.error(msg)

    return dset
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
        msg = "writing for scalar dataset: {}".format(src.name)
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        tgt[()] = x
        return


    fillvalue = get_fillvalue(src)

    msg = "iterating over chunks for {}".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    try:
        it = ChunkIterator(tgt)

        logging.debug("src dtype: {}".format(src.dtype))
        logging.debug("des dtype: {}".format(tgt.dtype))

        for s in it:
            arr = src[s]
            # don't write arr if it's all zeros (or the fillvalue if defined)
            empty_arr = np.zeros(arr.shape, dtype=arr.dtype)
            if fillvalue:
                empty_arr.fill(fillvalue)
            if np.array_equal(arr, empty_arr):
                msg = "skipping chunk for slice: {}".format(str(s))
            else:
                msg = "writing dataset data for slice: {}".format(s)
                tgt[s] = arr
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
    except (IOError, TypeError) as e:
        msg = "ERROR : failed to copy dataset data : {}".format(str(e))
        logging.error(msg)
        print(msg)
    msg = "done with dataload for {}".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    logging.info("flush fout")

# write_dataset


def create_links(gsrc, gdes, ctx):
    # add soft and external links
    srcid_desobj_map = ctx["srcid_desobj_map"]
    if ctx["verbose"]:
        print("create_links: {}".format(gsrc.name))
    for title in gsrc:
        msg = "got link: {}".format(title)
        if ctx["verbose"]:
            print(msg)
        logging.info(msg)
        lnk = gsrc.get(title, getlink=True)
        link_classname = lnk.__class__.__name__
        if link_classname == "HardLink":
            logging.debug("Got hardlink: {} gsrc: {} gdes: {}".format(title, gsrc, gdes))
            if title not in gdes:
                msg = "creating link {} with title: {}".format(gdes, title)
                if ctx["verbose"]:
                    print(msg)
                logging.info(msg)
                src_obj_id = gsrc[title].id
                src_obj_id_hash = src_obj_id.__hash__()
                logging.debug("got src_obj_id hash: {}".format(src_obj_id_hash))
                if src_obj_id_hash in srcid_desobj_map:
                    des_obj = srcid_desobj_map[src_obj_id_hash]
                    logging.debug("creating hardlink to {}".format(des_obj.id.id))
                    gdes[title] = des_obj
                else:
                    # TBD - in hdf5 1.10 it seems that two references to the same object
                    # can return different id's.  This will cause HDF5 files with
                    # multilinks to not load correctly
                    msg = "could not find map item to src id: {}".format(src_obj_id_hash)
                    logging.warn(msg)
                    if ctx["verbose"]:
                        print("WARNING: " + msg)
        elif link_classname == "SoftLink":
            msg = "creating SoftLink({}) with title: {}".format(lnk.path, title)
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if is_h5py(gdes):
                soft_link = h5py.SoftLink(lnk.path)
            else:
                soft_link = h5pyd.SoftLink(lnk.path)
            gdes[title] = soft_link
        elif link_classname == "ExternalLink":
            msg = "creating ExternalLink({}, {}) with title: {}".format(lnk.filename, lnk.path, title)
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if is_h5py(gdes):
                ext_link = h5py.ExternalLink(lnk.filename, lnk.path)
            else:
                ext_link = h5pyd.ExternalLink(lnk.filename, lnk.path)
            gdes[title] = ext_link
        else:
            msg = "Unexpected link type: {}".format(lnk.__class__.__name__)
            logging.warning(msg)
            if ctx["verbose"]:
                print(msg)
    logging.info("flush fout")


#----------------------------------------------------------------------------------
def create_group(gobj, ctx):
    msg = "creating group {}".format(gobj.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]
    grp = fout.create_group(gobj.name)
    srcid_desobj_map = ctx["srcid_desobj_map"]
    logging.debug("adding group id {} to {} in srcid_desobj_map".format(gobj.id.id, grp))
    srcid_desobj_map[gobj.id.__hash__()] = grp

    return grp
# create_group


# -----------------------------------------------------------------------------
def create_datatype(obj, ctx):
    msg = "creating datatype {}".format(obj.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]
    fout[obj.name] = obj.dtype
    srcid_desobj_map = ctx["srcid_desobj_map"]
    logging.debug("adding datatype id {} to {} in srcid_desobj_map".format(obj.id.id, fout[obj.name]))
    srcid_desobj_map[obj.id.__hash__()] = fout[obj.name]


# create_datatype

#----------------------------------------------------------------------------------
def load_file(fin, fout, verbose=False, dataload="ingest", s3path=None, compression=None,compression_opts=None):
    logging.info("input file: {}".format(fin.filename))
    logging.info("output file: {}".format(fout.filename))
    if dataload != "ingest":
        if not dataload:
            logging.info("no data load")
        elif dataload == "link":
            if not s3path:
                logging.error("s3path expected to be set")
                sys.exit(1)
            logging.info("using s3path")
        else:
            logging.error("unexpected dataload value:", dataload)
            sys.exit(1)

    # it would be nice to make a class out of these functions, but that
    # makes it hard to use visititems iterator.
    # instead, create a context object to pass arround common state
    ctx = {}
    ctx["fin"] = fin
    ctx["fout"] = fout
    ctx["verbose"] = verbose
    ctx["dataload"] = dataload  # ingest, link, or None
    ctx["default_compression"] = compression
    ctx["default_compression_opts"] = compression_opts
    ctx["s3path"] = s3path
    ctx["srcid_desobj_map"] = {}

    def copy_attribute_helper(name, obj):
        logging.info(f"copy attribute - name: {name}  obj: {obj.name}")
        tgt = fout[name]
        for a in obj.attrs:
            copy_attribute(tgt, a, obj, ctx)

    def object_create_helper(name, obj):
        logging.info(f"object create helper - name: {name} obj: {obj.name}")
        class_name = obj.__class__.__name__
        if class_name in ("Dataset", "Table"):
            create_dataset(obj, ctx)
        elif class_name == "Group":
            create_group(obj, ctx)
        elif class_name == "Datatype":
            create_datatype(obj, ctx)

    def object_link_helper(name, obj):
        class_name = obj.__class__.__name__
        logging.info("object_link_helper for object: {}".format(obj.name))
        if class_name == "Group":
            # create any soft/external links
            fout = ctx["fout"]
            grp = fout[name]
            create_links(obj, grp, ctx)

    def object_copy_helper(name, obj):
        class_name = obj.__class__.__name__
        logging.debug("object_copy_helper for object: {}".format(obj.name))

        if class_name in ("Dataset", "Table"):
            if ctx["dataload"] == "link" and not is_vlen(obj.dtype) and not is_compact(obj):
                logging.info("skip datacopy for link reference")
            else:
                logging.debug("calling write_dataset for dataset: {}".format(obj.name))
                tgt = fout[obj.name]
                write_dataset(obj, tgt, ctx)
        elif class_name == "Group":
            logging.debug("skip copy for group: {}".format(obj.name))
        elif class_name == "Datatype":
            logging.debug("skip copy for datatype: {}".format(obj.name))
        else:
            logging.error("no handler for object class: {}".format(type(obj)))

    # build a rough map of the file using the internal function above
    # copy over any attributes
    # create soft/external links (and hardlinks not already created)
    logging.info("creating target objects and attributes")

    # build a rough map of the file using the internal function above
    logging.info("creating target objects")
    fin.visititems(object_create_helper)

    # copy over any attributes
    logging.info("creating target attributes")
    fin.visititems(copy_attribute_helper)

    # create soft/external links (and hardlinks not already created)
    create_links(fin, fout, ctx)  # create root soft/external links
    fin.visititems(object_link_helper)

    if dataload == "ingest" or dataload == "link":
        # copy dataset data
        logging.info("copying dataset data")
        fin.visititems(object_copy_helper)
    else:
        logging.info("skipping dataset data copy (dataload is None)")

    # create any root attributes
    for ga in fin.attrs:
        copy_attribute(fout, ga, fin, ctx)

    # Fully flush the h5py handle.
    fout.close()

    # close up the source domain, see reason(s) for this below
    fin.close()
    msg = "load_file complete"
    logging.info(msg)
    if verbose:
        print(msg)

    return 0
    # load_file
