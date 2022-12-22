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
    sys.stderr.write(f"ERROR : {e} : install it to use this utility...")
    sys.exit(1)

if __name__ == "utillib":
    from chunkiter import ChunkIterator
else:
    from .chunkiter import ChunkIterator


def dump_dtype(dt):
    if not isinstance(dt, np.dtype):
        raise TypeError(f"expected np.dtype, but got: {type(dt)}")
    if len(dt) > 0:
        out = "{"
        for name in dt.fields:
            subdt = dt.fields[name][0]
            out += f"{name}: {dump_dtype(subdt)} |"
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
        msg = f"is_reference for {val} error: {ae}"
        logging.warning(msg)

    return False


def is_regionreference(val):
    try:
        if isinstance(val, object) and val.__class__.__name__ == "RegionReference":
            return True
        elif isinstance(val, type) and val.__name__ == "RegionReference":
            return True
    except AttributeError as ae:
        msg = f"is_reference for {val} error: {ae}"
        logging.warning(msg)

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
    elif dtype.metadata and "ref" in dtype.metadata:
        basedt = dtype.metadata["ref"]
        has_ref = is_reference(basedt)
    elif dtype.metadata and "vlen" in dtype.metadata:
        basedt = dtype.metadata["vlen"]
        has_ref = has_reference(basedt)
    return has_ref


def is_vlen(dtype):
    if dtype.metadata and "vlen" in dtype.metadata:
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
            logging.warning(f"runtime error getting fillvalue for dataset: {dset.name}")
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
    """Return a dtype based on input dtype, converting any Reference types from
    h5py style to h5pyd and vice-versa.
    """

    msg = f"convert dtype: {srcdt}, type: {type(srcdt)}"
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
        if srcdt.metadata and "ref" in srcdt.metadata:
            ref = srcdt.metadata["ref"]
            if is_reference(ref):
                if is_h5py(ctx["fout"]):
                    tgt_dt = h5py.special_dtype(ref=h5py.Reference)
                else:
                    tgt_dt = h5pyd.special_dtype(ref=h5pyd.Reference)
            elif is_regionreference(ref):
                if is_h5py(ctx["fout"]):
                    tgt_dt = h5py.special_dtype(ref=h5py.RegionReference)
                else:
                    tgt_dt = h5py.special_dtype(ref=h5py.RegionReference)
            else:
                msg = f"Unexpected ref type: {srcdt}"
                logging.error(msg)
                if not ctx["ignore_error"]:
                    raise TypeError(msg)
        elif srcdt.metadata and "vlen" in srcdt.metadata:
            src_vlen = srcdt.metadata["vlen"]
            if isinstance(src_vlen, np.dtype):
                tgt_base = convert_dtype(src_vlen, ctx)
            else:
                tgt_base = src_vlen
            if is_h5py(ctx["fout"]):
                tgt_dt = h5py.special_dtype(vlen=tgt_base)
            else:
                tgt_dt = h5pyd.special_dtype(vlen=tgt_base)
        elif srcdt.kind == "U":
            # use vlen for unicode strings
            if is_h5py(ctx["fout"]):
                tgt_dt = h5py.special_dtype(vlen=str)
            else:
                tgt_dt = h5pyd.special_dtype(vlen=str)
        else:
            tgt_dt = srcdt
    return tgt_dt


# ----------------------------------------------------------------------------------
def copy_element(val, src_dt, tgt_dt, ctx):
    msg = f"copy_element, val: {val} "
    msg += f"val type: {type(val)} src_dt:  {dump_dtype(src_dt)} "
    msg += f" tgt_dt: {dump_dtype(tgt_dt)}"
    logging.debug(msg)

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
    elif src_dt.metadata and "ref" in src_dt.metadata:
        if not tgt_dt.metadata or "ref" not in tgt_dt.metadata:
            raise TypeError(f"Expected tgt dtype to be ref, but got: {tgt_dt}")
        ref = tgt_dt.metadata["ref"]
        if is_reference(ref):
            # initialize out to null ref
            if is_h5py(ctx["fout"]):
                out = h5py.Reference()  # null h5py ref
            else:
                out = ""  # h5pyd refs are strings

            if ref:
                try:
                    fin_obj = fin[val]
                except AttributeError as ae:
                    msg = f"Unable able to get obj for ref value: {ae}"
                    logging.error(msg)
                    if not ctx["ignore_error"]:
                        raise IOError(msg)
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
                    if is_h5py(ctx["fout"]):
                        out = fout_obj.ref
                    else:
                        out = str(
                            fout_obj.ref
                        )  # convert to string for JSON serialization

        elif is_regionreference(ref):
            out = "tbd"
        else:
            raise TypeError(f"Unexpected ref type: {type(ref)}")
    elif src_dt.metadata and "vlen" in src_dt.metadata:
        logging.debug(
            "copy_elment, got vlen element, dt: {}".format(src_dt.metadata["vlen"])
        )
        if not isinstance(val, np.ndarray):
            raise TypeError(f"Expecting ndarray or vlen element, but got: {type(val)}")
        if not tgt_dt.metadata or "vlen" not in tgt_dt.metadata:
            raise TypeError(f"Expected tgt dtype to be vlen, but got: {tgt_dt}")
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


# ---------------------------------------------------------------------------------
def copy_array(src_arr, ctx):
    """Copy the numpy array to a new array.
    Convert any reference type to point to item in the target's hierarchy.
    """
    if not isinstance(src_arr, np.ndarray):
        raise TypeError(f"Expecting ndarray, but got: {src_arr}")
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


# ----------------------------------------------------------------------------------
def copy_attribute(desobj, name, srcobj, ctx):

    msg = f"creating attribute {name} in {srcobj.name}"
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
    if is_h5py(srcobj):
        src_empty = h5py.Empty
    else:
        src_empty = h5pyd.Empty
    if is_h5py(desobj):
        des_empty = h5py.Empty
    else:
        des_empty = h5pyd.Empty

    if isinstance(data, src_empty):
        # create Empty object with tgt dtype
        tgt_dt = convert_dtype(src_dt, ctx)
        tgtarr = des_empty(tgt_dt)
    else:
        srcarr = np.asarray(data, order="C", dtype=src_dt)
        tgtarr = copy_array(srcarr, ctx)
    try:
        desobj.attrs.create(name, tgtarr)
    except (IOError, TypeError) as e:
        msg = f"ERROR: failed to create attribute {name} "
        msg += f"of object {desobj.naame} -- {e}"
        logging.error(msg)
        if not ctx["ignore_error"]:
            raise IOError(msg)

# "safe" resize method where new extent can be <= existing extent
def resize_dataset(dset, extent, axis=0):
    logging.debug(f"resize_dataset {dset} to {extent}")
    try:
        dset.resize(extent, axis=axis)
    except IOError:
        # raise this if it's not a case where the extent is already increased
        if dset.shape[axis] < extent:
            raise

# ----------------------------------------------------------------------------------
def get_chunk_layout(dset):
    if is_h5py(dset):
        msg = "get_chunk_layout called on hdf5 dataset"
        logging.error(msg)
        raise IOError(msg)
    dset_json = dset.id.dcpl_json
    if "layout" not in dset_json:
        msg = f"expect to find layout key in dset_json: {dset_json}"
        logging.error(msg)
        raise IOError(msg)
    layout = dset_json["layout"]
    logging.debug(f"got chunk layout for dset id: {dset.id.id}: {layout}")
    return layout
    


# ----------------------------------------------------------------------------------
def get_chunk_dims(dset):
    if dset.chunks:
        chunk_dims = dset.chunks
    elif not is_h5py(dset):
        # h5pyd datset, check layout
        layout = dset.id.layout
        if layout and "dims" in layout:
            # linked datasets will use chunk shape in source hdf5 file
            # (or source dataset shape for contiguous datasets)
            chunk_dims = tuple(layout["dims"])
        else:
            # define a psuedo-chunk with same dimensions as the dataset
            chunk_dims = dset.shape
    elif dset.shape is None:
        chunk_dims = None
    elif len(dset.shape) > 0 and np.prod(dset.shape) > 0:
        # just use dataset shape
        chunk_dims = dset.shape
    else:
        # no chunk dims
        chunk_dims = None
    return chunk_dims

# ----------------------------------------------------------------------------------
def get_chunktable_dims(dset):
    rank = len(dset.shape)
    chunk_dims = get_chunk_dims(dset)
    table_dims = []
    for dim in range(rank):
        dset_extent = dset.shape[dim]
        chunk_extent = chunk_dims[dim]

        if dset_extent > 0 and chunk_extent > 0:
            table_extent = -(dset_extent // -chunk_extent)
        else:
            table_extent = 0
        table_dims.append(table_extent)
    table_dims = tuple(table_dims)
    return table_dims

# ----------------------------------------------------------------------------------
def get_num_chunks(dset):
    if dset.chunks:
        if is_h5py(dset):
            dsetid = dset.id
            spaceid = dsetid.get_space()
            num_chunks = dsetid.get_num_chunks(spaceid)
        else:
            # for hsds, just return maximum number of chunk in dataset
            chunk_table_dims = get_chunktable_dims(dset)
            num_chunks = np.prod(chunk_table_dims)
    else:
        # use a psuedo-chunk with same dimensions as the dataset
        num_chunks = 1
    return num_chunks

# ----------------------------------------------------------------------------------
def get_chunktable_dtype(include_file_uri=False):
    if include_file_uri:
        dt_str = h5pyd.special_dtype(vlen=bytes)
        dt = np.dtype([("offset", np.int64), ("size", np.int32), ("file_uri", dt_str)])
    else:
        dt = np.dtype([("offset", np.int64), ("size", np.int32)])
    return dt

# ----------------------------------------------------------------------------------
def get_chunk_table_index(chunk_offset, chunk_dims):
    if len(chunk_offset) != len(chunk_dims):
        msg = f"Unexptected chunk offset: {chunk_offset}"
        logging.error(msg)
    rank = len(chunk_offset)
    chunk_index = []
    for i in range(rank):
        chunk_index.append(chunk_offset[i]//chunk_dims[i])
    return tuple(chunk_index)

# ----------------------------------------------------------------------------------
def create_chunktable(dset, dset_dims, ctx):
    logging.debug(f"create_chunktable({dset}, {dset_dims}")

    num_chunks = get_num_chunks(dset)
    rank = len(dset_dims)
    extend = True if rank > len(dset.shape) else False
    if extend:
        include_file_uri = True
    else:
        include_file_uri = False

    logging.debug(f"num_chunks: {num_chunks}")

    chunks = {}  # pass a map to create_dataset

    if num_chunks > 10 or extend:
        # create anonymous dataset to hold chunk info
        dt = get_chunktable_dtype(include_file_uri=include_file_uri)

        chunktable_dims = [0,] if extend else []
        
        chunktable_dims.extend(get_chunktable_dims(dset))
        chunktable_dims = tuple(chunktable_dims)
        logging.debug(f"chunktable_dims: {chunktable_dims}")
        chunktable_maxshape = [None,] if extend else []
        chunktable_maxshape.extend(get_chunktable_dims(dset))
        chunk_dims = [1,] if extend else []
        chunk_dims.extend(get_chunk_dims(dset))

        fout = ctx["fout"]
        anon_dset = fout.create_dataset(None, shape=chunktable_dims, dtype=dt, maxshape=chunktable_maxshape)
        msg = f"created chunk table: {anon_dset}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        chunks["class"] = "H5D_CHUNKED_REF_INDIRECT"
        if not extend:
            chunks["file_uri"] = ctx["s3path"]
        chunks["dims"] = chunk_dims
        chunks["chunk_table"] = anon_dset.id.id
    elif num_chunks <= 1 and dset.chunks is None:
        # use contiguous mapping
        chunks["class"] = "H5D_CONTIGUOUS_REF"
        chunks["file_uri"] = ctx["s3path"]
        dset_offset = dset.id.get_offset()
        if dset_offset <= 0:
            msg = f"unexpected dataset_offset: {dset_offset}"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
        dset_size = dset.id.get_storage_size()
        if dset_size <= 0:
            msg = f"unexpected dataset storage size: {dset_size}"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
        chunks["offset"] = dset_offset
        # TBD - check the size is not too large
        chunks["size"] = dset_size
    else:
        # construct map of chunks if count is less than 10
        chunk_map = {}
        if is_h5py(dset):
            # for hdf5 use get_chunk_info function to get chunk location for each chunk
            spaceid = dset.id.get_space()
            chunk_dims = get_chunk_dims(dset)
            for i in range(num_chunks):
                chunk_info = dset.id.get_chunk_info(i, spaceid)
                index = chunk_info.chunk_offset
                logging.debug(f"got chunk_info: {chunk_info} for chunk: {i}")
                if not isinstance(index, tuple) or len(index) != rank:
                    msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                    logging.error(msg)
                    if not ctx["ignore_error"]:
                        raise IOError(msg)
                chunk_key = ""
                for dim in range(rank):
                    chunk_key += str(index[dim] // chunk_dims[dim])
                    if dim < rank - 1:
                        chunk_key += "_"
                logging.debug(f"adding chunk_key: {chunk_key}")
                chunk_map[chunk_key] = (chunk_info.byte_offset, chunk_info.size)
        else:
            msg = f"expected {dset} to be a h5py object"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)


        chunks["class"] = "H5D_CHUNKED_REF"
        if not extend:
            chunks["file_uri"] = ctx["s3path"]
        chunks["dims"] = dset.chunks
        chunks["chunks"] = chunk_map
    logging.info(f"using chunk layout: {chunks}")
    return chunks


# ----------------------------------------------------------------------------------
def update_chunktable(src, tgt, ctx):
    layout = tgt.id.layout
    if not layout:
        raise IOError("expected dataset layout to be set")
    if layout["class"] != "H5D_CHUNKED_REF_INDIRECT":
        logging.info("update_chunktable not supported for this chunk class")
        return
    rank = len(tgt.shape)
    chunktable_id = layout["chunk_table"]

    fout = ctx["fout"]

    chunktable = fout[f"datasets/{chunktable_id}"]
    chunk_dims = get_chunk_dims(src)
    chunktable_dims = get_chunktable_dims(src)

    msg = f"dataset chunk dimensions {chunktable_dims} not compatible with {chunktable.shape}"
    if len(chunktable_dims) == len(chunktable.shape):
        if chunktable_dims != chunktable.shape:
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return
    elif len(chunktable_dims) + 1 == len(chunktable.shape):
        if chunktable_dims != chunktable.shape[1:]:
            logging.error(msg)
            return
    else:
        logging.error(msg)
        if not ctx["ignore_error"]:
            raise IOError(msg)
        return

    # create a numpy array containing chunk refs for each chunk in src array
    extend = True if rank > len(src.shape) else False
    if extend and not ctx["s3path"]:
        logging.error("expected s3path to be set for extend mode")
        if not ctx["ignore_error"]:
            raise IOError(msg)
        return
    # prior to HSDS v0.7.0+, reads failed if str was used for s3path,
    # store as bytes
    s3path = ctx["s3path"].encode("utf-8")
    dt = get_chunktable_dtype(include_file_uri=extend)
    chunkinfo_arr = np.zeros(chunktable_dims, dtype=dt)
    rank = len(chunktable_dims)
    num_chunks = get_num_chunks(src)
   
    if is_h5py(src):   
        if src.chunks is None:
            chunk_offset = src.id.get_offset()
            chunk_size = src.id.get_storage_size()
            if extend:
                chunkinfo_arr[...] = (chunk_offset, chunk_size, s3path)
            else:
                chunkinfo_arr[...] = (chunk_offset, chunk_size)
        else:
            spaceid = src.id.get_space()
    
            for i in range(num_chunks):
                chunk_info = src.id.get_chunk_info(i, spaceid)
                index = get_chunk_table_index(chunk_info.chunk_offset, chunk_dims)
                if not isinstance(index, tuple) or len(index) != rank:
                    msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                    logging.error(msg)
                    if not ctx["ignore_error"]:
                        raise IOError(msg)
                    return
                e = [chunk_info.byte_offset, chunk_info.size]
                if extend:
                    e.append(s3path)
                e = tuple(e)
                chunkinfo_arr[index] = e
    else:
        if not extend:
            msg = "unexpected src type for update_chunktable"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return
    
        layout = get_chunk_layout(src)
        layout_class = layout["class"]
        if layout_class == "H5D_CONTIGUOUS_REF":
            chunk_offset = layout["offset"]
            chunk_size = layout["size"]
            file_uri = layout["file_uri"]
            chunkinfo_arr[...] = (chunk_offset, chunk_size, file_uri)
        elif layout_class == "H5D_CHUNKED_REF":
            file_uri = layout["file_uri"]
            chunkmap = layout["chunks"]  # e.g.{'0_2': [4016, 2000000]}}
            for k in chunkmap:
                v = chunkmap[k]
                v.append(s3path)
                v = tuple(v)
                index = []
                chunk_indices = k.split("_")
                for i in range(len(chunk_indices)):
                    index.append(int(chunk_indices[i]))
                index = tuple(index)
                chunkinfo_arr[index] = v
        elif layout_class == "H5D_CHUNKED_REF_INDIRECT":
            file_uri = layout["file_uri"] 
            orig_chunktable_id = layout["chunk_table"]
            orig_chunktable = fout[f"datasets/{orig_chunktable_id}"]
            # iterate through contents and add file uri
            arr = orig_chunktable[...]
            it = np.nditer(arr, flags=['multi_index'])
            for _ in it:
                value = arr[it.multi_index]
                if value[1] == 0:
                    # no chunk location set
                    continue
                e = list(value)
                e.append(file_uri)
                e = tuple(e)
                tgt_index = [0,]
                tgt_index.extend(it.multi_index)
                tgt_index = tuple(tgt_index)
                chunkinfo_arr[it.multi_index] = e
        else:
            msg = f"expected chunk ref class but got: {layout_class}"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return
            
    if len(tgt.shape) > len(src.shape):
        # append mode, extend the first dimension of table by one
        extent = chunktable.shape[0] + 1
        resize_dataset(chunktable, extent)
        chunktable[extent - 1, ...] = chunkinfo_arr
    else:
        chunktable[...] = chunkinfo_arr


# ----------------------------------------------------------------------------------
def create_dataset(dobj, ctx):
    """create a dataset using the properties of the passed in h5py dataset.
    If successful, proceed to copy attributes and data.
    """
    chunks = None
    dset = None
    dset_preappend = None

    msg = f"creating dataset {dobj.name}, shape: {dobj.shape}, type: {dobj.dtype}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]

    if dobj.name in fout:
        dset = fout[dobj.name]
        logging.debug(f"{dobj.name} already exists")
        if ctx["append"]:
            if ctx["extend_dim"]:
                msg = f"skipping creation of dataset {dobj.name} since already found"
                logging.info(msg)
                if ctx["verbose"]:
                    print(msg)
                return dset
            else:
                if len(dset.shape) == len(dobj.shape):
                    if dset.shape != dobj.shape:
                        msg = f"unable to append {dobj.name}: shape is not compatible"
                        logging.error(msg)    
                        if ctx["verbose"]:
                            print(msg)
                        return None
                    if len(dset.shape) == 0:
                        # don't try to extend scalar datasets (treat like attributes)
                        return dset

                    # add an extra dimension to append to
                    msg = f"re-creating {dobj.name} with extended dimension"
                    logging.info(msg)
                    if ctx["verbose"]:
                        print(msg)
                    dset_preappend = dset  # save to re-add data later
                    parent = dset.parent
                    obj_name = dobj.name.split("/")[-1]
                    logging.debug(f"removing link {dobj.name}")
                    del parent[obj_name]  # remove old link
                if len(dset.shape) == len(dobj.shape) + 1:
                    if dset.shape[1:] != dobj.shape:
                        msg = f"unable to append {dobj.name}: shape is not compatible"
                        logging.error(msg)
                        if not ctx["ignore_error"]:
                            raise IOError(msg)
                        return None
                    else:
                        # compatible shapes, can just return dset
                        return dset
        else:
            msg = f"unable to create dataset {dobj.name} already present"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return None
    else:
        if ctx["verbose"]:
            print(f"{dobj.name} not found")

    try:
        tgt_dtype = convert_dtype(dobj.dtype, ctx)
        if dset_preappend is not None:
            # add an extra unlimited dimension
            tgt_shape = [0,]
            tgt_maxshape = [None,]
        elif dobj.shape is None:
            tgt_shape = None
            tgt_maxshape = None
        else:
            tgt_shape = []
            tgt_maxshape = []

        if tgt_shape is None:
            rank = 0
        else:
            tgt_shape.extend(dobj.shape)
            tgt_maxshape.extend(dobj.maxshape)
            rank = len(tgt_shape)
        if rank > 0 and ctx["extend_dim"]:
            # set maxshape to unlimited for any dimension that is the extend_dim
            if dobj.name.split("/")[-1] == ctx["extend_dim"]:
                msg = f"setting {dobj.name} shape to unlimited"
                logging.info(msg)
                if ctx["verbose"]:
                    print(msg)
                if rank > 1:
                    msg = "expected extend dataset to be one-dimensional"
                    logging.warning(msg)
                    if ctx["verbose"]:
                        print(msg)
                for i in range(rank):
                    tgt_shape[i] = 0
                    tgt_maxshape[i] = None
            else:
                # check to see if any dimension scale refers to the extend dim
                for dim in range(len(dobj.dims)):
                    dimscales = dobj.dims[dim]
                    for i in range(len(dimscales)):
                        dimscale = dimscales[i]
                        if not dimscale:
                            continue
                        msg = f"dimscale for dim: {dim}: {dimscale}, type: {type(dimscale)}"
                        logging.debug(msg)
                        if dimscale.name.split("/")[-1] == ctx["extend_dim"]:
                            tgt_shape[dim] = 0
                            tgt_maxshape[dim] = None
                            msg = f"setting dimension {dim} of dataset {dobj.name} to unlimited"
                            logging.info(msg)
                            if ctx["verbose"]:
                                print(msg)
                            break

        kwargs = {"shape": tgt_shape, "maxshape": tgt_maxshape, "dtype": tgt_dtype}

        if (
            ctx["dataload"] == "link"
            and not is_vlen(dobj.dtype)
            and dobj.shape is not None
        ):
            chunks = create_chunktable(dobj, tgt_shape, ctx)
            logging.info(f"using chunk layout: {chunks}")

        # use the source object layout if we are not using reference mapping
        if chunks is None:
            # converting hsds dset with linked chunks to h5py dataset
            # just use the dims field of dobj.chunks as chunk shape
            chunks = get_chunk_dims(dobj)
        if chunks is not None:
            if dset_preappend is not None:
                # check to see if an extra dimension is needed for the chunk shape
                if isinstance(chunks, dict):
                    # chunktable is already adjusted
                    pass
                else:
                    new_chunks = [1,]
                    new_chunks.extend(chunks)
                    chunks = tuple(new_chunks)
            kwargs["chunks"] = chunks

        if (
            dobj.shape is None
            or len(dobj.shape) == 0
            or (is_vlen(dobj.dtype) and is_h5py(fout))
        ):
            # don't use compression/chunks for scalar datasets
            # or vlen
            pass
        else:
            kwargs["compression"] = dobj.compression
            kwargs["compression_opts"] = dobj.compression_opts
            if ctx["default_compression"] is not None and dobj.compression is None:
                kwargs["compression"] = ctx["default_compression"]
                kwargs["compression_opts"] = ctx["default_compression_opts"]
                if ctx["verbose"]:
                    print("applying default compression filter")
            kwargs["shuffle"] = dobj.shuffle
            kwargs["fletcher32"] = dobj.fletcher32
            kwargs["scaleoffset"] = dobj.scaleoffset
        # setting the fillvalue is failing in some cases
        # see: https://github.com/HDFGroup/h5pyd/issues/119
        # just setting to None for now
        fillvalue = get_fillvalue(dobj)
        if fillvalue is not None:
            logging.debug(f"got fillvalue: {fillvalue}")
            kwargs["fillvalue"] = fillvalue

        # finally, create the dataset    
        dset = fout.create_dataset(dobj.name, **kwargs)

        msg = f"dataset created, uuid: {dset.id.id}, "
        msg += f"chunk_size: {str(dset.chunks)}, "
        msg += f"chunks: {chunks}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        logging.debug(f"adding dataset id {dobj.id.id} to {dset}")
        srcid_desobj_map = ctx["srcid_desobj_map"]
        srcid_desobj_map[dobj.id.__hash__()] = dset
    except (IOError, TypeError, KeyError) as e:
        msg = f"ERROR: failed to create dataset: {e}"
        logging.error(msg)
        if not ctx["ignore_error"]:
            raise
        

    if dset_preappend is not None:
        write_dataset(dset_preappend, dset, ctx)
        # dset[0, ...] = dset_preappend[...]
    return dset

# ----------------------------------------------------------------------------------
def write_dataset(src, tgt, ctx):
    """write values from src dataset to target dataset."""
    msg = f"write_dataset src: {src.name} to tgt: {tgt.name}, shape: {src.shape}, type: {src.dtype}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    if src.shape is None:
        # null space dataset
        msg = f"no data for null space dataset: {src.name}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        return  # no data

    if len(src.shape) == 0:
        # scalar dataset
        x = src[()]
        msg = f"writing for scalar dataset: {src.name}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        tgt[()] = x
        return

    if np.prod(src.shape) == 0:
        msg = f"no data for dataset with zero extent: {src.name}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        return  # no data

    fillvalue = get_fillvalue(src)

    rank = len(src.shape)

    offset = [0,] * rank  # coordinate where we'll copy the source to

    if ctx["extend_dim"]:
        # resize dataset if a dimension is in the extended dimension
        if src.name.split("/")[-1] == ctx["extend_dim"]:
            if ctx["extend_offset"] is not None:
                offset[0] = ctx["extend_offset"]
            else:
                offset[0] = tgt.shape[0]
            new_extent = offset[0] + src.shape[0]
            if new_extent > tgt.shape[0]:
                msg = f"extending {tgt.name} shape to {new_extent}"
                logging.info(msg)
                if ctx["verbose"]:
                    print(msg)
                resize_dataset(tgt, new_extent, axis=0)
        else:
            # check to see if any dimension scale refers to the extend dim
            for dim in range(len(src.dims)):
                dimscales = src.dims[dim]
                for i in range(len(dimscales)):
                    dimscale = dimscales[i]
                    if not dimscale:
                        continue
                    msg = f"dimscale for dim: {dim}: {dimscale}, type: {type(dimscale)}"
                    logging.debug(msg)
                    if dimscale.name.split("/")[-1] == ctx["extend_dim"]:
                        if ctx["extend_offset"] is not None:
                            offset[dim] = ctx["extend_offset"]
                        else:
                            offset[dim] = tgt.shape[dim]
                        new_extent = offset[dim] + src.shape[dim]
                        if new_extent > tgt.shape[dim]:
                            msg = f"extending {tgt.name} shape to {new_extent} "
                            msg += f"for dimension: {dim}"
                            logging.info(msg)
                            if ctx["verbose"]:
                                print(msg)
                            resize_dataset(tgt, new_extent, axis=dim)
    elif len(tgt.shape) > len(src.shape):
        # append mode - extend first dimension by one
        new_extent = tgt.shape[0] + 1
        msg = f"extending first dimension to: {new_extent}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        resize_dataset(tgt, new_extent, axis=0)

    if ctx["dataload"] == "link":
        # don't write chunks, but update chunktable for chunk ref indirect
        if tgt.id.layout and tgt.id.layout["class"] == "H5D_CHUNKED_REF_INDIRECT":
            update_chunktable(src, tgt, ctx)
        else:
            pass # skip chunkterator for link option
        return

    msg = f"iterating over chunks for {src.name}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    try:
        it = ChunkIterator(src)

        logging.debug(f"src dtype: {src.dtype}")
        logging.debug(f"des dtype: {tgt.dtype}")

        for src_s in it:
            logging.debug(f"src selection: {src_s}")
            if rank == 1:
                start = src_s.start + offset[0]
                stop = src_s.stop + offset[0]
                if len(tgt.shape) > rank:
                    tgt_s = []
                    n = tgt.shape[0] - 1
                    tgt_s.append(slice(n, n + 1, 1))
                    tgt_s.append(slice(start, stop, 1))
                    tgt_s = tuple(tgt_s)
                else:
                    tgt_s = slice(start, stop, 1)
            else:
                tgt_s = []
                if len(tgt.shape) > rank:
                    n = tgt.shape[0] - 1
                    tgt_s.append(slice(n, n + 1, 1))

                for dim in range(rank):
                    start = src_s[dim].start + offset[dim]
                    stop = src_s[dim].stop + offset[dim]
                    tgt_s.append(slice(start, stop, 1))
                tgt_s = tuple(tgt_s)
            logging.debug(f"tgt selection: {tgt_s}")

            arr = src[src_s]
            # don't write arr if it's all zeros (or the fillvalue if defined)
            empty_arr = np.zeros(arr.shape, dtype=arr.dtype)
            if fillvalue:
                empty_arr.fill(fillvalue)
            if np.array_equal(arr, empty_arr):
                msg = f"skipping chunk for slice: {src_s}"
            else:
                msg = f"writing dataset data for slice: {src_s}"
                tgt[tgt_s] = arr
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
    except (IOError, TypeError) as e:
        msg = f"ERROR : failed to copy dataset data {src_s}: {e}"
        logging.error(msg)
        if not ctx["ignore_error"]:
            raise
    msg = f"done with dataload for {src.name}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    logging.info("flush fout")


# write_dataset


def create_links(gsrc, gdes, ctx):
    # add soft and external links
    srcid_desobj_map = ctx["srcid_desobj_map"]
    msg = f"create_links: {gsrc.name}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    for title in gsrc:
        msg = f"got link: {title}"
        if ctx["verbose"]:
            print(msg)
        logging.info(msg)
        lnk = gsrc.get(title, getlink=True)
        link_classname = lnk.__class__.__name__
        if link_classname == "HardLink":
            msg = f"Got hardlink: {title} gsrc: {gsrc} gdes: {gdes}"
            logging.debug(msg)
            if title not in gdes:
                msg = f"creating link {gdes} with title: {title}"
                if ctx["verbose"]:
                    print(msg)
                logging.info(msg)
                src_obj_id = gsrc[title].id
                src_obj_id_hash = src_obj_id.__hash__()
                logging.debug(f"got src_obj_id hash: {src_obj_id_hash}")
                if src_obj_id_hash in srcid_desobj_map:
                    des_obj = srcid_desobj_map[src_obj_id_hash]
                    logging.debug(f"creating hardlink to {des_obj.id.id}")
                    gdes[title] = des_obj
                else:
                    # TBD - in hdf5 1.10 it seems that two references to the same object
                    # can return different id's.  This will cause HDF5 files with
                    # multilinks to not load correctly
                    msg = f"could not find map item to src id: {src_obj_id_hash}"
                    logging.warn(msg)
                    if ctx["verbose"]:
                        print("WARNING: " + msg)
        elif link_classname == "SoftLink":
            msg = f"creating SoftLink({lnk.path}) with title: {title}"
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
            if is_h5py(gdes):
                soft_link = h5py.SoftLink(lnk.path)
            else:
                soft_link = h5pyd.SoftLink(lnk.path)
            gdes[title] = soft_link
        elif link_classname == "ExternalLink":
            msg = (
                f"creating ExternalLink({lnk.filename}, {lnk.path}) with title: {title}"
            )
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if is_h5py(gdes):
                ext_link = h5py.ExternalLink(lnk.filename, lnk.path)
            else:
                ext_link = h5pyd.ExternalLink(lnk.filename, lnk.path)
            gdes[title] = ext_link
        else:
            msg = f"Unexpected link type: {lnk.__class__.__name__}"
            logging.warning(msg)
            if ctx["verbose"]:
                print(msg)
    logging.info("flush fout")


# ----------------------------------------------------------------------------------
def create_group(gobj, ctx):
    msg = f"creating group {gobj.name}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]

    grp = None

    if gobj.name in fout:
        grp = fout[gobj.name]
        logging.debug(f"{gobj.name} already exists")
        if ctx["append"]:
            msg = f"skipping creation of group {gobj.name} since already found"
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
        else:
            msg = f"unable to create group {gobj.name}, already present"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
    else:
        if ctx["verbose"]:
            print(f"{gobj.name} not found")

        grp = fout.create_group(gobj.name)
        srcid_desobj_map = ctx["srcid_desobj_map"]
        msg = f"adding group id {gobj.id.id} to {grp} in srcid_desobj_map"
        logging.debug(msg)
        srcid_desobj_map[gobj.id.__hash__()] = grp

    return grp


# create_group


# -----------------------------------------------------------------------------
def create_datatype(obj, ctx):
    msg = f"creating datatype {obj.name}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]
    fout[obj.name] = obj.dtype
    srcid_desobj_map = ctx["srcid_desobj_map"]
    msg = f"adding datatype id {obj.id.id} to {fout[obj.name]} in srcid_desobj_map"
    logging.debug(msg)
    srcid_desobj_map[obj.id.__hash__()] = fout[obj.name]


# create_datatype

# ---------------------------------------------------------------------------------
def load_file(
    fin,
    fout,
    verbose=False,
    dataload="ingest",
    s3path=None,
    compression=None,
    compression_opts=None,
    append=False,
    extend_dim=None,
    extend_offset=0,
    ignore_error=False,
):

    logging.info(f"input file: {fin.filename}")
    logging.info(f"output file: {fout.filename}")
    if dataload != "ingest":
        if not dataload:
            logging.info("no data load")
        elif dataload == "link":
            if not s3path:
                logging.error("s3path expected to be set")
                sys.exit(1)
            logging.info("using s3path")
        else:
            logging.error(f"unexpected dataload value: {dataload}")
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
    ctx["append"] = append
    ctx["extend_dim"] = extend_dim
    ctx["extend_offset"] = extend_offset
    ctx["srcid_desobj_map"] = {}
    ctx["ignore_error"] = ignore_error

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
        logging.info(f"object_link_helper for object: {obj.name}")
        if class_name == "Group":
            # create any soft/external links
            fout = ctx["fout"]
            grp = fout[name]
            create_links(obj, grp, ctx)

    def object_copy_helper(name, obj):
        class_name = obj.__class__.__name__
        logging.debug(f"object_copy_helper for object: {obj.name}")

        if class_name in ("Dataset", "Table"):
            logging.debug(f"calling write_dataset for dataset: {obj.name}")
            tgt = fout[obj.name]
            write_dataset(obj, tgt, ctx)
        elif class_name == "Group":
            logging.debug(f"skip copy for group: {obj.name}")
        elif class_name == "Datatype":
            logging.debug(f"skip copy for datatype: {obj.name}")
        else:
            logging.error(f"no handler for object class: {type(obj)}")

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
