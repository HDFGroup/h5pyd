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

# copy rather than link for any datasets with product of extents less than the following
MIN_DSET_ELEMENTS_FOR_LINKING = 512

# adjust chunk shape to fit between min and max chunk sizes when possible
MIN_CHUNK_SIZE = 1 * 1024 * 1024
MAX_CHUNK_SIZE = 8 * 1024 * 1024
CHUNK_BASE = 64 * 1024    # Multiplier by which chunks are adjusted

H5Z_FILTER_MAP = {
    32001: "blosclz",
    32004: "lz4",
    32008: "bitshuffle",
    32015: "zstd",
}


# check if hdf5 library version supports chunk iteration
hdf_library_version = h5py.version.hdf5_version_tuple
library_has_chunk_iter = (hdf_library_version >= (1, 14, 0) or (
    hdf_library_version < (1, 12, 0) and (hdf_library_version >= (1, 10, 10))))


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
        msg = f"is_regionreference for {val} error: {ae}"
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


def get_reftype(obj):
    if is_h5py(obj):
        ref_type = h5py. special_dtype(ref=h5py.Reference)
    else:
        ref_type = h5pyd.special_dtype(ref=h5pyd.Reference)
    return ref_type


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
    if is_h5py(dset):
        cpl = dset.id.get_create_plist()
        if cpl.get_layout() == h5py.h5d.COMPACT:
            return True
        else:
            return False
    else:
        return False  # compact storage not used with HSDS


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


def get_chunk_layout_class(dset):
    layout_json = get_chunk_layout(dset)
    if layout_json and "class" in layout_json:
        return layout_json["class"]
    else:
        return None


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


def guess_chunk(shape, typesize):
    """ Guess an appropriate chunk layout for a dataset, given its shape and
    the size of each element in bytes.  Will allocate chunks only as large
    as MAX_SIZE.  Chunks are generally close to some power-of-2 fraction of
    each axis, slightly favoring bigger values for the last index.

    Undocumented and subject to change without warning.
    """

    # For unlimited dimensions we have to guess 1024
    shape = tuple((x if x != 0 else 1024) for i, x in enumerate(shape))

    ndims = len(shape)
    if ndims == 0:
        raise ValueError("Chunks not allowed for scalar datasets.")

    chunks = np.array(shape, dtype='=f8')
    if not np.all(np.isfinite(chunks)):
        raise ValueError("Illegal value in chunk tuple")

    # Determine the optimal chunk size in bytes using a PyTables expression.
    # This is kept as a float.
    dset_size = np.prod(chunks) * typesize
    target_size = CHUNK_BASE * (2 ** np.log10(dset_size / (1024. * 1024)))

    if target_size > MIN_CHUNK_SIZE:
        target_size = MAX_CHUNK_SIZE
    elif target_size < MIN_CHUNK_SIZE:
        target_size = MIN_CHUNK_SIZE

    idx = 0
    while True:
        # Repeatedly loop over the axes, dividing them by 2.  Stop when:
        # 1a. We're smaller than the target chunk size, OR
        # 1b. We're within 50% of the target chunk size, AND
        #  2. The chunk is smaller than the maximum chunk size

        chunk_bytes = np.prod(chunks) * typesize

        if (chunk_bytes < target_size or abs(chunk_bytes - target_size) / target_size < 0.5) and \
           chunk_bytes < MAX_CHUNK_SIZE:
            break

        if np.prod(chunks) == 1:
            break  # Element size larger than CHUNK_MAX
        chunks[idx % ndims] = np.ceil(chunks[idx % ndims] / 2.0)
        idx += 1

    return tuple(int(x) for x in chunks)


class ChunkIterator(object):
    """
    Class to iterate through list of chunks of a given dataset
    """

    def __init__(self, dset, source_sel=None):
        self._shape = dset.shape
        rank = len(dset.shape)

        if not dset.chunks:
            # coniguous layout - create some psuedo-chunks so we do't
            # try to read to much data in one selection
            self._layout = guess_chunk(dset.shape, dset.dtype.itemsize)
        elif isinstance(dset.chunks, dict):
            self._layout = dset.chunks["dims"]
        else:
            self._layout = dset.chunks

        if source_sel is None:
            # select over entire dataset
            slices = []
            for dim in range(rank):
                slices.append(slice(0, self._shape[dim]))
            self._sel = tuple(slices)
        else:
            if isinstance(source_sel, slice):
                self._sel = (source_sel,)
            else:
                self._sel = source_sel
        if len(self._sel) != rank:
            raise ValueError(
                "Invalid selection - selection region must have same rank as dataset"
            )
        self._chunk_index = []
        for dim in range(rank):
            s = self._sel[dim]
            if s.start < 0 or s.stop > self._shape[dim] or s.stop <= s.start:
                msg = "Invalid selection - selection region must be within dataset space"
                raise ValueError(msg)
            index = s.start // self._layout[dim]
            self._chunk_index.append(index)

    def __iter__(self):
        return self

    def __next__(self):
        rank = len(self._shape)
        slices = []
        if rank == 0 or self._chunk_index[0] * self._layout[0] >= self._sel[0].stop:
            # ran past the last chunk, end iteration
            raise StopIteration()

        for dim in range(rank):
            s = self._sel[dim]
            start = self._chunk_index[dim] * self._layout[dim]
            stop = (self._chunk_index[dim] + 1) * self._layout[dim]
            # adjust the start if this is an edge chunk
            if start < s.start:
                start = s.start
            if stop > s.stop:
                stop = s.stop  # trim to end of the selection
            s = slice(start, stop, 1)
            slices.append(s)

        # bump up the last index and carry forward if we run outside the selection
        dim = rank - 1
        while dim >= 0:
            s = self._sel[dim]
            self._chunk_index[dim] += 1

            chunk_end = self._chunk_index[dim] * self._layout[dim]
            if chunk_end < s.stop:
                # we still have room to extend along this dimensions
                return tuple(slices)

            if dim > 0:
                # reset to the start and continue iterating with higher dimension
                self._chunk_index[dim] = 0
            dim -= 1
        return tuple(slices)


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

            if ref and val:
                try:
                    fin_obj = fin[val]
                except AttributeError as ae:
                    msg = f"Unable able to get obj for ref value: {ae}"
                    logging.error(msg)
                    if not ctx["ignore_error"]:
                        raise IOError(msg)
                    return None

                h5path = fin_obj.name
                if not h5path:
                    msg = "No path found for ref object"
                    logging.warning(msg)
                    if ctx["verbose"]:
                        print(msg)
                else:
                    fout_obj = fout[h5path]
                    if is_h5py(ctx["fout"]):
                        out = fout_obj.ref
                    else:
                        # convert to string for JSON serialization
                        out = str(fout_obj.ref)

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
        count = int(np.prod(src_arr.shape))
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
    logging.info(msg)

    if ctx["verbose"]:
        print(msg)

    tgtarr = None
    data = srcobj.attrs[name]
    src_dt = None

    # check for non-numpy types that might get returned
    if is_regionreference(data):
        msg = "regionreference types not supported, "
        msg += f"attribute {name} in object {desobj.name} will not be loaded"
        if ctx["verbose"]:
            print(msg)
        logging.warning(msg)
        return

    if is_reference(data):
        src_dt = get_reftype(srcobj)
        tgt_dt = get_reftype(desobj)
        tgt_ref = copy_element(data, src_dt, tgt_dt, ctx)
        try:
            desobj.attrs.create(name, tgt_ref)
        except (IOError, TypeError) as e:
            msg = f"ERROR: failed to create attribute {name} "
            msg += f"of object {desobj.name} for reference type -- {e}"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)

        # done with non-numpy compatible data
        return

    try:
        src_dt = data.dtype
    except AttributeError:
        # convert to numpy type
        data = np.asarray(data)
        src_dt = data.dtype

    if src_dt.kind == "S" and isinstance(data, bytes):
        # check that this is actually utf-encodable
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            msg = f"byte value for attribute {name} in {srcobj.name} "
            msg += "is not utf8 encodable - using surrogateescaping"
            logging.warning(msg)
            if ctx["verbose"]:
                print(msg)
            data = data.decode("utf-8", errors="surrogateescape")
            src_dt = None  # let numpy figure out the unicode type

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
        msg += f"of object {desobj.name} -- {e}"
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
    if dset.shape is None:
        # null space dataset - no data
        return 0
    elif np.prod(dset.shape) == 0:
        # zero extent dataset
        return 0
    elif np.prod(dset.shape) == 1:
        # scalar or extent 1 dataset
        return 1
    elif dset.chunks:
        if is_h5py(dset):
            dsetid = dset.id
            spaceid = dsetid.get_space()
            try:
                num_chunks = dsetid.get_num_chunks(spaceid)
            except ValueError:
                # thrown for compact or contiguous datasets,
                # just treat as 1 chunk
                return 1
        else:
            # for hsds, just return maximum number of chunk in dataset
            chunk_table_dims = get_chunktable_dims(dset)
            num_chunks = np.prod(chunk_table_dims)
    else:
        # use a psuedo-chunk with same dimensions as the dataset
        num_chunks = 1
    return num_chunks


# ----------------------------------------------------------------------------------
def get_dset_offset(dset):
    """ Return dataset file offset for HDF5 contiguous datasets """
    if not is_h5py(dset):
        return -1
    if dset.chunks is not None:
        return -1
    offset = dset.id.get_offset()
    if offset is None:
        return -1
    return offset


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
        chunk_index.append(chunk_offset[i] // chunk_dims[i])
    return tuple(chunk_index)


# ----------------------------------------------------------------------------------
def get_chunk_locations(dset, ctx, include_file_uri=False):
    if not is_h5py(dset):
        msg = "get_chunklocations should only be used with HDF5 datasets"
        logging.error(msg)
        if ctx["ignore_error"]:
            return None
        else:
            raise IOError(msg)

    if dset.chunks is None:
        msg = "get_chunklocations - dataset is not chunked"
        logging.error(msg)
        if ctx["ignore_error"]:
            return None
        else:
            raise IOError(msg)

    rank = len(dset.shape)

    spaceid = dset.id.get_space()
    logging.debug(f"using chunk_iter: {library_has_chunk_iter}")

    dt = get_chunktable_dtype(include_file_uri=include_file_uri)

    chunktable_dims = get_chunktable_dims(dset)
    logging.debug(f"chunktable_dims: {chunktable_dims}")

    if chunktable_dims is None:
        msg = "no chunktable dimension returned"
        logging.error(msg)
        if ctx["ignore_error"]:
            return None
        else:
            raise IOError(msg)

    chunk_arr = np.zeros(chunktable_dims, dtype=dt)

    if include_file_uri:
        s3path = ctx["s3path"].encode("utf-8")
    else:
        s3path = None

    if dset.chunks is None:
        chunk_offset = get_dset_offset(dset)
        if np.prod(chunktable_dims) != 1:
            msg = "Expected one chunk"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
        if chunk_offset <= 0:
            msg = "Expected dset_offset to be greater than zero"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)

        chunk_size = dset.id.get_storage_size()
        if include_file_uri:
            e = (chunk_offset, chunk_size, s3path)
        else:
            e = (chunk_offset, chunk_size)

        chunk_arr[...] = e

        # return one-element array
        return chunk_arr

    # get chunk locations for non-contiguous datasets
    chunk_dims = get_chunk_dims(dset)

    if library_has_chunk_iter:
        def init_chunktable_callback(chunk_info):
            # Use chunk offset as index
            index = get_chunk_table_index(chunk_info[0], chunk_dims)
            byte_offset = chunk_info[2]
            chunk_size = chunk_info[3]

            if not isinstance(index, tuple) or len(index) != rank:
                msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                logging.error(msg)
                raise IOError(msg)

            if include_file_uri:
                e = (byte_offset, chunk_size, s3path)
            else:
                e = (byte_offset, chunk_size)

            chunk_arr[index] = e

        dset.id.chunk_iter(init_chunktable_callback)
    else:
        # Using old HDF5 version without H5Dchunk_iter
        num_chunks = get_num_chunks(dset)

        for i in range(num_chunks):
            chunk_info = dset.id.get_chunk_info(i, spaceid)
            index = get_chunk_table_index(chunk_info[0], chunk_dims)
            byte_offset = chunk_info[2]
            chunk_size = chunk_info[3]
            logging.debug(f"got chunk_info: {chunk_info} for chunk: {i}")
            if not isinstance(index, tuple) or len(index) != rank:
                msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                logging.error(msg)
                raise IOError(msg)

            if include_file_uri:
                e = (byte_offset, chunk_size, s3path)
            else:
                e = (byte_offset, chunk_size)

            chunk_arr[index] = e

            if i % 5000 == 0:
                logging.info(f"{i} chunks indexed")

    return chunk_arr


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
        kwargs = {}
        kwargs["shape"] = chunktable_dims
        kwargs["dtype"] = dt
        kwargs["maxshape"] = chunktable_maxshape
        if ctx["dataload"] == "fastlink" and dset.name:
            kwargs["initializer"] = "chunklocator"
            initializer_opts = []
            initializer_opts.append(f"--h5path={dset.name}")
            linkpath = ctx["s3path"]
            s3prefix = "s3://"
            if linkpath.startswith(s3prefix):
                linkpath = linkpath[len(s3prefix):]
            index = linkpath.find("/")
            if index < 1:
                msg = f"unexpected linkpath: {linkpath}"
                logging.error(msg)
                raise ValueError(msg)
            bucket = linkpath[:index]
            filepath = linkpath[(index + 1):]
            initializer_opts.append(f"--filepath={filepath}")
            initializer_opts.append(f"--bucket={bucket}")
            logging.info(f"using initializer: {initializer_opts}")
            kwargs["initializer_opts"] = initializer_opts

        anon_dset = fout.create_dataset(None, **kwargs)
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
        dset_offset = get_dset_offset(dset)
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
        # get the chunk locations
        chunk_map = {}
        chunk_dims = get_chunk_dims(dset)
        spaceid = dset.id.get_space()

        for i in range(num_chunks):
            chunk_info = dset.id.get_chunk_info(i, spaceid)
            index = chunk_info.chunk_offset
            logging.debug(f"got chunk_info: {chunk_info} for chunk: {i}")
            if not isinstance(index, tuple) or len(index) != rank:
                msg = f"Unexpected array_offset: {index} for dataset with rank: {rank}"
                logging.error(msg)
                if ctx["ignore_error"]:
                    continue
                else:
                    raise IOError(msg)

            chunk_key = ""
            for dim in range(rank):
                chunk_key += str(index[dim] // chunk_dims[dim])
                if dim < rank - 1:
                    chunk_key += "_"
            logging.debug(f"adding chunk_key: {chunk_key}")
            chunk_map[chunk_key] = (chunk_info.byte_offset, chunk_info.size)

        chunks["class"] = "H5D_CHUNKED_REF"
        if not extend:
            chunks["file_uri"] = ctx["s3path"]
        chunks["dims"] = dset.chunks
        chunks["chunks"] = chunk_map

    logging.info(f"using chunk layout: {chunks}")
    return chunks


# ----------------------------------------------------------------------------------
def create_h5image_chunktable(num_bytes, s3path, dataload, fout):
    logging.debug(f"create_h5image_chunktable({num_bytes}, {s3path}")
    CHUNK_SIZE = 2 * 1024 * 1024  # 2MB
    chunks = {}
    num_chunks = -(num_bytes // -CHUNK_SIZE)  # ceil
    if dataload in ("link", "fastlink") and s3path:
        chunks = {"file_uri": s3path}

        if num_chunks <= 0:
            msg = "unexpected error in setting chunks for h5image"
            logging.error(msg)
            raise ValueError(msg)
        elif num_chunks == 1:
            chunks["class"] = "H5D_CONTIGUOUS_REF"
            chunks["offset"] = 0
            chunks["size"] = num_bytes
            logging.debug(f"using chunk layout for link option: {chunks}")
        elif num_chunks <= 100:
            # 2MB - 200 MB
            chunks["class"] = "H5D_CHUNKED_REF"
            chunks["dims"] = [CHUNK_SIZE,]
            # set the chunk locations
            chunk_map = {}
            chunks["dims"] = [CHUNK_SIZE,]
            offset = 0

            for i in range(num_chunks):
                if offset + CHUNK_SIZE > num_bytes:
                    chunk_size = (offset + CHUNK_SIZE) - num_bytes
                else:
                    chunk_size = CHUNK_SIZE
                chunk_map[str(i)] = (offset, chunk_size)
                offset += chunk_size

            chunks["chunks"] = chunk_map
        else:
            # num_chunks > 100
            # create anonymous dataset to hold chunk info
            chunks["class"] = "H5D_CHUNKED_REF_INDIRECT"
            chunks["dims"] = [CHUNK_SIZE,]
            dt = get_chunktable_dtype()

            chunktable_dims = [num_chunks,]
            anon_dset = fout.create_dataset(None, chunktable_dims, dtype=dt)
            msg = f"created chunk table: {anon_dset}"
            logging.info(msg)
            chunks["chunk_table"] = anon_dset.id.id
    else:
        # non-linked case
        chunks["class"] = "H5D_CHUNKED"
        if num_chunks <= 1:
            chunk_shape = [num_bytes,]
        else:
            chunk_shape = [CHUNK_SIZE,]
        chunks["dims"] = chunk_shape

    logging.info(f"using chunk layout: {chunks}")
    return chunks


# ----------------------------------------------------------------------------------
def update_chunktable(src, tgt, ctx):
    tgt_layout = get_chunk_layout(tgt)

    if not tgt_layout:
        raise IOError("expected dataset layout to be set")
    tgt_layout_class = tgt_layout.get("class")
    if tgt_layout_class != "H5D_CHUNKED_REF_INDIRECT":
        logging.info("update_chunktable not supported for this chunk class")
        return
    if ctx["dataload"] == "fastlink":
        logging.info("skip update_chunktable for fastload")
        return
    rank = len(tgt.shape)
    chunktable_id = tgt_layout["chunk_table"]

    fout = ctx["fout"]
    logging.info(f"update_chunk_table {src.name}, id: {src.id.id}")

    # create a numpy array containing chunk refs for each chunk in src array
    extend = True if rank > len(src.shape) else False
    if extend and not ctx["s3path"]:
        msg = "expected s3path to be set for extend mode"
        logging.error(msg)
        if not ctx["ignore_error"]:
            raise IOError(msg)
        return

    chunktable = fout[f"datasets/{chunktable_id}"]
    chunk_arr = get_chunk_locations(src, ctx, include_file_uri=extend)

    msg = f"dataset chunk dimensions {chunktable.shape} not compatible with {chunk_arr.shape}"
    if len(chunktable.shape) == len(chunk_arr.shape):
        if chunktable.shape != chunk_arr.shape:
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return
    elif len(chunk_arr.shape) + 1 == len(chunktable.shape):
        if chunk_arr.shape != chunktable.shape[1:]:
            logging.error(msg)
            return
    else:
        logging.error(msg)
        if not ctx["ignore_error"]:
            raise IOError(msg)
        return

    if not is_h5py(src):
        # hsds dataset
        if not extend:
            msg = "unexpected src type for update_chunktable"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return

        src_layout = get_chunk_layout(src)
        src_layout_class = src_layout["class"]
        if src_layout_class == "H5D_CONTIGUOUS_REF":
            chunk_offset = src_layout["offset"]
            chunk_size = src_layout["size"]
            file_uri = src_layout["file_uri"]
            chunk_arr = [(chunk_offset, chunk_size, file_uri),]
        elif src_layout_class == "H5D_CHUNKED_REF":
            file_uri = src_layout["file_uri"]
            chunkmap = src_layout["chunks"]  # e.g.{'0_2': [4016, 2000000]}}
            for k in chunkmap:
                v = chunkmap[k]
                v.append(file_uri)
                v = tuple(v)
                index = []
                chunk_indices = k.split("_")
                for i in range(len(chunk_indices)):
                    index.append(int(chunk_indices[i]))
                index = tuple(index)
                chunk_arr[index] = v
        elif src_layout_class == "H5D_CHUNKED_REF_INDIRECT":
            file_uri = src_layout["file_uri"]
            orig_chunktable_id = src_layout["chunk_table"]
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
                chunk_arr[it.multi_index] = e
        else:
            msg = f"expected chunk ref class but got: {src_layout_class}"
            logging.error(msg)
            if not ctx["ignore_error"]:
                raise IOError(msg)
            return

    if len(tgt.shape) > len(src.shape):
        # append mode, extend the first dimension of table by one
        extent = chunktable.shape[0] + 1
        resize_dataset(chunktable, extent)
        chunktable[extent - 1, ...] = chunk_arr
    else:
        chunktable[...] = chunk_arr


# ----------------------------------------------------------------------------------
def expandChunk(chunk_shape, max_shape, typesize):
    """Extend the chunk shape until it is above the MIN target."""

    if chunk_shape is None:
        return None

    logging.debug(f"orig chunk_shape: {chunk_shape}")

    rank = len(chunk_shape)

    if rank != len(max_shape):
        raise ValueError("non-compatible arguments to expandChunk")

    if rank == 0:
        # scalar - can't be expanded
        return tuple(chunk_shape)

    chunk_shape = list(chunk_shape).copy()

    while True:
        chunk_size = np.prod(chunk_shape).item() * typesize
        if chunk_size >= MIN_CHUNK_SIZE:
            # this shape works
            break
        if chunk_size == 0:
            # can't do anything with zero-size chunks
            break

        extended = False

        for i in range(rank):
            # start from the low-order dimension
            dim = rank - i - 1
            nextent = chunk_shape[dim]
            if nextent * 2 <= max_shape[dim]:
                chunk_shape[dim] = nextent * 2
                extended = True
                break

        if not extended:
            # unable to increase chunk_shape further
            break

    logging.debug(f"expandChunk - returning {chunk_shape}")

    return tuple(chunk_shape)


# ----------------------------------------------------------------------------------
def create_dataset(dobj, ctx):
    """create a dataset using the properties of the passed in h5py dataset.
    If successful, proceed to copy attributes and data.
    """
    chunks = None
    dset = None
    dset_preappend = None

    msg = f"create_dataset({dobj.name})"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    fout = ctx["fout"]

    if dobj.name in fout:
        dset = fout[dobj.name]
        logging.debug(f"{dobj.name} already exists")
        if ctx["no_clobber"]:
            msg = f"skipping creation of dataset {dobj.name} since already exist and no-clobber option is used"
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
            return
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

        if (ctx["dataload"] in ("link", "fastlink") and (
            not is_vlen(dobj.dtype)) and (
            dobj.shape is not None) and (
            len(dobj.shape) > 0) and (
            not is_compact(dobj)) and (
                np.prod(dobj.shape) > MIN_DSET_ELEMENTS_FOR_LINKING)):

            chunks = create_chunktable(dobj, tgt_shape, ctx)
            logging.debug(f"using chunk layout for link option: {chunks}")

        # use the source object layout if we are not using reference mapping
        if chunks is None and dobj.shape is not None and len(dobj.shape) > 0:
            # converting hsds dset with linked chunks to h5py dataset
            # just use the dims field of dobj.chunks as chunk shape
            chunks = get_chunk_dims(dobj)

        if chunks is not None and not is_h5py(fout):
            # expand chunk if too small

            if dset_preappend is not None:
                # check to see if an extra dimension is needed for the chunk shape
                if isinstance(chunks, dict):
                    # chunktable is already adjusted
                    pass
                else:
                    new_chunks = [1,]
                    new_chunks.extend(chunks)
                    chunks = tuple(new_chunks)
                    logging.debug("extend chunks for preappend:", chunks)
            else:
                if isinstance(chunks, dict):
                    if "dims" in chunks:
                        chunk_dims = chunks["dims"]
                        layout_class = chunks.get("class")
                        server_version = fout.serverver
                        if server_version and server_version.startswith("0.9"):

                            if layout_class == "H5D_CHUNKED_REF_INDIRECT":
                                logging.debug("expand chunks for hyperchunksing")
                                # currently hyperchunks only supported for 1d datasets
                                logging.debug(f"hdf5 chunk dims: {chunk_dims}")
                                chunks["hyper_dims"] = chunk_dims
                                chunk_dims = expandChunk(chunk_dims, dobj.shape, dobj.dtype.itemsize)
                                logging.debug(f"expanded chunks: {chunk_dims}")
                                logging.debug(f"expanded chunks: {chunk_dims}")
                                chunks["dims"] = chunk_dims
                                logging.debug(f"updating for hyper_dims: {chunks}")
                    else:
                        # contiguous or compact, using dataset shape
                        pass
                else:
                    # just a list with chunk shape
                    if len(chunks) == 1:
                        # currently hyperchunks only supported for 1d datasets
                        chunks = expandChunk(chunks, dobj.shape, dobj.dtype.itemsize)
                        logging.debug(f"expanded chunks: {chunks}")

            logging.debug(f"setting chunks kwargs to: {chunks}")

            kwargs["chunks"] = chunks

        if (dobj.shape is None or (len(dobj.shape) == 0) or (
                is_vlen(dobj.dtype) and is_h5py(fout))):
            # don't use compression/chunks for scalar datasets
            # or vlen
            pass
        else:
            logging.debug(f"filter setup for {dobj.name}")
            if not ctx["ignorefilters"]:
                kwargs["compression"] = dobj.compression
                kwargs["compression_opts"] = dobj.compression_opts
                kwargs["shuffle"] = dobj.shuffle

            if ctx["default_compression"] is not None and not kwargs.get("compression"):
                kwargs["compression"] = ctx["default_compression"]
                kwargs["compression_opts"] = ctx["default_compression_opts"]
                if ctx["verbose"]:
                    print("applying default compression filter")

            # TBD: it would be better if HSDS could let us know what filters
            # are supported (like it does with compressors)
            # For now, just hard-ccreate_datasetcreate_datasetode fletcher32 and scaleoffset to be ignored
            if dobj.fletcher32:
                msg = f"fletcher32 filter used by dataset: {dobj.name} is not "
                msg += "supported by HSDS, this filter will not be used"
                logging.warning(msg)
                # kwargs["fletcher32"] = dobj.fletcher32
            if dobj.scaleoffset:
                msg = f"scaleoffset filter used by dataset: {dobj.name} is not "
                msg += "supported by HSDS, this filter will not be used"
                logging.warning(msg)

            if is_h5py(dobj) and not kwargs.get("compression"):
                # apply any custom filters as long as they are supported in HSDS
                for filter_id in dobj._filters:
                    filter_opts = dobj._filters[filter_id]
                    try:
                        filter_id = int(filter_id)
                    except ValueError:
                        msg = "unrecognized filter id: {filter_id} for {dobj.name}, ignoring"
                        logging.warning(msg)

                    if not isinstance(filter_id, int):
                        continue

                    if filter_id in H5Z_FILTER_MAP:
                        filter_name = H5Z_FILTER_MAP[filter_id]
                        if filter_name == "bitshuffle":
                            kwargs["shuffle"] = filter_name
                            logging.info(f"using bitshuffle on {dobj.name}")
                        else:
                            # supported non-standard compressor
                            kwargs["compression"] = filter_name
                            logging.info(f"using compressor: {filter_name} for {dobj.name}")
                            if isinstance(filter_opts, int) or isinstance(filter_opts, dict):
                                kwargs["compression_opts"] = filter_opts
                                logging.info(f"compression_opts: {filter_opts}")
                            else:
                                msg = f"ignoring compression_opts for filter: {filter_name}"
                                logging.warning(msg)
                    else:
                        logging.warning(f"filter id {filter_id} for {dobj.name} not supported")

        # kwargs["scaleoffset"] = dobj.scaleoffset
        # setting the fillvalue is failing in some cases
        # see: https://github.com/HDFGroup/h5pyd/issues/119
        # don't set fill value for reference types
        fillvalue = get_fillvalue(dobj)
        if fillvalue is not None and not has_reference(tgt_dtype):
            logging.debug(f"got fillvalue: {fillvalue}")
            kwargs["fillvalue"] = fillvalue

        # finally, create the dataset
        msg = f"creating dataset {dobj.name}, shape: {dobj.shape}, type: {tgt_dtype}"
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
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

    if not is_h5py(tgt) and get_chunk_layout_class(tgt) != "H5D_CHUNKED":
        # this is one of the ref layouts
        if get_chunk_layout_class(tgt) == "H5D_CHUNKED_REF_INDIRECT":
            # don't write chunks, but update chunktable for chunk ref indirect
            update_chunktable(src, tgt, ctx)
        else:
            pass  # skip chunkterator for link option
        return

    msg = f"iterating over chunks for {src.name}"
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)
    try:
        logging.debug(f"src dtype: {src.dtype}")
        logging.debug(f"des dtype: {tgt.dtype}")

        empty_arr = None
        it = ChunkIterator(src)
        for src_s in it:
            logging.debug(f"src selection: {src_s}")
            if rank == 1 and isinstance(src_s, slice):
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
            if empty_arr is None or empty_arr.shape != arr.shape:
                empty_arr = np.zeros(arr.shape, dtype=arr.dtype)
            if fillvalue:
                empty_arr.fill(fillvalue)

            try:
                is_equal = np.array_equal(arr, empty_arr)
            except ValueError as ve:
                msg = "ValueError on np.array_equal check - assuming not equal"
                logging.warning(f"{msg}: {ve}")
                if ctx["verbose"]:
                    print(msg)
                is_equal = False
            if is_equal:
                msg = f"skipping chunk for slice: {src_s}"
            else:
                msg = f"writing dataset data for slice: {src_s}"
                arr = copy_array(arr, ctx)
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
                    logging.warning(msg)
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
        if ctx["no_clobber"]:
            msg = f"skipping creation of group {gobj.name} since already exists and no-clobber mode is used"
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
            return

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


def get_filesize(f):
    f.seek(0, 2)  # seek to end of file
    num_bytes = f.tell()
    f.seek(0)  # reset synch pointer
    return num_bytes


def load_h5image(
        fin,
        fout,
        verbose=False,
        dataload="ingest",
        s3path=None
):
    num_bytes = get_filesize(fin)
    msg = f"input file: {num_bytes} bytes"
    logging.info(msg)
    if verbose:
        print(msg)

    chunks = create_h5image_chunktable(num_bytes, s3path, dataload, fout)

    dset = fout.create_dataset("h5image", (num_bytes,), chunks=chunks, dtype=np.uint8)
    if dataload == "ingest":
        # copy the file data by pages
        page_size = dset.chunks[0]
        if verbose:
            print(f"page size: {page_size}")
        offset = 0
        while True:
            data = fin.read(page_size)
            if len(data) == 0:
                break
            arr = np.frombuffer(data, dtype=dset.dtype)
            dset[offset:(offset + len(data))] = arr
            offset += len(data)
            msg = f"wrote {len(data)} bytes"
            logging.info(msg)
            if verbose:
                print(msg)

    msg = "load h5imag done"
    logging.info(msg)
    if verbose:
        print(msg)


# ---------------------------------------------------------------------------------
def load_file(
    fin,
    fout,
    verbose=False,
    dataload="ingest",
    s3path=None,
    compression=None,
    compression_opts=None,
    ignorefilters=False,
    append=False,
    no_clobber=False,
    extend_dim=None,
    extend_offset=0,
    ignore_error=False,
):

    logging.info(f"input file: {fin.filename}")
    logging.info(f"output file: {fout.filename}")
    logging.info(f"dataload: {dataload}")
    if dataload != "ingest":
        if not dataload:
            logging.info("no data load")
        elif dataload in ("link", "fastlink"):
            if not s3path:
                logging.error("s3path expected to be set")
                sys.exit(1)
            logging.info(f"using s3path: {s3path}")
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
    ctx["ignorefilters"] = ignorefilters
    ctx["s3path"] = s3path
    ctx["append"] = append
    ctx["no_clobber"] = no_clobber
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
