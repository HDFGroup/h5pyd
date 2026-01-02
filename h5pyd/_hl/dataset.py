##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

from __future__ import absolute_import

import posixpath as pp
import sys
import time
import numpy
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from h5json.hdf5dtype import Reference, RegionReference
from h5json.hdf5dtype import createDataType, check_dtype, special_dtype, guess_dtype, getTypeItem
from h5json.hdf5db import _decode
from h5json.shape_util import getShapeJson, getShapeClass, getMaxDims
from h5json.dset_util import generateLayout
from h5json.filters import getFilterItem, isCompressionFilter
from h5json.array_util import array_for_new_object
from h5json import selections as sel

from .base import HLObject
from .base import Empty
from .objectid import DatasetID
from .datatype import Datatype
from .. import config

_LEGACY_GZIP_COMPRESSION_VALS = frozenset(range(10))
VERBOSE_REFRESH_TIME = 1.0  # 1 second


def readtime_dtype(basetype, names):
    """Make a NumPy dtype appropriate for reading"""
    # Check if basetype is the special case for storing complex numbers
    is_complex_basetype = basetype.names is not None and basetype.names == ("r", "i")
    is_complex_basetype = is_complex_basetype and all(dt.kind == "f" for dt, off in basetype.fields.values())
    is_complex_basetype = is_complex_basetype and basetype.fields["r"][0] == basetype.fields["i"][0]
    if is_complex_basetype:
        itemsize = basetype.itemsize
        if itemsize == 16:
            return numpy.dtype(numpy.complex128)
        elif itemsize == 8:
            return numpy.dtype(numpy.complex64)
        else:
            TypeError(f"Unsupported dtype for complex numbers: {basetype}")

    if len(names) == 0:  # Not compound, or we want all fields
        return basetype

    if basetype.names is None:  # Names provided, but not compound
        raise ValueError("Field names only allowed for compound types")

    for name in names:  # Check all names are legal
        if name not in basetype.names:
            raise ValueError(f"Field {name} does not appear in this type.")

    return numpy.dtype([(name, basetype.fields[name][0]) for name in names])


def make_new_dset(
    parent,
    shape=None,
    dtype=None,
    data=None,
    chunks=None,
    compression=None,
    shuffle=None,
    fletcher32=None,
    maxshape=None,
    compression_opts=None,
    fillvalue=None,
    scaleoffset=None,
    track_order=None,
    track_times=None,
    initializer=None,
    initializer_opts=None
):
    """Return a new low-level dataset identifier

    Only creates anonymous datasets.
    """

    cfg = config.get_config()

    if track_times is not None:
        if track_times not in (True, False):
            raise TypeError("invalid track_times")

    # Convert data to a C-contiguous ndarray
    if data is not None and not isinstance(data, Empty):
        from . import base

        data = array_for_new_object(data, specified_dtype=dtype)

    # Validate shape
    if shape is None:
        if data is None:
            if dtype is None:
                raise TypeError("One of data, shape or dtype must be specified")
            data = Empty(dtype)
        shape = data.shape
    else:
        shape = (shape,) if isinstance(shape, int) else tuple(shape)
        if data is not None and (
            numpy.prod(shape, dtype=numpy.ulonglong) != numpy.prod(data.shape, dtype=numpy.ulonglong)
        ):
            raise ValueError("Shape tuple is incompatible with data")

    shape_json = getShapeJson(shape, maxdims=maxshape)
    if getShapeClass(shape_json) == "H5S_NULL":
        shape = "H5S_NULL"  # pass this to the createDataset method

    if isinstance(dtype, Datatype):
        # Named types are used as-is
        type_json = dtype.id.type_json
    else:
        # Validate dtype
        if dtype is None and data is None:
            dtype = numpy.dtype("=f4")
        elif data is not None:
            if dtype is None:
                if hasattr(data, "dtype"):
                    dtype = data.dtype
                else:
                    dtype = guess_dtype(data)
            else:
                dtype = numpy.dtype(dtype)
                if dtype.kind == "O" and data.dtype.kind == "O" and not dtype.metadata:
                    # use metadata fields from data.dtype if not set in dtype
                    dtype = data.dtype
                else:
                    pass  # just use given dtype
        else:
            dtype = numpy.dtype(dtype)

        if dtype.kind == "O" and dtype.metadata and "ref" in dtype.metadata:
            type_json = {}
            type_json["class"] = "H5T_REFERENCE"
            meta_type = dtype.metadata["ref"]
            if meta_type is Reference:
                type_json["base"] = "H5T_STD_REF_OBJ"
            elif meta_type is RegionReference:
                type_json["base"] = "H5T_STD_REF_DSETREG"
            else:
                errmsg = "Unexpected metadata type"
                raise ValueError(errmsg)
        else:
            type_json = getTypeItem(dtype)

    filters = []
    if shuffle:
        shuffle_filter = getFilterItem("shuffle")
        filters.append(shuffle_filter)

    if scaleoffset:
        scaleoffset_filter = getFilterItem("scaleoffset")
        filters.append(scaleoffset_filter)

    if fletcher32:
        fletcher32_filter = getFilterItem("fletcher32")
        filters.append(fletcher32_filter)

    # Legacy
    if compression is True:
        compression_filter = getFilterItem("gzip")
        if compression_opts:
            compression_filter["level"] = compression_opts
        filters.append(compression_filter)
    elif compression:
        if isinstance(compression, int):
            compression_filter = getFilterItem("gzip")
            compression_filter["level"] = compression
        else:
            compression_filter = getFilterItem(compression)
            if compression_opts:
                compression_filter["level"] = compression_opts
            # TBD: how to set options for non-gzip filters?
        filters.append(compression_filter)
    else:
        pass  # no compression filter

    if filters and chunks is None:
        chunks = True  # specify chunking if filter is used

    # TBD - make these values part of config
    CHUNK_MIN = 512 * 1024  # Soft lower limit (512k)
    CHUNK_MAX = 8096 * 1024  # Hard upper limit (8M)
    kwargs = {"chunk_min": CHUNK_MIN, "chunk_max": CHUNK_MAX, "chunks": chunks}
    layout = generateLayout(shape_json, type_json, **kwargs)

    dcpl = {}  # creation property list
    if layout:
        dcpl["layout"] = layout
    if filters:
        dcpl["filters"] = filters
    if initializer:
        dcpl["initiallizer"] = initializer
    if initializer_opts:
        dcpl["initializer_opts"] = initializer_opts

    if fillvalue is not None:
        # is it compatible with the array type?
        if fillvalue:
            if hasattr(fillvalue, "tolist"):
                # convert numpy object to list
                fillvalue = fillvalue.tolist()
            fillvalue = _decode(fillvalue)
            if not isinstance(fillvalue, str) and hasattr(fillvalue, "__iter__"):
                # fill value is a list, or similar: check that dtype is compound
                if len(fillvalue) != len(dtype):
                    raise ValueError("Invalid fill value for non-compound type dataset")
                fillvalue = list(fillvalue)
            else:
                if len(dtype) > 1:
                    raise ValueError("Invalid fill value for compound type dataset")

            dcpl["fillValue"] = fillvalue

    if track_order or cfg.track_order:
        dcpl["CreateOrder"] = 1

    if chunks and isinstance(chunks, dict):
        dcpl["layout"] = chunks

    if maxshape is not None and len(maxshape) > 0:
        if shape is not None:
            maxshape = tuple(m if m is not None else 0 for m in maxshape)
        else:
            print("maxshape provided but no shape")

    dset_uuid = parent.id.db.createDataset(shape, maxdims=maxshape, dtype=dtype, cpl=dcpl)

    if data is not None and not isinstance(data, Empty):
        # init data
        sel_all = sel.select(tuple(shape), ...)
        parent.id.db.setDatasetValues(dset_uuid, sel_all, data)

    dset = DatasetID(parent, dset_uuid)

    return dset


class AstypeWrapper:
    """Wrapper to convert data on reading from a dataset.
    """
    def __init__(self, dset, dtype):
        self._dset = dset
        self._dtype = numpy.dtype(dtype)

    def __getitem__(self, args):
        return self._dset.__getitem__(args, new_dtype=self._dtype)

    def __len__(self):
        """ Get the length of the underlying dataset

        >>> length = len(dataset.astype('f8'))
        """
        return len(self._dset)

    def __array__(self, dtype=None, copy=True):
        if copy is False:
            raise ValueError(
                f"AstypeWrapper.__array__ received {copy=} "
                f"but memory allocation cannot be avoided on read"
            )

        data = self[:]
        if dtype is not None:
            return data.astype(dtype, copy=False)
        return data


class AsStrWrapper:
    """Wrapper to decode strings on reading the dataset"""

    def __init__(self, dset, encoding, errors="strict"):
        self._dset = dset
        if encoding is None:
            encoding = "ascii"
        self.encoding = encoding
        self.errors = errors

    def __getitem__(self, args):
        bytes_arr = self._dset[args]
        # numpy.char.decode() seems like the obvious thing to use. But it only
        # accepts numpy string arrays, not object arrays of bytes (which we
        # return from HDF5 variable-length strings). And the numpy
        # implementation is not faster than doing it with a loop; in fact, by
        # not converting the result to a numpy unicode array, the
        # naive way can be faster! (Comparing with numpy 1.18.4, June 2020)
        if numpy.isscalar(bytes_arr):
            return bytes_arr.decode(self.encoding, self.errors)

        return numpy.array(
            [b.decode(self.encoding, self.errors) for b in bytes_arr.flat], dtype=object
        ).reshape(bytes_arr.shape)

    def __len__(self):
        """Get the length of the underlying dataset

        >>> length = len(dataset.asstr())
        """
        return len(self._dset)


class FieldsWrapper:
    """Wrapper to extract named fields from a dataset with a struct dtype"""
    extract_field = None

    def __init__(self, dset, prior_dtype, names):
        self._dset = dset
        if isinstance(names, str):
            self.extract_field = names
            names = [names]
        self.read_dtype = readtime_dtype(prior_dtype, names)

    def __array__(self, dtype=None, copy=True):
        if copy is False:
            raise ValueError(
                f"FieldsWrapper.__array__ received {copy=} "
                f"but memory allocation cannot be avoided on read"
            )
        data = self[:]
        if dtype is not None:
            return data.astype(dtype, copy=False)
        else:
            return data

    def __getitem__(self, args):
        data = self._dset.__getitem__(args, new_dtype=self.read_dtype)
        if self.extract_field is not None:
            data = data[self.extract_field]
        return data

    def __len__(self):
        """ Get the length of the underlying dataset

        >>> length = len(dataset.fields(['x', 'y']))
        """
        return len(self._dset)


class ChunkIterator(object):
    """
    Class to iterate through list of chunks of a given dataset
    """

    def __init__(self, dset, source_sel=None):
        self._shape = dset.shape
        rank = len(dset.shape)

        if not dset.chunks:
            # can only use with chunked datasets
            raise TypeError("Chunked dataset required")

        if isinstance(dset.chunks, dict):
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


class Dataset(HLObject):

    """
    Represents an HDF5 dataset
    """

    def astype(self, dtype):
        """Get a context manager allowing you to perform reads to a
        different destination type, e.g.:

        >>> with dataset.astype('f8'):
        ...     double_precision = dataset[0:100:2]
        """
        return AstypeWrapper(self, dtype)

    def asstr(self, encoding=None, errors="strict"):
        """Get a wrapper to read string data as Python strings:

        >>> str_array = dataset.asstr()[:]

        The parameters have the same meaning as in ``bytes.decode()``.
        If ``encoding`` is unspecified, it will use the encoding in the HDF5
        datatype (either ascii or utf-8).
        """
        type_json = self.id.type_json
        if type_json["class"] != "H5T_STRING":
            raise TypeError(
                "dset.asstr() can only be used on datasets with "
                "an HDF5 string datatype"
            )
        if encoding is None:
            if "length" in type_json and type_json["length"] != "H5T_VARIABLE":
                encoding = "utf-8"  # default to utf-8 for fixed length string types
            elif "charSet" in type_json:
                charSet = type_json["charSet"]
                if charSet == "H5T_CSET_UTF8":
                    encoding = "utf-8"
            else:
                # default to ascii for variable length strings
                encoding = "ascii"

        return AsStrWrapper(self, encoding, errors=errors)

    def fields(self, names, *, _prior_dtype=None):
        """Get a wrapper to read a subset of fields from a compound data type:

        >>> 2d_coords = dataset.fields(['x', 'y'])[:]

        If names is a string, a single field is extracted, and the resulting
        arrays will have that dtype. Otherwise, it should be an iterable,
        and the read data will have a compound dtype.
        """
        if _prior_dtype is None:
            _prior_dtype = self.dtype
        return FieldsWrapper(self, _prior_dtype, names)

    @property
    def dims(self):
        from .dims import DimensionManager

        return DimensionManager(self)

    @property
    def ndim(self):
        """Numpy-style attribute giving the number of dimensions"""

        shape = self.shape
        if shape is None:
            return 0
        else:
            return len(shape)

    @property
    def shape(self):
        """Numpy-style shape tuple giving dataset dimensions"""
        # just return the cached shape value
        # (although potentially it could have changed on server)

        shape_json = self.id.shape_json
        if shape_json["class"] == "H5S_NULL":
            dims = None
        elif shape_json["class"] == "H5S_SCALAR":
            dims = ()  # return empty
        else:
            dims = tuple(shape_json["dims"])

        return dims

    @shape.setter
    def shape(self, shape):
        self.resize(shape)

    @property
    def size(self):
        """Numpy-style attribute giving the total dataset size"""
        shape = self.shape
        if shape is None:
            return None
        return numpy.prod(shape, dtype=numpy.int64).item()

    @property
    def nbytes(self):
        """Numpy-style attribute giving the raw dataset size as the number of bytes"""
        size = self.size
        if (size is None):
            # if we are an empty 0-D array, then there are no bytes in the dataset
            return 0
        return self.dtype.itemsize * size

    @property
    def dtype(self):
        """Numpy dtype representing the datatype"""
        return self._dtype

    @property
    def chunks(self):
        """Dataset chunks (or None)"""

        chunks = None
        layout = self.id.layout

        if layout and layout['class'] in ('H5D_CHUNKED', 'H5D_CHUNKED_REF', 'H5D_CHUNKED_REF_INDIRECT'):
            if "dims" in layout:
                chunks = layout['dims']

        if isinstance(chunks, list):
            chunks = tuple(chunks)
        return chunks

    @property
    def compression(self):

        """iterate through list of filters and return compression filter
        or None if none found"""

        compressor = None
        filters = self.id.filters
        for filter in filters:
            if isCompressionFilter(filter):
                compressor = filter["name"]
                break
        return compressor

    @property
    def compression_opts(self):
        """Compression setting.  Int(0-9) for gzip, 2-tuple for szip."""

        opts = None
        filters = self.id.filters

        for filter in filters:
            if isCompressionFilter(filter):
                if "level" in filter:
                    # deflate and lz4 both use this filter option
                    opts = filter["level"]
                elif filter["class"] == "H5Z_FILTER_SZIP":
                    # TBD - what about bitsPerScanline and coding?
                    if "bitsPerPixel" in filter and "pixelsPerBlock" in filter:
                        opts = []
                        opts.append(filter["bitsPerPixel"])
                        opts.append(filter["pixelsPerBlock"])
                        opts = tuple(opts)
                else:
                    pass  # TBD: support other filter opts?

        return opts

    @property
    def shuffle(self):
        """Shuffle filter present (T/F)"""

        filters = self.id.filters
        for filter in filters:
            # check by class or name
            if filter["class"] == "H5Z_FILTER_SHUFFLE":
                return True

        return False

    @property
    def fletcher32(self):
        """Fletcher32 filter is present (T/F)"""

        filters = self.id.filters
        for filter in filters:
            # check by class or name
            if filter["class"] == "H5Z_FILTER_FLETCHER32":
                return True

        return False

    @property
    def scaleoffset(self):
        """Scale/offset filter settings. For integer data types, this is
        the number of bits stored, or 0 for auto-detected. For floating
        point data types, this is the number of decimal places retained.
        If the scale/offset filter is not in use, this is None."""

        filters = self.id.filters
        for filter in filters:
            # check by class or name
            if filter["class"] == "H5Z_FILTER_SCALEOFFSET":
                if "scaleOffset" in filter:
                    return filter["scaleOffset"]
                else:
                    return 0
        return None

    @property
    def maxshape(self):
        """Shape up to which this dataset can be resized.  Axes with value
        None have no resize limit."""

        shape_json = self.id.shape_json
        shape_class = getShapeClass(shape_json)
        if shape_class == "H5S_NULL":
            return None
        elif shape_class == "H5S_SCALAR":
            return ()
        else:
            pass  # H5S_SIMPLE
        dims = getMaxDims(shape_json)

        # HSDS returns H5S_UNLIMITED for unlimited dims
        return tuple(x if (x != 0 and x != "H5S_UNLIMITED") else None for x in dims)

    @property
    def fillvalue(self):
        """Fill value for this dataset (0 by default)"""
        dcpl = self.id.dcpl_json
        if "fillValue" in dcpl:
            fill_value = dcpl["fillValue"]
            if isinstance(fill_value, list):
                # convert to tuple so numpy will do the proper thing for
                # compound types
                fill_value = tuple(fill_value)
            arr = numpy.asarray([fill_value,], dtype=self._dtype)
        else:
            # create default array
            arr = numpy.zeros((1,), dtype=self.dtype)

        return arr[0]

    @property
    def _is_empty(self):
        """check if this is a null-space dataset"""
        return self.shape is None

    @property
    def num_chunks(self):
        """return number of allocated chunks"""
        self._getVerboseInfo()
        return self._num_chunks

    @property
    def allocated_size(self):
        """return storage used by all allocated chunks"""
        self._getVerboseInfo()
        return self._allocated_size

    def __init__(self, bind, track_order=None):
        """Create a new Dataset object by binding to a low-level DatasetID."""

        if not isinstance(bind, DatasetID):
            raise ValueError(f"{bind} is not a DatasetID")
        HLObject.__init__(self, bind, track_order=track_order)

        self._local = None  # local()

        # make a numpy dtype out of the type json
        self._dtype = createDataType(self.id.type_json)
        # self._item_size = getItemSize(self.id.type_json)

        self._num_chunks = None      # additional state we'll get when requested
        self._allocated_size = None  # as above
        self._verboseUpdated = None  # when the verbose data was fetched

        # self._local.astype = None #todo

    def _getVerboseInfo(self):
        now = time.time()
        if (self._verboseUpdated is None or now - self._verboseUpdated > VERBOSE_REFRESH_TIME):
            # resync the verbose data
            rsp_json = {}  # TBD
            if "num_chunks" in rsp_json:
                self._num_chunks = rsp_json["num_chunks"]
            else:
                # not available yet, set to 0
                self._num_chunks = 0
            if "allocated_size" in rsp_json:
                self._allocated_size = rsp_json["allocated_size"]
            else:
                # not available, set to 0
                self._allocated_size = 0
            self._verboseUpdated = now

    def resize(self, size, axis=None):
        """Resize the dataset, or the specified axis.

        The dataset must be stored in chunked format; it can be resized up to
        the "maximum shape" (keyword maxshape) specified at creation time.
        The rank of the dataset cannot be changed.

        "Size" should be a shape tuple, or if an axis is specified, an integer.

        BEWARE: This functions differently than the NumPy resize() method!
        The data is not "reshuffled" to fit in the new shape; each axis is
        grown or shrunk independently.  The coordinates of existing data are
        fixed.
        """

        if self.chunks is None:
            raise TypeError("Only chunked datasets can be resized")

        if axis is not None:
            if not (axis >= 0 and axis < self.id.rank):
                raise ValueError(f"Invalid axis (0 to {self.id.rank - 1} allowed)")
            try:
                newlen = int(size)
            except TypeError:
                raise TypeError("Argument must be a single int if axis is specified")

            size = list(self.shape)
            size[axis] = newlen

        size = tuple(size)

        db = self.id.db

        # update the size
        db.resizeDataset(self.id.uuid, size)

    def __len__(self):
        """The size of the first axis.  TypeError if scalar.

        """
        size = self.len()
        if size > sys.maxsize:
            raise OverflowError(
                "Value too big for Python's __len__; use Dataset.len() instead."
            )
        return size

    def len(self):
        """The size of the first axis.  TypeError if scalar.

        Use of this method is preferred to len(dset), as Python's built-in
        len() cannot handle values greater then 2**32 on 32-bit systems.
        """
        shape = self.shape
        if shape is None or len(shape) == 0:
            raise TypeError("Attempt to take len() of scalar dataset")
        return shape[0]

    def __iter__(self):
        """Iterate over the first axis.  TypeError if scalar.

        BEWARE: Modifications to the yielded data are *NOT* written to file.
        """
        shape = self.shape
        for i in range(shape[0]):
            yield self[i]

    def iter_chunks(self, sel=None):
        """Return chunk iterator.  If set, the sel argument is a slice or
        tuple of slices that defines the region to be used. If not set, the
        entire dataspace will be used for the iterator.

        For each chunk within the given region, the iterator yields a tuple of
        slices that gives the intersection of the given chunk with the
        selection area.

        A TypeError will be raised if the dataset is not chunked.

        A ValueError will be raised if the selection region is invalid.

        """
        if self.ndim < 1:
            raise TypeError("iter_chunks not supported for zero-dimension datasets")
        return ChunkIterator(self, sel)

    def __getitem__(self, args, new_dtype=None):
        """Read a slice from the HDF5 dataset.

        Takes slices and recarray-style field names (more than one is
        allowed!) in any order.  Obeys basic NumPy rules, including
        broadcasting.

        Also supports:

        * Boolean "mask" array indexing
        """
        if new_dtype is not None:
            self.log.debug(f"getitem.new_dtype: {new_dtype}")
        args = args if isinstance(args, tuple) else (args,)
        self.log.debug("dataset.__getitem__")
        for arg in args:
            arg_len = 0
            try:
                arg_len = len(arg)
            except TypeError:
                pass  # ignore
            if arg_len < 3:
                self.log.debug(f"arg: {arg} type: {type(arg)}")
            else:
                self.log.debug(f"arg: [{arg[0]},...] type: {type(arg)}")

        # Sort field indices from the rest of the args.
        names = tuple(x for x in args if isinstance(x, str))
        if names:
            self.log.debug(f"names: {names}")
            # Read a subset of the fields in this structured dtype
            if len(names) == 1:
                names = names[0]  # Read with simpler dtype of this field
            args = tuple(x for x in args if not isinstance(x, str))
            return self.fields(names, _prior_dtype=new_dtype)[args]

        if new_dtype is None:
            new_dtype = self.dtype
        else:
            self.log.debug(f"new_dtype: {new_dtype}")

        """
        new_dtype = getattr(self._local, "astype", None)
        if new_dtype is not None:
            new_dtype = readtime_dtype(new_dtype, names)
        else:
            # This is necessary because in the case of array types, NumPy
            # discards the array information at the top level.
            new_dtype = readtime_dtype(self.dtype, names)
            self.log.debug(f"new_dtype: {new_dtype}")
        """
        if new_dtype.kind == "S" and check_dtype(ref=self.dtype):
            new_dtype = special_dtype(ref=Reference)

        mtype = new_dtype

        db = self.id.db

        # === Special-case region references ====
        """
        TODO
        if len(args) == 1 and isinstance(args[0], h5r.RegionReference):

            obj = h5r.dereference(args[0], self.id)
            if obj != self.id:
                raise ValueError("Region reference must point to this dataset")

            sid = h5r.get_region(args[0], self.id)
            mshape = sel.guess_shape(sid)
            if mshape is None:
                return numpy.array((0,), dtype=new_dtype)
            if numpy.prod(mshape) == 0:
                return numpy.array(mshape, dtype=new_dtype)
            out = numpy.empty(mshape, dtype=new_dtype)
            sid_out = h5s.create_simple(mshape)
            sid_out.select_all()
            self.id.read(sid_out, sid, out, mtype)
            return out
        """

        # === Check for zero-sized datasets =====
        if self._is_empty:
            # These are the only access methods NumPy allows for such objects
            if len(args) == 0 or (len(args) == 1 and args[0] == Ellipsis):
                return Empty(self.dtype)
            raise ValueError("Empty datasets cannot be sliced")

        # === Scalar dataspaces =================

        if self.shape == ():
            selection = sel.select(self, args)
            self.log.info(f"selection.mshape: {selection.mshape}")

            sel_all = sel.select((), ...)
            arr = db.getDatasetValues(self.id.uuid, sel_all)

            if selection.mshape is None:
                msg = f"return scalar selection of: {arr}, dtype: {arr.dtype}, shape: {arr.shape}"
                self.log.info(msg)
                val = arr[()]
                if isinstance(val, str):
                    # h5py always returns bytes, so encode the str
                    # TBD: what about compound types containing strings?
                    val = val.encode("utf-8")
                return val

            return arr

        # === Everything else ===================

        # Perform the dataspace selection
        selection = sel.select(self, args)
        self.log.debug("selection_constructor")

        if selection.nselect == 0:
            # force compliance with h5py selection behavior
            shape = numpy.empty(self.shape)[args].shape
            return numpy.ndarray(shape, dtype=new_dtype)
        # Up-converting to (1,) so that numpy.ndarray correctly creates
        # np.void rows in case of multi-field dtype. (issue 135)
        single_element = selection.mshape == ()
        mshape = (1,) if single_element else selection.mshape

        self.log.debug(f"dataset shape: {self.shape}")
        self.log.debug(f"mshape: {mshape}")

        # Perform the actual read
        fields = None

        if mtype.names != self.dtype.names:
            fields = ":".join(mtype.names)

        if fields:
            raise IOError("field selection not supported yet")  # TBD

        arr = db.getDatasetValues(self.id.uuid, selection)

        return arr

    def __setitem__(self, args, val):
        """Write to the HDF5 dataset from a Numpy array.

        NumPy's broadcasting rules are honored, for "simple" indexing
        (slices and integers).  For advanced indexing, the shapes must
        match.
        """
        self.log.info(f"Dataset __setitem__, args: {args}")

        args = args if isinstance(args, tuple) else (args,)

        # get the val dtype if we're passed a numpy array
        try:
            msg = f"val dtype: {val.dtype}, shape: {val.shape} kind: {val.dtype.kind} metadata: {val.dtype.metadata}"
            self.log.debug(msg)
            if numpy.prod(val.shape) == 0:
                self.log.info("no elements in numpy array, skipping write")
        except AttributeError:
            self.log.debug("val not ndarray")
            pass  # not a numpy object, just leave dtype as None

        if self.shape is None:
            # null space dataset
            if isinstance(val, Empty):
                return  # nothing to do
            else:
                raise TypeError("Unable to assign values to dataset with null shape")

        elif isinstance(val, Empty):
            pass  # no data

        if isinstance(val, Reference):
            # h5pyd References are just strings
            self.log.info("converting Reference to string")
            val = val.tolist()

        # Sort field indices from the slicing
        names = tuple(x for x in args if isinstance(x, str))
        args = tuple(x for x in args if not isinstance(x, str))

        # Generally we try to avoid converting the arrays on the Python
        # side.  However, for compound literals this is unavoidable.
        # For h5pyd, do extra check and convert type on client side for efficiency
        vlen_base_class = check_dtype(vlen=self.dtype)
        if vlen_base_class is not None and vlen_base_class not in (bytes, str):
            self.log.debug(f"asarray to base_class: {vlen_base_class}")
            try:
                # Attempt to directly convert the input array of vlen data to its base class
                val = numpy.asarray(val, dtype=vlen_base_class)

            except (ValueError, TypeError):
                # Failed to convert input array to vlen base class directly, instead create a new array where
                # each element is an array of the Dataset's dtype
                try:
                    # Force output shape
                    tmp = numpy.empty(shape=val.shape, dtype=self.dtype)
                    tmp[:] = [numpy.array(x, dtype=self.dtype) for x in val]
                    val = tmp
                except (ValueError, TypeError):
                    msg = "ValueError converting value element by element"
                    self.log.debug(msg)

            if vlen_base_class == val.dtype:
                if val.ndim > 1:
                    # Reshape array to 2D, where first dim = product of all dims except last, and second dim = last dim
                    # Then flatten it to 1D
                    tmp = numpy.empty(shape=val.shape[:-1], dtype=self.dtype)
                    tmp.ravel()[:] = [
                        i
                        for i in val.reshape(
                            (
                                numpy.prod(val.shape[:-1], dtype=numpy.ulonglong),
                                val.shape[-1],
                            )
                        )
                    ]
                else:
                    tmp = numpy.array([None], dtype=self.dtype)
                    tmp[0] = val
                val = tmp

        elif (
            isinstance(val, complex) or getattr(getattr(val, "dtype", None), "kind", None) == "c"
        ):
            if self.dtype.kind != "V" or self.dtype.names != ("r", "i"):
                msg = f"Wrong dataset dtype for complex number values: {self.dtype.fields}"
                raise TypeError(msg)

            if isinstance(val, complex):
                val = numpy.asarray(val, dtype=type(val))
            tmp = numpy.empty(shape=val.shape, dtype=self.dtype)
            tmp["r"] = val.real
            tmp["i"] = val.imag
            val = tmp

        elif self.dtype.kind == "O" or (
            self.dtype.kind == "V" and (
                               not isinstance(val, numpy.ndarray) or val.dtype.kind != "V"
            ) and (self.dtype.subdtype is None)
        ):
            # TBD: Do we need something like the following in the above if condition:
            # (self.dtype.str != val.dtype.str)
            # for cases where the val is a numpy array but different type than self?

            if len(names) == 1 and self.dtype.fields is not None:
                # Single field selected for write, from a non-array source
                if not names[0] in self.dtype.fields:
                    raise ValueError(f"No such field for indexing: {names[0]}")
                dtype = self.dtype.fields[names[0]][0]
                cast_compound = True
            else:
                dtype = self.dtype
                cast_compound = False

            self.log.debug(f"asarray dtype: {dtype}, cast_compound: {cast_compound}")
            val = numpy.asarray(val, dtype=dtype.base, order="C")
            if cast_compound:
                # val = val.astype(numpy.dtype([(names[0], dtype)]))
                val = val.view(numpy.dtype([(names[0], dtype)]))
                val = val.reshape(val.shape[:len(val.shape) - len(dtype.shape)])

        elif isinstance(val, numpy.ndarray):
            # convert array if needed
            # TBD - need to handle cases where the type shape is different
            self.log.debug("got numpy array")
            if val.dtype != self.dtype and val.dtype.shape == self.dtype.shape:
                self.log.info(f"converting {val.dtype} to {self.dtype}")
                # convert array
                tmp = numpy.empty(val.shape, dtype=self.dtype)
                tmp[...] = val[...]
                val = tmp
        else:
            self.log.debug(f"asarray for {self.dtype}")
            val = numpy.asarray(val, order="C", dtype=self.dtype)

        # Check for array dtype compatibility and convert
        mshape = None
        self.log.debug(f"self.dtype.subdtype: {self.dtype.subdtype}")
        if self.dtype.subdtype is not None:
            shp = self.dtype.subdtype[1]   # type shape
            valshp = val.shape[-len(shp):]
            if valshp != shp:  # Last dimension has to match
                raise TypeError(f"When writing to array types,\
                                 last N dimensions have to match (got {valshp}, but should be {shp})")
            mtype = numpy.dtype((val.dtype, shp))
            self.log.debug(f"mtype for subdtype: {mtype}")
            mshape = val.shape[0:len(val.shape) - len(shp)]

        # Check for field selection
        if len(names) != 0:
            # Catch common errors
            if self.dtype.fields is None:
                raise TypeError("Illegal slicing argument (not a compound dataset)")
            mismatch = [x for x in names if x not in self.dtype.fields]
            if len(mismatch) != 0:
                mismatch = ", ".join(f"{x}" for x in mismatch)
                raise ValueError(f"Illegal slicing argument (fields {mismatch} not in dataset type)")

        # Use mtype derived from array (let DatasetID.write figure it out)

        mshape = val.shape
        self.log.debug(f"mshape: {mshape}")
        self.log.debug(f"data dtype: {val.dtype}")

        # Perform the dataspace selection
        selection = sel.select(self, args)
        self.log.debug(f"selection.mshape: {selection.mshape}")
        if selection.nselect == 0:
            return

        # Broadcast scalars if necessary.

        if mshape == () and selection.mshape is not None and selection.mshape != ():

            if self.dtype.subdtype is not None:
                raise TypeError("Scalar broadcasting is not supported for array dtypes")

            if False:
                # Perform the write, with broadcasting
                pass  # TBD
            else:
                self.log.debug("broadcast scalar on client")
                val2 = numpy.empty(selection.mshape, dtype=val.dtype)
                val2[...] = val
                val = val2
                mshape = val.shape

        db = self.id.db
        db.setDatasetValues(self.id.uuid, selection, val)

    def read_direct(self, dest, source_sel=None, dest_sel=None):
        """Read data directly from HDF5 into an existing NumPy array.

        The destination array must be C-contiguous and writable.
        Selections must be the output of numpy.s_[<args>].

        Broadcasting is supported for simple indexing.
        """

        if self._is_empty:
            raise TypeError("Empty datasets have no numpy representation")
        if not isinstance(dest, numpy.ndarray):
            raise TypeError("Dest must be ndarray")
        if not dest.flags["C_CONTIGUOUS"]:
            raise TypeError("Dest must be C-contiguous array")

        if source_sel is None:
            source_sel = sel.SimpleSelection(self.shape)
        else:
            source_sel = sel.select(self.shape, source_sel)  # for numpy.s_

        if dest_sel is None:
            dest_sel = sel.SimpleSelection(dest.shape)
        else:
            dest_sel = sel.select(dest.shape, dest_sel)

        slices = []
        for i in range(len(dest.shape)):
            start = dest_sel.start[i]
            stop = start + dest_sel.count[i]
            step = dest_sel.step[i]
            slices.append(slice(start, stop, step))
        slices = tuple(slices)

        if source_sel.getSelectNpoints() != dest_sel.getSelectNpoints():
            raise TypeError("Invalid shape")

        arr = self.__getitem__(source_sel)
        dest.__setitem__(slices, arr)

    def write_direct(self, source, source_sel=None, dest_sel=None):
        """Write data directly to HDF5 from a NumPy array.

        The source array must be C-contiguous.  Selections must be
        the output of numpy.s_[<args>].

        Broadcasting is supported for simple indexing.
        """

        if self._is_empty:
            raise TypeError("Empty datasets cannot be written to")
        if not isinstance(source, numpy.ndarray):
            raise TypeError("Source must be ndarray")
        if not source.flags["C_CONTIGUOUS"]:
            raise TypeError("Source must be C-contiguous array")

        if dest_sel is None:
            dest_sel = sel.SimpleSelection(self.shape)
        else:
            dest_sel = sel.select(self.shape, dest_sel)

        if source_sel is None:
            source_sel = sel.SimpleSelection(source.shape)
        else:
            source_sel = sel.select(source.shape, source_sel)  # for numpy.s_

        slices = []
        for i in range(len(source.shape)):
            start = source_sel.start[i]
            stop = start + source_sel.count[i]
            step = source_sel.step[i]
            slices.append(slice(start, stop, step))
        slices = tuple(slices)

        if source_sel.getSelectNpoints() != dest_sel.getSelectNpoints():
            raise TypeError("Invalid shape")

        data = source.__getitem__(slices)
        self.__setitem__(dest_sel, data)

    def __array__(self, dtype=None, copy=True):
        if copy is False:
            raise ValueError(
                f"AstypeWrapper.__array__ received {copy=} "
                f"but memory allocation cannot be avoided on read"
            )

        # Special case for (0,)*-shape datasets
        if self.shape is None or numpy.prod(self.shape) == 0:
            return numpy.empty(self.shape, dtype=self.dtype if dtype is None else dtype)

        data = self[:]
        if dtype is not None:
            return data.astype(dtype, copy=False)
        return data

    def __repr__(self):
        if not self:
            r = "<Closed HDF5 dataset>"
        else:
            if self.name is None:
                namestr = '("anonymous")'
            else:
                name = pp.basename(pp.normpath(self.name))
                if name:
                    namestr = f'"{name}"'
                else:
                    namestr = "/"
            r = f'<HDF5 dataset {namestr}: shape {self.shape}, type "{self.dtype.str}">'
        return r

    def refresh(self):
        """Refresh the dataset metadata by reloading from the file.
        """
        self.id.refresh()
        self._num_chunks = None  # additional state we'll get when requested
        self._allocated_size = None  # as above
        self._verboseUpdated = None  # when the verbose data was fetched

    def make_scale(self, name=""):
        """Make this dataset an HDF5 dimension scale.

        You can then attach it to dimensions of other datasets like this:

            other_ds.dims[0].attach_scale(ds)

        You can optionally pass a name to associate with this scale.
        """
        self.dims.create_scale(self, name=name)

    """
      Convert a list to a tuple, recursively.
      Example. [[1,2],[3,4]] -> ((1,2),(3,4))
    """

    def toTuple(self, data):
        if type(data) in (list, tuple):
            return tuple(self.toTuple(x) for x in data)
        else:
            return data


class MultiManager():
    """
    high-level object to support slicing operations
    that map to H5Dread_multi/H5Dwrite_multi
    """
    # Avoid overtaxing HSDS
    max_workers = 16

    def __init__(self, datasets=None, logger=None):
        if (datasets is None) or (len(datasets) == 0):
            raise ValueError("MultiManager requires non-empty list of datasets")
        self.datasets = datasets
        if logger is None:
            self.log = logging
        else:
            self.log = logging.getLogger(logger)

    def read_dset_tl(self, args):
        """
        Thread-local method to read from a single dataset
        """
        dset = args[0]
        idx = args[1]
        try:
            read_args = args[2]
        except Exception as e:
            raise e
        return (idx, dset[read_args])

    def write_dset_tl(self, args):
        """
        Thread-local method to write to a single dataset
        """
        dset = args[0]
        # idx = args[1]
        write_args = args[2]
        write_vals = args[3]
        try:
            dset[write_args] = write_vals
        except Exception as e:
            raise e
        return

    def __getitem__(self, args):
        """
        Read the same slice from each of the datasets
        managed by this MultiManager.
        """
        # Spread requests out evenly among all available SNs

        # TODO: This should eventually be handled at the config/HTTPConn level
        try:
            num_endpoints = int(os.environ["SN_CORES"])
            port_range = os.environ["SN_PORT_RANGE"]
            ports = port_range.split('-')

            if len(ports) != 2:
                raise ValueError("Malformed SN_PORT_RANGE")

            low_port = int(ports[0])
            high_port = int(ports[1])

        except Exception as e:
            msg = f"{e}: Defaulting Number of SN_COREs to 1"
            self.log.debug(msg)
            num_endpoints = 1

        if (num_endpoints > 1):
            next_port = low_port
            port_len = len(ports[0])

            for i, dset in enumerate(self.datasets):
                endpt = dset.id.http_conn._endpoint
                endpt = endpt[:len(endpt) - port_len] + str(next_port)
                dset.id.http_conn._endpoint = endpt
                next_port += 1

                if next_port > high_port:
                    next_port = low_port

        # TODO: Handle the case where some or all datasets share an HTTPConn object

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Unwrap one-selection list
            if (isinstance(args, list) and len(args) == 1):
                args = args[0]

            if not isinstance(args, list):
                read_futures = [executor.submit(self.read_dset_tl,
                                (self.datasets[i], i, args)) for i in range(len(self.datasets))]
            elif isinstance(args, list) and len(args) == len(self.datasets):
                read_futures = [executor.submit(self.read_dset_tl,
                                (self.datasets[i], i, args[i])) for i in range(len(self.datasets))]
            else:
                raise ValueError("Number of selections must be one or equal number of datasets")

            ret_data = [None] * len(self.datasets)

            for future in as_completed(read_futures):
                try:
                    result = future.result()
                    idx = result[0]
                    dset_data = result[1]
                    ret_data[idx] = dset_data
                except Exception as exc:
                    executor.shutdown(wait=False)
                    raise ValueError(f"Error during multi-read: {exc}")
            return ret_data

    def __setitem__(self, args, vals):
        """
        Write to the provided slice of each dataset
        managed by this MultiManager.
        """
        # TODO: This should eventually be handled at the config/HTTPConn level
        try:
            num_endpoints = int(os.environ["SN_CORES"])
            port_range = os.environ["SN_PORT_RANGE"]
            ports = port_range.split('-')

            if len(ports) != 2:
                raise ValueError("Malformed SN_PORT_RANGE")

            low_port = int(ports[0])
            high_port = int(ports[1])

            if (high_port - low_port) != num_endpoints - 1:
                raise ValueError("Malformed port range specification; must be sequential ports")

        except Exception as e:
            msg = f"{e}: Defaulting Number of SN_COREs to 1"
            self.log.debug(msg)
            num_endpoints = 1

        # TODO: Handle the case where some or all datasets share an HTTPConn object
        # For now, assume each connection is distinct
        if (num_endpoints > 1):
            next_port = low_port
            port_len = len(ports[0])

            for i, dset in enumerate(self.datasets):
                endpt = dset.id.http_conn._endpoint
                endpt = endpt[:len(endpt) - port_len] + str(next_port)
                dset.id.http_conn._endpoint = endpt
                next_port += 1

                if next_port > high_port:
                    next_port = low_port

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Unwrap one-selection list
            if (isinstance(args, list) and len(args) == 1):
                args = args[0]

            if not isinstance(args, list):
                write_futures = [executor.submit(self.write_dset_tl,
                                 (self.datasets[i], i, args, vals[i])) for i in range(len(self.datasets))]
            elif isinstance(args, list) and len(args) == len(self.datasets):
                write_futures = [executor.submit(self.write_dset_tl,
                                 (self.datasets[i], i, args[i], vals[i])) for i in range(len(self.datasets))]
            else:
                raise ValueError("Number of selections must be one or equal to number of datasets")

            for future in as_completed(write_futures):
                try:
                    future.result()
                except Exception as exc:
                    executor.shutdown(wait=False)
                    raise ValueError(f"Error during multi-write: {exc}")
            return
