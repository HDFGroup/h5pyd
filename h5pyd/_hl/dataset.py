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
from copy import copy
import sys
import time
import base64
import numpy

from .base import HLObject, jsonToArray, bytesToArray, arrayToBytes
from .base import Empty, guess_dtype
from .h5type import Reference, RegionReference
from .base import _decode
from .objectid import DatasetID
from . import filters
from . import selections as sel
from .datatype import Datatype
from .h5type import getTypeItem, createDataType, check_dtype, special_dtype, getItemSize

_LEGACY_GZIP_COMPRESSION_VALS = frozenset(range(10))
VERBOSE_REFRESH_TIME = 1.0  # 1 second


def readtime_dtype(basetype, names):
    """Make a NumPy dtype appropriate for reading"""
    # Check if basetype is the special case for storing complex numbers
    if (
        basetype.names is not None
        and basetype.names == ("r", "i")
        and all(dt.kind == "f" for dt, off in basetype.fields.values())
        and basetype.fields["r"][0] == basetype.fields["i"][0]
    ):
        itemsize = basetype.itemsize
        if itemsize == 16:
            return numpy.dtype(numpy.complex128)
        elif itemsize == 8:
            return numpy.dtype(numpy.complex64)
        else:
            TypeError("Unsupported dtype for complex numbers: %s" % basetype)

    if len(names) == 0:  # Not compound, or we want all fields
        return basetype

    if basetype.names is None:  # Names provided, but not compound
        raise ValueError("Field names only allowed for compound types")

    for name in names:  # Check all names are legal
        if name not in basetype.names:
            raise ValueError("Field %s does not appear in this type." % name)

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
    track_times=None,
):
    """Return a new low-level dataset identifier

    Only creates anonymous datasets.
    """

    # fill in fields for the body of the POST request as we got
    body = {}

    # Convert data to a C-contiguous ndarray
    if data is not None and not isinstance(data, Empty):
        from . import base

        data = base.array_for_new_object(data, specified_dtype=dtype)

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
            numpy.product(shape, dtype=numpy.ulonglong)
            != numpy.product(data.shape, dtype=numpy.ulonglong)
        ):
            raise ValueError("Shape tuple is incompatible with data")

    if shape is None:
        body["shape"] = "H5S_NULL"
    else:
        body["shape"] = shape

    if track_times is not None:
        if track_times not in (True, False):
            raise TypeError("invalid track_times")

    if isinstance(maxshape, int):
        maxshape = (maxshape,)
    tmp_shape = maxshape if maxshape is not None else shape

    # Validate chunk shape
    if isinstance(chunks, int) and not isinstance(chunks, bool):
        chunks = (chunks,)
    if isinstance(chunks, tuple) and any(
        chunk > dim for dim, chunk in zip(tmp_shape, chunks) if dim is not None
    ):
        errmsg = (
            f"Chunk shape must not be greater than data shape in any dimension. "
            "{chunks} is not compatible with {shape}"
        )
        raise ValueError(errmsg)

    # validate chunks is not False if maxshape or compression is set
    if chunks is False:
        if maxshape is not None:
            raise ValueError("chunks must not be False with extendible datasets")
        if compression is not None:
            raise ValueError("chunks must not be False with compression")

    if chunks and shape is None and (data is None or isinstance(data, Empty)):
        raise TypeError("Chunk layout may not be specified with empty dataset")

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
    body["type"] = type_json

    layout = None
    if chunks is not None and isinstance(chunks, dict):
        # use the given chunk layout
        layout = chunks
        chunks = None

    if isinstance(dtype, Datatype):
        # Named types are used as-is
        type_json = dtype.id.type_json
    else:
        # Validate dtype
        if dtype is None:
            dtype = numpy.dtype("=f4")
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
            # tid = h5t.py_create(dtype, logical=1)
    body["type"] = type_json

    compressors = parent.id.http_conn.compressors

    # Legacy
    if compression is True:
        if compression_opts is None:
            compression_opts = 4
        compression = "gzip"

    # Legacy
    if compression in _LEGACY_GZIP_COMPRESSION_VALS:
        if compression_opts is not None:
            raise TypeError("Conflict in compression options")
        compression_opts = compression
        compression = "gzip"

    if compression:
        if isinstance(compression, int):
            if compression < 0:
                raise ValueError(f"Invalid filter: {compression}")
            if compression not in range(0, 10):
                raise ValueError(f"Unknown compression: {compression}")
            compression_opts = compression
            compression = "gzip"
        elif compression not in compressors:
            msg = "Unknown compression, expect one of the following "
            msg += f"values: {compressors}"
            raise ValueError(msg)

    dcpl = filters.generate_dcpl(
        shape,
        dtype,
        chunks,
        compression,
        compression_opts,
        shuffle,
        fletcher32,
        maxshape,
        scaleoffset,
        layout,
    )

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

    if chunks and isinstance(chunks, dict):
        dcpl["layout"] = chunks

    body["creationProperties"] = dcpl

    """
    if track_times in (True, False):
        dcpl.set_obj_track_times(track_times)
    elif track_times is not None:
        raise TypeError("track_times must be either True or False")
    """
    if maxshape is not None and len(maxshape) > 0:
        if shape is not None:
            maxshape = tuple(m if m is not None else 0 for m in maxshape)
            body["maxdims"] = maxshape
        else:
            print("Warning: maxshape provided but no shape")

    req = "/datasets"

    rsp = parent.POST(req, body=body)

    json_rep = {}
    json_rep["id"] = rsp["id"]

    req = "/datasets/" + rsp["id"]
    rsp = parent.GET(req)

    json_rep["shape"] = rsp["shape"]
    json_rep["type"] = rsp["type"]
    json_rep["lastModified"] = rsp["lastModified"]
    if "creationProperties" in rsp:
        json_rep["creationProperties"] = rsp["creationProperties"]
    else:
        json_rep["creationProperties"] = {}
    if "layout" in rsp:
        json_rep["layout"] = rsp["layout"]

    dset_id = DatasetID(parent, json_rep)

    if data is not None:
        # init data
        dset = Dataset(dset_id)
        dset[...] = data

    return dset_id


class AstypeWrapper(object):
    """Wrapper to convert data on reading from a dataset."""

    def __init__(self, dset, dtype):
        self._dset = dset
        self._dtype = numpy.dtype(dtype)

    def __getitem__(self, args):
        return self._dset.__getitem__(args, new_dtype=self._dtype)

    def __enter__(self):
        # pylint: disable=protected-access
        print(
            "Using astype() as a context manager is deprecated. "
            "Slice the returned object instead, like: ds.astype(np.int32)[:10]"
        )
        self._dset._local.astype = self._dtype
        return self

    def __exit__(self, *args):
        # pylint: disable=protected-access
        self._dset._local.astype = None

    def __len__(self):
        """Get the length of the underlying dataset

        >>> length = len(dataset.astype('f8'))
        """
        return len(self._dset)


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


class ChunkIterator(object):
    """
    Class to iterate through list of chunks of a given dataset
    """

    def __init__(self, dset, source_sel=None):
        self._shape = dset.shape
        rank = len(dset.shape)

        if not dset.chunks:
            # can only use with chunked datasets
            # (currently all datasets are chunked, but check for future compat)
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
                raise ValueError(
                    "Invalid selection - selection region must be within dataset space"
                )
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

    @property
    def dims(self):
        from .dims import DimensionManager

        return DimensionManager(self)

    @property
    def ndim(self):
        """Numpy-style attribute giving the number of dimensions"""
        if self._shape is None:
            return 0
        else:
            return len(self._shape)

    @property
    def shape(self):
        """Numpy-style shape tuple giving dataset dimensions"""
        # just return the cached shape value
        # (although potentially it could have changed on server)
        return self._shape

    def get_shape(self, check_server=False):
        # this version will optionally refetch the shape from the server
        # (if the dataset is resiable)
        shape_json = self.id.shape_json
        if shape_json["class"] == "H5S_NULL":
            return None
        if shape_json["class"] == "H5S_SCALAR":
            return ()  # return empty

        if "maxdims" not in shape_json or not check_server:
            # not resizable, just return dims
            dims = shape_json["dims"]
        else:
            # resizable, retrieve current shape
            req = "/datasets/" + self.id.uuid + "/shape"
            rsp = self.GET(req)
            shape_json = rsp["shape"]
            dims = shape_json["dims"]
        self._shape = tuple(dims)
        return self._shape

    @shape.setter
    def shape(self, shape):
        self.resize(shape)

    @property
    def size(self):
        """Numpy-style attribute giving the total dataset size"""
        if self._shape is None:
            return None
        return numpy.prod(self._shape).item()

    @property
    def nbytes(self):
        """Numpy-style attribute giving the raw dataset size as the number of bytes"""
        size = self.size
        if (
            size is None
        ):  # if we are an empty 0-D array, then there are no bytes in the dataset
            return 0
        return self.dtype.itemsize * size

    @property
    def dtype(self):
        """Numpy dtype representing the datatype"""
        return self._dtype

    @property
    def value(self):
        """Alias for dataset[()]"""
        DeprecationWarning(
            "dataset.value has been deprecated. " "Use dataset[()] instead."
        )
        return self[()]

    @property
    def chunks(self):
        """Dataset chunks (or None)"""
        ret = self.id.chunks
        if isinstance(ret, list):
            ret = tuple(ret)
        return ret

    @property
    def compression(self):

        """iterate through list of filters and return compression filter
        or None if none found"""
        compressors = self.id.http_conn.compressors
        for filter in self._filters:
            if isinstance(filter, str):
                filter_name = filter
            elif isinstance(filter, dict) and "name" in filter:
                filter_name = filter["name"]
            else:
                filter_name = None

            if filter_name and filter_name in compressors:
                return filter_name
        return None

    @property
    def compression_opts(self):
        """Compression setting.  Int(0-9) for gzip, 2-tuple for szip."""
        compressors = self.id.http_conn.compressors
        for filter in self._filters:
            if isinstance(filter, str):
                return None  # compression filter, but no options
            elif isinstance(filter, dict) and "name" in filter:
                filter_name = filter["name"]
                if filter_name not in compressors:
                    continue
                if filter_name == "szip":
                    opt_keys = (
                        "bitsPerPixel",
                        "coding",
                        "pixelsPerBlock",
                        "pixelsPerScanline",
                    )
                    opt = []
                    for opt_key in opt_keys:
                        if opt_key in filter:
                            opt.append(filter[opt_key])
                    if len(opt) == len(opt_keys):
                        # expected number of options
                        return tuple(opt)
                    else:
                        return None
                if "level" in filter:
                    # just return level as an int
                    return filter["level"]
                else:
                    return None

        return None

    @property
    def shuffle(self):
        """Shuffle filter present (T/F)"""
        for filter in self._filters:
            # check by class or name
            if "class" in filter and filter["class"] == "H5Z_FILTER_SHUFFLE":
                return True
            if "name" in filter and filter["name"] == "shuffle":
                return True

        return False

    @property
    def fletcher32(self):
        """Fletcher32 filter is present (T/F)"""
        return "fletcher32" in self._filters
        for filter in self._filters:
            # check by class or name
            if "class" in filter and filter["class"] == "H5Z_FILTER_FLETCHER32":
                return True
            if "name" in filter and filter["name"] == "fletcher32":
                return True

        return False

    @property
    def scaleoffset(self):
        """Scale/offset filter settings. For integer data types, this is
        the number of bits stored, or 0 for auto-detected. For floating
        point data types, this is the number of decimal places retained.
        If the scale/offset filter is not in use, this is None."""
        for filter in self._filters:
            # check by class or name
            if ("class" in filter and filter["class"] == "H5Z_FILTER_SCALEOFFSET") or (
                "name" in filter and filter["name"] == "scaleoffset"
            ):
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
        if self.id.shape_json["class"] == "H5S_SCALAR":
            return ()  # empty tuple

        if "maxdims" not in shape_json:
            # not resizable, just return dims
            dims = shape_json["dims"]
        else:
            dims = shape_json["maxdims"]

        # HSDS returns H5S_UNLIMITED for ulimitied dims, h5serv, returns 0
        return tuple(x if (x != 0 and x != "H5S_UNLIMITED") else None for x in dims)
        # dims = space.get_simple_extent_dims(True)
        # return tuple(x if x != h5s.UNLIMITED else None for x in dims)

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
            arr = numpy.asarray(
                [
                    fill_value,
                ],
                dtype=self._dtype,
            )
        else:
            # create default array
            arr = numpy.zeros((1,), dtype=self.dtype)

        return arr[0]

    @property
    def _is_empty(self):
        """check if this is a null-space datset"""
        return self._shape is None

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

    def __init__(self, bind):
        """Create a new Dataset object by binding to a low-level DatasetID."""

        if not isinstance(bind, DatasetID):
            raise ValueError("%s is not a DatasetID" % bind)
        HLObject.__init__(self, bind)

        self._dcpl = self.id.dcpl_json
        self._filters = filters.get_filters(self._dcpl)

        self._local = None  # local()

        # make a numpy dtype out of the type json
        self._dtype = createDataType(self.id.type_json)
        self._item_size = getItemSize(self.id.type_json)

        self._shape = self.get_shape()

        self._num_chunks = None  # aditional state we'll get when requested
        self._allocated_size = None  # as above
        self._verboseUpdated = None  # when the verbose data was fetched

        # self._local.astype = None #todo

    def _getVerboseInfo(self):
        now = time.time()
        if (
            self._verboseUpdated is None
            or now - self._verboseUpdated > VERBOSE_REFRESH_TIME
        ):
            # resynch the verbose data
            req = "/datasets/" + self.id.uuid + "?verbose=1"
            rsp_json = self.GET(req)
            if "num_chunks" in rsp_json:
                self._num_chunks = rsp_json["num_chunks"]
            else:
                # not avaailable yet, set to 0
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
                raise ValueError("Invalid axis (0 to %s allowed)" % (self.id.rank - 1))
            try:
                newlen = int(size)
            except TypeError:
                raise TypeError("Argument must be a single int if axis is specified")

            size = list(self._shape)
            size[axis] = newlen

        size = tuple(size)

        # send the request to the server
        body = {"shape": size}
        req = "/datasets/" + self.id.uuid + "/shape"
        self.PUT(req, body=body)
        # self.id.set_extent(size)
        # h5f.flush(self.id)  # THG recommends
        self._shape = size  # save the new shape

    def __len__(self):
        """The size of the first axis.  TypeError if scalar.

        Limited to 2**32 on 32-bit systems; Dataset.len() is preferred.
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
        shape = self._shape
        if shape is None or len(shape) == 0:
            raise TypeError("Attempt to take len() of scalar dataset")
        return shape[0]

    def __iter__(self):
        """Iterate over the first axis.  TypeError if scalar.

        BEWARE: Modifications to the yielded data are *NOT* written to file.
        """
        shape = self._shape
        # to reduce round trips, grab BUFFER_SIZE items at a time
        # TBD: set buffersize based on size of each row
        BUFFER_SIZE = 1000

        arr = None
        self.log.info("__iter__")
        if len(shape) == 0:
            raise TypeError("Can't iterate over a scalar dataset")
        for i in range(shape[0]):
            if i % BUFFER_SIZE == 0:
                # grab another buffer
                numrows = BUFFER_SIZE
                if shape[0] - i < numrows:
                    numrows = shape[0] - i
                self.log.debug("get {} iter items".format(numrows))
                arr = self[i : numrows + i]

            yield arr[i % BUFFER_SIZE]

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

    def _getQueryParam(self, start, stop, step=None):
        param = ""
        rank = len(self._shape)
        if rank == 0:
            return None
        if step is None:
            step = (1,) * rank
        param += "["
        for i in range(rank):
            field = "{}:{}:{}".format(start[i], stop[i], step[i])
            param += field
            if i != (rank - 1):
                param += ","
        param += "]"
        return param

    def __getitem__(self, args, new_dtype=None):
        """Read a slice from the HDF5 dataset.

        Takes slices and recarray-style field names (more than one is
        allowed!) in any order.  Obeys basic NumPy rules, including
        broadcasting.

        Also supports:

        * Boolean "mask" array indexing
        """
        if new_dtype is not None:
            self.log.warning("new_dtype is not supported")
        args = args if isinstance(args, tuple) else (args,)
        self.log.debug("dataset.__getitem__")
        for arg in args:
            arg_len = 0
            try:
                arg_len = len(arg)
            except TypeError:
                pass  # ignore
            if arg_len < 3:
                self.log.debug("arg: {} type: {}".format(arg, type(arg)))
            else:
                self.log.debug("arg: [{},...] type: {}".format(arg[0], type(arg)))

        # Sort field indices from the rest of the args.
        names = tuple(x for x in args if isinstance(x, str))
        args = tuple(x for x in args if not isinstance(x, str))

        new_dtype = getattr(self._local, "astype", None)
        if new_dtype is not None:
            new_dtype = readtime_dtype(new_dtype, names)
        else:
            # This is necessary because in the case of array types, NumPy
            # discards the array information at the top level.
            new_dtype = readtime_dtype(self.dtype, names)
            self.log.debug("new_dtype: {}".format(new_dtype))
        if new_dtype.kind == "S" and check_dtype(ref=self.dtype):
            new_dtype = special_dtype(ref=Reference)

        mtype = new_dtype

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
            if numpy.product(mshape) == 0:
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

        if self._shape == ():
            selection = sel.select(self, args)
            self.log.info("selection.mshape: {}".format(selection.mshape))

            # TBD - refactor the following with the code for the non-scalar case
            req = "/datasets/" + self.id.uuid + "/value"
            rsp = self.GET(req, format="binary")

            if isinstance(rsp, bytes):
                # got binary response
                self.log.info("got binary response for scalar selection")
                # arr = numpy.frombuffer(rsp, dtype=new_dtype)
                arr = bytesToArray(rsp, new_dtype, self._shape)

                if not self.dtype.shape:
                    self.log.debug("reshape arr to: {}".format(self._shape))
                    arr = numpy.reshape(arr, self._shape)
            else:
                # got JSON response
                # need some special conversion for compound types --
                # each element must be a tuple, but the JSON decoder
                # gives us a list instead.
                data = rsp["value"]
                self.log.info("got json response for scalar selection")
                if len(mtype) > 1 and type(data) in (list, tuple):
                    converted_data = []
                    for i in range(len(data)):
                        converted_data.append(self.toTuple(data[i]))
                    data = tuple(converted_data)

                arr = numpy.empty((), dtype=new_dtype)
                arr[()] = data
            if selection.mshape is None:
                self.log.info(
                    "return scalar selection of: {}, dtype: {}, shape: {}".format(
                        arr, arr.dtype, arr.shape
                    )
                )
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
            return numpy.ndarray(selection.mshape, dtype=new_dtype)
        # Up-converting to (1,) so that numpy.ndarray correctly creates
        # np.void rows in case of multi-field dtype. (issue 135)
        single_element = selection.mshape == ()
        mshape = (1,) if single_element else selection.mshape

        rank = len(self._shape)

        self.log.debug("dataset shape: {}".format(self._shape))
        self.log.debug("mshape: {}".format(mshape))
        self.log.debug("single_element: {}".format(single_element))
        # Perfom the actual read
        rsp = None
        req = "/datasets/" + self.id.uuid + "/value"
        params = {}

        if self.id._http_conn.mode == "r" and self.id._http_conn.cache_on:
            # enables lambda to be used on server
            self.log.debug("setting nonstrict parameter")
            params["nonstrict"] = 1
        else:
            self.log.debug("not settng nonstrict")

        if isinstance(selection, sel.SimpleSelection):
            # Divy up large selections into pages, so no one request
            # to the server will take unduly long to process
            chunk_layout = self.id.chunks
            if chunk_layout is None:
                chunk_layout = self._shape
            elif isinstance(chunk_layout, dict):
                # CHUNK_REF layout
                if "dims" not in chunk_layout:
                    self.log.error("Unexpected chunk_layout: {}".format(chunk_layout))
                else:
                    chunk_layout = tuple(chunk_layout["dims"])

            max_chunks = 1
            split_dim = -1

            sel_start = selection.start
            sel_step = selection.step
            sel_stop = []

            self.log.debug(f"selection._sel: {selection._sel}")
            scalar_selection = selection._sel[3]
            chunks_per_page = 1
            # determine the dimension for paging
            for i in range(rank):
                stop = sel_start[i] + selection.count[i] * sel_step[i]
                if stop > self._shape[i]:
                    stop = self._shape[i]
                sel_stop.append(stop)
                if scalar_selection[i]:
                    # scalar index so will hit just one chunk
                    continue
                count = sel_stop[i] - sel_start[i]
                num_chunks = count // chunk_layout[i]
                if count % chunk_layout[i] > 0:
                    num_chunks += 1  # get the integer ceiling
                if split_dim < 0 or num_chunks > max_chunks:
                    max_chunks = num_chunks
                    split_dim = i
                chunks_per_page = max_chunks

            self.log.info(
                "selection: start {} stop {} step {}".format(
                    sel_start, sel_stop, sel_step
                )
            )
            self.log.debug("split_dim: {}".format(split_dim))
            self.log.debug("chunks_per_page: {}".format(chunks_per_page))

            # determine which dimension of the target array to split on
            mshape_split_dim = 0
            for i in range(rank):
                if scalar_selection[i]:
                    continue
                if i == split_dim:
                    break
                mshape_split_dim += 1

            self.log.debug(f"mshape_split_dim: {split_dim}")
            chunk_size = chunk_layout[split_dim]
            self.log.debug(f"chunk size for split_dim: {chunk_size}")

            arr = numpy.empty(mshape, dtype=mtype)

            done = False
            while not done:
                num_rows = chunks_per_page * chunk_layout[split_dim]
                self.log.debug(f"num_rows: {num_rows}")
                page_start = list(copy(sel_start))

                num_pages = max_chunks // chunks_per_page
                if max_chunks % chunks_per_page > 0:
                    num_pages += 1  # get the integer ceiling

                des_index = 0  # this is where we'll copy to the arr for each page

                self.log.debug(
                    f"paged read, chunks_per_page: {chunks_per_page} max_chunks: {max_chunks}, num_pages: {num_pages}"
                )

                for page_number in range(num_pages):
                    self.log.debug(f"page_number: {page_number}")
                    self.log.debug(f"start: {page_start}  stop: {sel_stop}")

                    page_stop = list(copy(sel_stop))
                    page_stop[split_dim] = page_start[split_dim] + num_rows

                    if sel_step[split_dim] > 1:
                        # make sure the stop is aligned with the step value
                        rem = page_stop[split_dim] % sel_step[split_dim]
                        if rem != 0:
                            page_stop[split_dim] += sel_step[split_dim] - rem
                    if page_stop[split_dim] > sel_stop[split_dim]:
                        page_stop[split_dim] = sel_stop[split_dim]

                    self.log.info(f"page_stop: {page_stop[split_dim]}")

                    page_mshape = list(copy(mshape))
                    page_mshape[mshape_split_dim] = (
                        1
                        + (page_stop[split_dim] - page_start[split_dim] - 1)
                        // sel_step[split_dim]
                    )

                    page_mshape = tuple(page_mshape)
                    self.log.info(f"page_mshape: {page_mshape}")

                    params["select"] = self._getQueryParam(
                        page_start, page_stop, sel_step
                    )
                    try:
                        rsp = self.GET(req, params=params, format="binary")
                    except IOError as ioe:
                        self.log.info(f"got IOError: {ioe.errno}")
                        if ioe.errno == 413 and chunks_per_page > 1:
                            # server rejected the request, reduce the page size
                            chunks_per_page //= 2
                            self.log.info(f"New chunks_per_page: {chunks_per_page}")
                            break
                        else:
                            raise IOError(f"Error retrieving data: {ioe.errno}")
                    if isinstance(rsp, str):
                        # hexencoded response?
                        # this is returned by API Gateway for lamba responses
                        rsp = bytes.fromhex(rsp)
                        # from here treat it like a byte responses
                    if type(rsp) in (bytes, bytearray):
                        # got binary response
                        # TBD - check expected number of bytes
                        self.log.info(f"binary response, {len(rsp)} bytes")
                        # arr1d = numpy.frombuffer(rsp, dtype=mtype)
                        arr1d = bytesToArray(rsp, mtype, page_mshape)
                        page_arr = numpy.reshape(arr1d, page_mshape)
                    else:
                        # got JSON response
                        # need some special conversion for compound types --
                        # each element must be a tuple, but the JSON decoder
                        # gives us a list instead.
                        self.log.info("json response")

                        data = rsp["value"]
                        self.log.debug(data)

                        page_arr = jsonToArray(page_mshape, mtype, data)
                        self.log.debug(f"jsontoArray returned: {page_arr}")

                    # get the slices to copy into the target array
                    slices = []
                    for i in range(len(mshape)):
                        if i == mshape_split_dim:
                            num_rows = page_arr.shape[mshape_split_dim]
                            slices.append(slice(des_index, des_index + num_rows))
                            des_index += num_rows
                        else:
                            slices.append(slice(0, mshape[i]))
                    self.log.debug("slices: {}".format(slices))
                    arr[tuple(slices)] = page_arr

                    page_start[split_dim] = page_stop[split_dim]
                    self.log.debug(f"new page_start: {page_start}")
                    rows_remaining = sel_stop[split_dim] - page_start[split_dim]
                    if rows_remaining <= 0:
                        self.log.debug("done = True")
                        done = True
                        break
                    self.log.debug(f"{rows_remaining} rows left")

        elif isinstance(selection, sel.FancySelection):
            select = selection.getQueryParam()
            params["select"] = select
            MAX_SELECT_QUERY_LEN = 100
            if len(select) > MAX_SELECT_QUERY_LEN:
                # use a post method to avoid long query strings
                self.log.info("using post select")
                try:
                    rsp = self.POST(req, body=params, format="binary")
                except IOError as ioe:
                    self.log.info(f"got IOError: {ioe.errno}")
                    raise IOError(f"Error retrieving data: {ioe.errno}")
            else:
                try:
                    rsp = self.GET(req, params=params, format="binary")
                except IOError as ioe:
                    self.log.info(f"got IOError: {ioe.errno}")
                    raise IOError(f"Error retrieving data: {ioe.errno}")
            if type(rsp) in (bytes, bytearray):
                # got binary response
                self.log.info(f"binary response, {len(rsp)} bytes")
                arr = bytesToArray(rsp, mtype, mshape)
            else:
                # got JSON response
                # need some special conversion for compound types --
                # each element must be a tuple, but the JSON decoder
                # gives us a list instead.
                self.log.info("json response")

                data = rsp["value"]
                # self.log.debug(data)

                arr = jsonToArray(mshape, mtype, data)
                self.log.debug(f"jsontoArray returned: {arr}")
        elif isinstance(selection, sel.PointSelection):
            format = "json"  # default as JSON request since h5serv does not yet support binary
            body = {}

            points = selection.points.tolist()
            rank = len(self._shape)
            # verify the points are in range and strictly monotonic (for the 1d case)
            last_point = -1
            delistify = False

            if len(points) == rank and isinstance(points[0], int) and rank > 1:
                # Single point selection - need to wrap this in an array
                self.log.info("single point selection")
                points = [
                    points,
                ]
            else:
                for point in points:
                    if isinstance(point, (list, tuple)):
                        if not isinstance(point, (list, tuple)):
                            raise ValueError("invalid point argument")
                        if len(point) != rank:
                            raise ValueError("invalid point argument")
                        for i in range(rank):
                            if point[i] < 0 or point[i] >= self._shape[i]:
                                raise IndexError("point out of range")
                        if rank == 1:
                            delistify = True
                            if point[0] <= last_point:
                                raise TypeError(
                                    "index points must be strictly increasing"
                                )
                            last_point = point[0]

                    elif rank == 1 and isinstance(point, int):
                        if point < 0 or point > self._shape[0]:
                            raise IndexError("point out of range")
                        if point <= last_point:
                            raise TypeError("index points must be strictly increasing")
                        last_point = point
                    else:
                        raise ValueError("invalid point argument")

            if self.id.id.startswith("d-"):
                # send points as binary request for HSDS
                format = "binary"
                arr_points = numpy.asarray(
                    points, dtype="u8"
                )  # must use unsigned 64-bit int
                body = arr_points.tobytes()
                self.log.info(
                    "point select binary request, num bytes: {}".format(len(body))
                )
            else:
                if delistify:
                    self.log.info("delistifying point selection")
                    # convert to int if needed
                    body["points"] = []
                    for point in points:
                        if isinstance(point, (list, tuple)):
                            body["points"].append(point[0])
                        else:
                            body["points"] = point
                else:
                    # can just assign
                    body["points"] = points
                self.log.info("sending point selection request: {}".format(body))
            rsp = self.POST(req, format=format, body=body)
            if type(rsp) in (bytes, bytearray):
                if len(rsp) // mtype.itemsize != selection.mshape[0]:
                    raise IOError(
                        "Expected {} elements, but got {}".format(
                            selection.mshape[0], (len(rsp) // mtype.itemsize)
                        )
                    )
                arr = numpy.frombuffer(rsp, dtype=mtype)
            else:
                data = rsp["value"]
                if len(data) != selection.mshape[0]:
                    raise IOError(
                        "Expected {} elements, but got {}".format(
                            selection.mshape[0], len(data)
                        )
                    )

                arr = numpy.asarray(data, dtype=mtype, order="C")

        else:
            raise ValueError("selection type not supported")

        self.log.info("got arr: {}, cleaning up shape!".format(arr.shape))
        # Patch up the output for NumPy
        if len(names) == 1:
            arr = arr[names[0]]  # Single-field recarray convention
        if arr.shape == ():
            arr = numpy.asscalar(arr)
        elif single_element:
            arr = arr[0]

        # elif len(arr.shape) > 1:
        #    arr = numpy.squeeze(arr)  # reduce dimension if there are single dimension entries
        return arr

    def __setitem__(self, args, val):
        """Write to the HDF5 dataset from a Numpy array.

        NumPy's broadcasting rules are honored, for "simple" indexing
        (slices and integers).  For advanced indexing, the shapes must
        match.
        """
        self.log.info("Dataset __setitem__, args: {}".format(args))
        use_base64 = True  # may need to set this to false below for some types

        args = args if isinstance(args, tuple) else (args,)

        # get the val dtype if we're passed a numpy array
        try:
            self.log.debug(
                f"val dtype: {val.dtype}, shape: {val.shape} metadata: {val.dtype.metadata}"
            )
            if numpy.product(val.shape) == 0:
                self.log.info("no elements in numpy array, skipping write")
        except AttributeError:
            self.log.debug("val not ndarray")
            pass  # not a numpy object, just leave dtype as None

        if self._shape is None:
            # null space dataset
            if isinstance(val, Empty):
                return  # nothing to do
            else:
                raise TypeError("Unable to assign values to dataset with null shape")

        elif isinstance(val, Empty):
            pass  #  no data

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
        vlen = check_dtype(vlen=self.dtype)
        if vlen is not None and vlen not in (bytes, str):
            try:
                val = numpy.asarray(val, dtype=vlen)

            except ValueError:
                try:
                    val = numpy.array(
                        [numpy.array(x, dtype=self.dtype) for x in val],
                        dtype=self.dtype,
                    )
                except ValueError as e:
                    self.log.debug(
                        f"valueError converting value element by element: {e} "
                    )
            if vlen == val.dtype:
                if val.ndim > 1:
                    tmp = numpy.empty(shape=val.shape[:-1], dtype=self.dtype)
                    tmp.ravel()[:] = [
                        i
                        for i in val.reshape(
                            (
                                numpy.product(val.shape[:-1], dtype=numpy.ulonglong),
                                val.shape[-1],
                            )
                        )
                    ]
                else:
                    tmp = numpy.array([None], dtype=self.dtype)
                    tmp[0] = val
                val = tmp

        elif (
            isinstance(val, complex)
            or getattr(getattr(val, "dtype", None), "kind", None) == "c"
        ):
            if self.dtype.kind != "V" or self.dtype.names != ("r", "i"):
                raise TypeError(
                    "Wrong dataset dtype for complex number values: %s"
                    % self.dtype.fields
                )
            if isinstance(val, complex):
                val = numpy.asarray(val, dtype=type(val))
            tmp = numpy.empty(shape=val.shape, dtype=self.dtype)
            tmp["r"] = val.real
            tmp["i"] = val.imag
            val = tmp

        elif self.dtype.kind == "O" or (
            self.dtype.kind == "V"
            and (not isinstance(val, numpy.ndarray) or val.dtype.kind != "V")
            and (self.dtype.subdtype == None)
        ):
            # TBD: Do we need something like the following in the above if condition:
            # (self.dtype.str != val.dtype.str)
            # for cases where the val is a numpy array but different type than self?
            if len(names) == 1 and self.dtype.fields is not None:
                # Single field selected for write, from a non-array source
                if not names[0] in self.dtype.fields:
                    raise ValueError("No such field for indexing: %s" % names[0])
                dtype = self.dtype.fields[names[0]][0]
                cast_compound = True
            else:
                dtype = self.dtype
                cast_compound = False
            val = numpy.asarray(val, dtype=dtype, order="C")
            if cast_compound:
                val = val.astype(numpy.dtype([(names[0], dtype)]))

        elif isinstance(val, numpy.ndarray):
            # convert array if needed
            # TBD - need to handle cases where the type shape is different
            self.log.debug("got numpy array")
            if val.dtype != self.dtype and val.dtype.shape == self.dtype.shape:
                self.log.info("converting {} to {}".format(val.dtype, self.dtype))
                # convert array
                tmp = numpy.empty(val.shape, dtype=self.dtype)
                tmp[...] = val[...]
                val = tmp
        else:
            val = numpy.asarray(val, order="C", dtype=self.dtype)

        # Check for array dtype compatibility and convert
        mshape = None
        """
        # TBD..
        if self.dtype.subdtype is not None:
            shp = self.dtype.subdtype[1]   # type shape
            valshp = val.shape[-len(shp):]
            if valshp != shp:  # Last dimension has to match
                raise TypeError("When writing to array types, last N dimensions have to match (got %s, but should be %s)" % (valshp, shp,))
            mtype = h5t.py_create(numpy.dtype((val.dtype, shp)))
            mshape = val.shape[0:len(val.shape)-len(shp)]


        # Make a compound memory type if field-name slicing is required
        elif len(names) != 0:

            mshape = val.shape

            # Catch common errors
            if self.dtype.fields is None:
                raise TypeError("Illegal slicing argument (not a compound dataset)")
            mismatch = [x for x in names if x not in self.dtype.fields]
            if len(mismatch) != 0:
                mismatch = ", ".join('"%s"'%x for x in mismatch)
                raise ValueError("Illegal slicing argument (fields %s not in dataset type)" % mismatch)

            # Write non-compound source into a single dataset field
            if len(names) == 1 and val.dtype.fields is None:
                subtype = h5y.py_create(val.dtype)
                mtype = h5t.create(h5t.COMPOUND, subtype.get_size())
                mtype.insert(self._e(names[0]), 0, subtype)

            # Make a new source type keeping only the requested fields
            else:
                fieldnames = [x for x in val.dtype.names if x in names] # Keep source order
                mtype = h5t.create(h5t.COMPOUND, val.dtype.itemsize)
                for fieldname in fieldnames:
                    subtype = h5t.py_create(val.dtype.fields[fieldname][0])
                    offset = val.dtype.fields[fieldname][1]
                   mtype.insert(self._e(fieldname), offset, subtype)

        # Use mtype derived from array (let DatasetID.write figure it out)
        else:
            mshape = val.shape
            #mtype = None
        """
        mshape = val.shape
        self.log.debug(f"mshape: {mshape}")
        self.log.debug(f"data dtype: {val.dtype}")

        # Perform the dataspace selection
        selection = sel.select(self, args)
        self.log.debug(f"selection.mshape: {selection.mshape}")
        if selection.nselect == 0:
            return

        # Broadcast scalars if necessary.
        if mshape == () and selection.mshape != None and selection.mshape != ():
            self.log.debug("broadcast scalar")
            if self.dtype.subdtype is not None:
                raise TypeError("Scalar broadcasting is not supported for array dtypes")
            val2 = numpy.empty(selection.mshape, dtype=val.dtype)
            val2[...] = val
            val = val2
            mshape = val.shape

        # Perform the write, with broadcasting
        # Be careful to pad memory shape with ones to avoid HDF5 chunking
        # glitch, which kicks in for mismatched memory/file selections
        """
        # TBD: do we need this adjustment?
        if(len(mshape) < len(self._shape)):
            mshape_pad = (1,)*(len(self._shape)-len(mshape)) + mshape
        else:
            mshape_pad = mshape
        """
        req = "/datasets/" + self.id.uuid + "/value"

        params = {}
        body = {}

        format = "json"

        if use_base64:

            if self.id.uuid.startswith("d-"):
                # server is HSDS, use binary data, use param values for selection
                format = "binary"
                body = arrayToBytes(val, vlen=vlen)
                self.log.debug(f"writing binary data, {len(body)}")
            else:
                # h5serv, base64 encode, body json for selection
                # TBD - replace with above once h5serv supports binary req
                data = val.tobytes()
                data = base64.b64encode(data)
                data = data.decode("ascii")
                self.log.debug("data: {}".format(data))
                body["value_base64"] = data
                self.log.debug("writing base64 data, {} bytes".format(len(data)))
        else:
            if type(val) is not list:
                val = val.tolist()
            val = _decode(val)
            self.log.debug("writing json data, {} elements".format(len(val)))
            self.log.debug("data: {}".format(val))
            body["value"] = val

        if selection.select_type != sel.H5S_SELECT_ALL:
            select_param = selection.getQueryParam()
            self.log.debug(f"got select query param: {select_param}")
            params["select"] = select_param

        self.PUT(req, body=body, format=format, params=params)
        """
        mspace = h5s.create_simple(mshape_pad, (h5s.UNLIMITED,)*len(mshape_pad))
        for fspace in selection.broadcast(mshape):
            self.id.write(mspace, fspace, val, mtype)
        """

    def read_direct(self, dest, source_sel=None, dest_sel=None):
        """Read data directly from HDF5 into an existing NumPy array.

        The destination array must be C-contiguous and writable.
        Selections must be the output of numpy.s_[<args>].

        Broadcasting is supported for simple indexing.
        """

        # TBD: avoid data copy
        # TBD: support broadcast
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

        # TBD - broadcast support

    def write_direct(self, source, source_sel=None, dest_sel=None):
        """Write data directly to HDF5 from a NumPy array.

        The source array must be C-contiguous.  Selections must be
        the output of numpy.s_[<args>].

        Broadcasting is supported for simple indexing.
        """

        # TBD: avoid data copy
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

        # TBD: support broadcast

    def __array__(self, dtype=None):
        """Create a Numpy array containing the whole dataset.  DON'T THINK
        THIS MEANS DATASETS ARE INTERCHANGABLE WITH ARRAYS.  For one thing,
        you have to read the whole dataset everytime this method is called.
        """
        arr = numpy.empty(self._shape, dtype=self.dtype if dtype is None else dtype)

        # Special case for (0,)*-shape datasets
        if self._shape is None or numpy.product(self._shape) == 0:
            return arr

        self.read_direct(arr)
        return arr

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
            r = f'<HDF5 dataset {namestr}: shape {self._shape}, type "{self.dtype.str}">'
        return r

    def refresh(self):
        """Refresh the dataset metadata by reloading from the file.

        This is part of the SWMR features and only exist when the HDF5
        librarary version >=1.9.178
        """
        pass  # todo

    def flush(self):
        """Flush the dataset data and metadata to the file.
        If the dataset is chunked, raw data chunks are written to the file.

        This is part of the SWMR features and only exist when the HDF5
        librarary version >=1.9.178
        """
        pass  # todo

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
