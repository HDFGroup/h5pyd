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
import time
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

try:
    import h5py
    import h5pyd
    import numpy as np
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n"
                     % str(e))
    sys.exit(1)

if __name__ == "utillib":
    from chunkiter import ChunkIterator
else:
    from .chunkiter import ChunkIterator


class ChunkInfo:
    """
    Extract Dataset chunk info
    """

    def __init__(self, h5_path, dset_path):
        """
        Parameters
        ----------
        h5_path : str
            .h5 file path
        dset_path : str
            HDF5 dataset name / path
        """
        self._h5_path = h5_path
        self._dset_path = dset_path
        with h5py.File(h5_path, mode='r') as f:
            dobj = f[dset_path]
            self._dset_dims = dobj.shape
            self._chunk_dims = dobj.chunks
            dsetid = dobj.id
            self._spaceid = dsetid.get_space()
            if self._chunk_dims:
                self._num_chunks = dsetid.get_num_chunks(self._spaceid)
            else:
                msg = '{} is not chunked!'.format(dset_path)
                logging.error(msg)
                raise RuntimeError(msg)

    @property
    def h5_path(self):
        """
        .h5 file path

        Returns
        -------
        str
        """
        return self._h5_path

    @property
    def dset_path(self):
        """
        HDF5 dataset name / path

        Returns
        -------
        str
        """
        return self._dset_path

    @property
    def dset_dims(self):
        """
        Dataset shape

        Returns
        -------
        tuple
        """
        return self._dset_dims

    @property
    def rank(self):
        """
        number of dataset dimensions

        Returns
        -------
        int
        """
        return len(self.dset_dims)

    @property
    def chunk_dims(self):
        """
        Dataset chunks (chunk shape)

        Returns
        -------
        tuple
        """
        return self._chunk_dims

    @property
    def spaceid(self):
        """
        Dataset space id

        Returns
        -------
        obj
        """
        return self._spaceid

    @property
    def num_chunks(self):
        """
        Number of chunks in dataset

        Returns
        -------
        int
        """
        return self._num_chunks

    @staticmethod
    def _get_chunk_info(h5_path, dset_path, chunks, spaceid=None,
                        rank=None):
        """
        Get specific chunk info for given object from given file

        Parameters
        ----------
        h5_path : str
            .h5 file path
        dset_path : str
            HDF5 dataset name / path
        chunks : list
            Chunks to get info for
        spaceid : obj | None
            dataset spaceid
        rank : int | None
            number of dataset dimensions

        Returns
        -------
        chunk_info: dict
            {index: (byte_offset, size)}
        """
        chunk_info = {}
        with h5py.File(h5_path, mode='r') as f:
            dset = f[dset_path]
            if rank is None:
                rank = len(dset.shape)

            if spaceid is None:
                spaceid = dset.id.get_space()

            for i, chunk_num in enumerate(chunks):
                info = dset.id.get_chunk_info(chunk_num, spaceid)

                index = info.chunk_offset
                logging.debug("got chunk_info: {} for chunk: {}"
                              .format(info, chunk_num))
                if not isinstance(index, tuple) or len(index) != rank:
                    msg = ("Unexpected array_offset: {} for dataset with"
                           " rank: {}".format(index, rank))
                    logging.error(msg)
                    raise IOError(msg)

                chunk_info.update({index: (info.byte_offset, info.size)})
                logging.debug('Info extracted for {} out of {} chunks'
                              .format(i, len(chunks)))

        return chunk_info

    @staticmethod
    def _create_worker_chunks(numb_chunks, chunks_per_worker=10):
        """
        Compute the slice of sc_table to submit to each worker

        Parameters
        ----------
        numb_chunks : int
            Number of chunks in dataset
        chunks_per_worker : int, optional
            Number of chunks to submit to each worker in parallel,
            by default 10

        Returns
        -------
        worker_chunks : list
            List of chunks to send to each worker
        """
        chunks = list(range(0, numb_chunks, chunks_per_worker))
        if chunks[-1] < numb_chunks:
            chunks += [numb_chunks]

        worker_chunks = []
        for s, e in enumerate(chunks[1:]):
            worker_chunks.append(list(range(chunks[s], e)))

        return worker_chunks

    def get_chunk_info(self, max_workers=None, chunks_per_worker=10):
        """
        Get chunk info with given number of maximum workers. If > 1 run in
        parallel, else in Serial. If None use all available cores

        Parameters
        ----------
        max_workers : int, optional
            maximum workers to use to get chunk info. If > 1 run in parallel,
            else in Serial. If None use all available cores, by default None
        chunks_per_worker : int, optional
            Number of chunks to submit to each worker in parallel,
            by default 10

        Returns
        -------
        chunk_info: dict
            {index: (byte_offset, size)}
        """
        ts = time.time()
        if max_workers is None:
            max_workers = os.cpu_count()

        min_chunks = max_workers * chunks_per_worker
        if self.num_chunks < min_chunks and max_workers > 1:
            msg = ('Number of chunks ({}) is insufficient to warrent parallel '
                   'processing, chunk_info will be extracted in serial'
                   .format(self.num_chunks))
            logging.warning(msg)
            max_workers = 1

        if max_workers > 1:
            chunk_info = {}
            logging.debug('Getting chunk info in parallel with {} workers'
                          .format(max_workers))
            worker_chunks = \
                self._create_worker_chunks(self.num_chunks,
                                           chunks_per_worker=chunks_per_worker)
            spawn = multiprocessing.get_context('spawn')
            with ProcessPoolExecutor(max_workers=max_workers,
                                     mp_context=spawn) as exe:
                futures = []
                for chunks in worker_chunks:
                    future = exe.submit(self._get_chunk_info,
                                        self.h5_path, self.dset_path,
                                        chunks, spaceid=self.spaceid,
                                        rank=self.rank)
                    futures.append(future)

                for i, future in enumerate(as_completed(futures)):
                    chunk_info.update(future.result())
                    logging.debug('Completed {} out of {} chunks'
                                  .format(i * chunks_per_worker,
                                          self.num_chunks))
        else:
            logging.debug('Getting chunk info in serial')
            chunk_info = self._get_chunk_info(self.h5_path, self.dset_path,
                                              list(range(self.num_chunks)),
                                              spaceid=self.spaceid,
                                              rank=self.rank)

        tt = (time.time() - ts) / 60
        logging.debug('Chunk extracted in {:.4f} minutes'.format(tt))

        return chunk_info

    def chunk_map(self, max_workers=1, chunks_per_worker=10):
        """
        Create chunk map

        Parameters
        ----------
        max_workers : int, optional
            maximum workers to use to get chunk info. If > 1 run in parallel,
            else in Serial. If None use all available cores, by default None
        chunks_per_worker : int, optional
            Number of chunks to submit to each worker in parallel,
            by default 10

        Returns
        -------
        chunk_map: dict
            {chunk_key: (byte_offset, size)}
        """
        ts = time.time()
        if self.num_chunks >= 10:
            msg = ('WARNING: {} has {} chunks. chunk_map is most efficient '
                   ' for datasets with less than 10 chunks. It is advised '
                   ' that you use a chunkinfo_arr instead!'
                   .format(self.dset_path, self.num_chunks))
            logging.info(msg)

        chunk_map = {}
        chunk_info = self.get_chunk_info(max_workers=max_workers,
                                         chunks_per_worker=chunks_per_worker)

        for index, info in chunk_info.items():
            chunk_key = ""
            for dim in range(self.rank):
                chunk_key += str(index[dim] // self.chunk_dims[dim])
                if dim < self.rank - 1:
                    chunk_key += "_"

            logging.debug("adding chunk_key: {}".format(chunk_key))
            chunk_map[chunk_key] = info

        tt = (time.time() - ts) / 60
        logging.debug('chunk_map created in {:.4f} minutes'.format(tt))

        return chunk_map

    def chunkinfo_arr(self, max_workers=None, chunks_per_worker=10):
        """
        Create chunkinfo array

        Parameters
        ----------
        max_workers : int, optional
            maximum workers to use to get chunk info. If > 1 run in parallel,
            else in Serial. If None use all available cores, by default None
        chunks_per_worker : int, optional
            Number of chunks to submit to each worker in parallel,
            by default 10

        Returns
        -------
        chunkinfo_arr: ndarray
            ndarray of chunk's (byte_offset, size)
        """
        ts = time.time()
        if self.num_chunks < 10:
            msg = ('WARNING: {} has {} chunks. chunk_table is most efficient '
                   ' for datasets with more than 10 chunks. It is advised '
                   ' that you use a chunk_map instead!'
                   .format(self.dset_path, self.num_chunks))
            logging.warning(msg)

        dt = np.dtype([('offset', np.int64), ('size', np.int32)])

        chunkinfo_arr_dims = []
        for dim in range(self.rank):
            chunkinfo_arr_dims.append(
                int(np.ceil(self.dset_dims[dim] / self.chunk_dims[dim])))

        chunkinfo_arr_dims = tuple(chunkinfo_arr_dims)
        logging.debug("creating chunkinfo array of shape: {}"
                      .format(chunkinfo_arr_dims))
        chunkinfo_arr = np.zeros(np.prod(chunkinfo_arr_dims), dtype=dt)

        chunk_info = self.get_chunk_info(max_workers=max_workers,
                                         chunks_per_worker=chunks_per_worker)
        for index, info in chunk_info.items():
            offset = 0
            stride = 1
            for i in range(self.rank):
                dim = self.rank - i - 1
                offset += (index[dim] // self.chunk_dims[dim]) * stride
                stride *= chunkinfo_arr_dims[dim]

            chunkinfo_arr[offset] = info

        chunkinfo_arr = chunkinfo_arr.reshape(chunkinfo_arr_dims)

        tt = (time.time() - ts) / 60
        logging.debug('chunkinfo_arr created in {:.4f} minutes'.format(tt))

        return chunkinfo_arr

    @classmethod
    def get(cls, h5_path, dset_path, max_workers=None, chunks_per_worker=10):
        """
        Get either chunk_map or chunkinfo_arr depending on the number of chunks

       Parameters
        ----------
        h5_path : str
            .h5 file path
        dset_path : str
            HDF5 dataset name / path
        max_workers : int, optional
            maximum workers to use to get chunk info. If > 1 run in parallel,
            else in Serial. If None use all available cores, by default None
        chunks_per_worker : int, optional
            Number of chunks to submit to each worker in parallel,
            by default 10
        """
        logging.debug('Getting chunk info {} in {} using {} workers'
                      .format(dset_path, h5_path, max_workers))
        chunk_info = cls(h5_path, dset_path)
        if chunk_info.num_chunks < 10:
            logging.debug('- Creating chunk_map')
            out = chunk_info.chunk_map(max_workers=max_workers,
                                       chunks_per_worker=chunks_per_worker)
        else:
            logging.debug('- Creating chunkinfo_arr')
            out = chunk_info.chunkinfo_arr(max_workers=max_workers,
                                           chunks_per_worker=chunks_per_worker)

        return out


class ObjectHelper:
    """
    Helper to create objects in HSDS
    """

    def __init__(self, h5, hsds, dataload="ingest", s3_path=None,
                 compression_filter=None, compression_opts=None,
                 srcid_desobj_map=None, verbose=False):
        """
        Parameters
        ----------
        h5 : h5py.File
            Open h5py File instance (.h5) to load data from
        hsds : h5pyd.File
            Open h5pyd File instance (HSDS) to load data into
        dataload : str | None, optional
            ingest, s3link, None, by default "ingest"
        s3_path : str, optional
            Path to .h5 file on S3, by default None
        srcid_desobj_map : dict, optional
            Mapping of src obj ids to HDF5 objects, by default none
        compression_filter : str, optional
            Compression filter to use for datasets, by default None
        compression_opts : int | str, optional
            Compression filter level | options, by default None
        verbose : bool, optional
            Verbose logging using print, by default False
        """
        self._h5 = h5
        self._hsds = hsds
        self._dataload = dataload
        self._s3_path = s3_path
        self._compression_filter = compression_filter
        self._compression_opts = compression_opts
        if srcid_desobj_map is None:
            srcid_desobj_map = {}

        self._srcid_desobj_map = srcid_desobj_map
        self.verbose = verbose

    def __call__(self, name, obj):
        """
        Functional represenation to pass to visititems
        """
        logging.debug('Visiting {}'.format(obj.name))
        class_name = obj.__class__.__name__
        if class_name in ("Dataset", "Table"):
            logging.debug('Dataset {} will be loaded later'
                          .format(name))
        elif class_name == "Group":
            grp = self.create_group(name, obj)

            if grp is not None:
                for ga in obj.attrs:
                    self.copy_attribute(grp, ga, obj)
        elif class_name == "Datatype":
            self.create_datatype(obj)
        else:
            logging.error("no handler for object class: {}"
                          .format(type(obj)))

    @staticmethod
    def dump_dtype(dt):
        """
        Dump dtype
        """
        if not isinstance(dt, np.dtype):
            raise TypeError("expected np.dtype, but got: {}".format(type(dt)))
        if dt > 0:
            out = "{"
            for name in dt.fields:
                subdt = dt.fields[name][0]
                out += "{}: {} |".format(name, ObjectHelper.dump_dtype(subdt))

            out = out[:-1] + "}"
        else:
            ref = h5py.check_dtype(ref=dt)
            if ref:
                out = str(ref)
            else:
                vlen = h5py.check_dtype(vlen=dt)
                if vlen:
                    out = "VLEN: " + ObjectHelper.dump_dtype(vlen)
                else:
                    out = str(dt)

        return out

    @staticmethod
    def is_h5py(obj):
        """
        Return True if objref is a h5py object and False is not
        """
        # Return True if objref is a h5py object and False is not
        out = False
        if isinstance(obj, object) and isinstance(obj.id.id, int):
            out = True

        return out

    @staticmethod
    def is_reference(val, region=False):
        """
        Return True if val is a Reference and False if not
        """
        if region:
            name = 'RegionReference'
        else:
            name = 'Reference'

        out = False
        try:
            if isinstance(val, object) and val.__class__.__name__ == name:
                out = True
            elif isinstance(val, type) and val.__name__ == name:
                out = True
        except AttributeError as ae:
            logging.exception("is_reference for {} error: {}".format(val, ae))

        return out

    @staticmethod
    def has_reference(dtype):
        """
        Check if dtype has a reference
        """
        has_ref = False
        if not isinstance(dtype, np.dtype):
            return False

        if len(dtype) > 0:
            for name in dtype.fields:
                item = dtype.fields[name]
                if ObjectHelper.has_reference(item[0]):
                    has_ref = True
                    break
        elif dtype.metadata and 'ref' in dtype.metadata:
            basedt = dtype.metadata['ref']
            has_ref = ObjectHelper.is_reference(basedt)
        elif dtype.metadata and 'vlen' in dtype.metadata:
            basedt = dtype.metadata['vlen']
            has_ref = ObjectHelper.has_reference(basedt)

        return has_ref

    def convert_dtype(self, srcdt):
        """
        Return a dtype based on input dtype, converting any Reference types
        from h5py style to h5pyd and vice-versa.
        """
        logging.debug("convert dtype: {}, type: {},"
                      .format(srcdt, type(srcdt)))

        if len(srcdt) > 0:  # pylint: disable=len-as-condition
            fields = []
            for name in srcdt.fields:
                item = srcdt.fields[name]
                # item is a tuple of dtype and integer offset
                field_dt = self.convert_dtype(item[0])
                fields.append((name, field_dt))

            tgt_dt = np.dtype(fields)
        else:
            # check if this a "special dtype"
            if srcdt.metadata and 'ref' in srcdt.metadata:
                ref = srcdt.metadata['ref']
                if self.is_reference(ref):
                    if self.is_h5py(self._hsds):
                        tgt_dt = h5py.special_dtype(ref=h5py.Reference)
                    else:
                        tgt_dt = h5pyd.special_dtype(ref=h5pyd.Reference)
                elif self.is_reference(ref, region=True):
                    if self.is_h5py(self._hsds):
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
                    tgt_base = self.convert_dtype(src_vlen)
                else:
                    tgt_base = src_vlen
                if self.is_h5py(self._hsds):
                    tgt_dt = h5py.special_dtype(vlen=tgt_base)
                else:
                    tgt_dt = h5pyd.special_dtype(vlen=tgt_base)
            else:
                tgt_dt = srcdt

        return tgt_dt

    @staticmethod
    def create_links(h5_group, hsds_group, srcid_desobj_map, verbose=False):
        """
        Create any group links
        """
        msg = "create_links: {}".format(h5_group.name)
        logging.debug(msg)
        if verbose:
            print(msg)

        for title in h5_group:
            msg = "got link: {}".format(title)
            logging.debug(msg)
            if verbose:
                print(msg)

            lnk = h5_group.get(title, getlink=True)
            link_classname = lnk.__class__.__name__
            if link_classname == "HardLink":
                logging.debug("Got hardlink: {} h5_group: {} hsds_group: {}"
                              .format(title, h5_group, hsds_group))
                if title not in hsds_group:
                    msg = ("creating link {} with title: {}"
                           .format(hsds_group, title))
                    logging.info(msg)
                    if verbose:
                        print(msg)

                    src_obj_id = h5_group[title].id
                    src_obj_id_hash = src_obj_id.__hash__()
                    logging.debug("got src_obj_id hash: {}"
                                  .format(src_obj_id_hash))
                    if src_obj_id_hash in srcid_desobj_map:
                        des_obj = srcid_desobj_map[src_obj_id_hash]
                        logging.debug("creating hardlink to {}"
                                      .format(des_obj.id.id))
                        hsds_group[title] = des_obj
                    else:
                        msg = ("could not find map item to src id: {}"
                               .format(src_obj_id_hash))
                        logging.warning(msg)
                        if verbose:
                            print("WARNIGN: " + msg)

            elif link_classname == "SoftLink":
                msg = ("creating SoftLink({}) with title: {}"
                       .format(lnk.path, title))
                logging.info(msg)
                if verbose:
                    print(msg)

                if ObjectHelper.is_h5py(hsds_group):
                    soft_link = h5py.SoftLink(lnk.path)
                else:
                    soft_link = h5pyd.SoftLink(lnk.path)

                hsds_group[title] = soft_link
            elif link_classname == "ExternalLink":
                msg = ("creating ExternalLink({}, {}) with title: {}"
                       .format(lnk.filename, lnk.path, title))
                logging.info(msg)
                if verbose:
                    print(msg)

                if ObjectHelper.is_h5py(hsds_group):
                    ext_link = h5py.ExternalLink(lnk.filename, lnk.path)
                else:
                    ext_link = h5pyd.ExternalLink(lnk.filename, lnk.path)

                hsds_group[title] = ext_link
            else:
                msg = ("Unexpected link type: {}"
                       .format(lnk.__class__.__name__))
                logging.warning(msg)
                if verbose:
                    print(msg)

        logging.info("flush fout")

    def copy_element(self, val, src_dt, tgt_dt):
        """
        Copy value element
        """
        logging.debug("copy_element, val: {}, val type: {}, src_dt: {}, "
                      "tgt_dt: {}".format(val, type(val),
                                          self.dump_dtype(src_dt),
                                          self.dump_dtype(tgt_dt)))
        out = None
        if len(src_dt) > 0:
            out_fields = []
            i = 0
            for name in src_dt.fields:
                field_src_dt = src_dt.fields[name][0]
                field_tgt_dt = tgt_dt.fields[name][0]
                field_val = val[i]
                i += 1
                out_field = self.copy_element(field_val, field_src_dt,
                                              field_tgt_dt)
                out_fields.append(out_field)
                out = tuple(out_fields)
        elif src_dt.metadata and 'ref' in src_dt.metadata:
            if not tgt_dt.metadata or 'ref' not in tgt_dt.metadata:
                raise TypeError("Expected tgt dtype to be ref, but got: {}"
                                .format(tgt_dt))

            ref = tgt_dt.metadata['ref']
            if self.is_reference(ref):
                # initialize out to null ref
                if self.is_h5py(self._hsds):
                    out = h5py.Reference()  # null h5py ref
                else:
                    out = ''  # h5pyd refs are strings

                if ref:
                    try:
                        fin_obj = self._h5[val]
                    except AttributeError as ae:
                        logging.exception("Unable able to get obj for ref "
                                          "value: {}".format(ae))
                        return None

                    # TBD - for hsget, the name property is not getting set
                    h5path = fin_obj.name
                    if not h5path:
                        msg = "No path found for ref object"
                        logging.warning(msg)
                        if self.verbose:
                            print(msg)

                    else:
                        fout_obj = self._hsds[h5path]
                        if self.is_h5py(self._hsds):
                            out = fout_obj.ref
                        else:
                            # convert to string for JSON serialization
                            out = str(fout_obj.ref)

            elif self.is_reference(ref, region=True):
                out = "tbd"
            else:
                raise TypeError("Unexpected ref type: {}".format(type(ref)))
        elif src_dt.metadata and 'vlen' in src_dt.metadata:
            logging.debug("copy_elment, got vlen element, dt: {}"
                          .format(src_dt.metadata["vlen"]))
            if not isinstance(val, np.ndarray):
                raise TypeError("Expecting ndarray or vlen element, but got: "
                                "{}".format(type(val)))

            if not tgt_dt.metadata or 'vlen' not in tgt_dt.metadata:
                raise TypeError("Expected tgt dtype to be vlen, but got: {}"
                                .format(tgt_dt))

            src_vlen_dt = src_dt.metadata["vlen"]
            tgt_vlen_dt = tgt_dt.metadata["vlen"]
            if self.has_reference(src_vlen_dt):
                if len(val.shape) == 0:
                    # scalar array
                    e = val[()]
                    v = self.copy_element(e, src_vlen_dt, tgt_vlen_dt)
                    out = np.array(v, dtype=tgt_dt)
                else:
                    out = np.zeros(val.shape, dtype=tgt_dt)
                    for i, _ in enumerate(out):
                        e = val[i]
                        out[i] = self.copy_element(e, src_vlen_dt, tgt_vlen_dt)
            else:
                # can just directly copy the array
                out = np.zeros(val.shape, dtype=tgt_dt)
                out[...] = val[...]
        else:
            out = val  # can just copy as is

        return out

    def copy_array(self, src_arr):
        """
        Copy the numpy array to a new array.
        Convert any reference type to point to item in the target's hierarchy.
        """
        if not isinstance(src_arr, np.ndarray):
            raise TypeError("Expecting ndarray, but got: {}".format(src_arr))

        tgt_dt = self.convert_dtype(src_arr.dtype)
        tgt_arr = np.zeros(src_arr.shape, dtype=tgt_dt)

        if self.has_reference(src_arr.dtype):
            # flatten array to simplify iteration
            count = np.product(src_arr.shape)
            tgt_arr_flat = tgt_arr.reshape((count,))
            src_arr_flat = src_arr.reshape((count,))
            for i in range(count):
                e = src_arr_flat[i]
                element = self.copy_element(e, src_arr.dtype, tgt_dt)
                tgt_arr_flat[i] = element

            tgt_arr = tgt_arr_flat.reshape(src_arr.shape)
        else:
            # can just copy the entire array
            tgt_arr[...] = src_arr[...]

        return tgt_arr

    def copy_attribute(self, desobj, name, srcobj):
        """
        Copy object attribute
        """
        msg = "creating attribute {} in {}".format(name, srcobj.name)
        logging.debug(msg)
        if self.verbose:
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
        tgtarr = self.copy_array(srcarr)
        try:
            desobj.attrs.create(name, tgtarr)
        except (IOError, TypeError) as e:
            logging.exception("ERROR: failed to create attribute {} of object "
                              "{} -- {}".format(name, desobj.name, str(e)))

    def load_datasets(self, obj, dataload='ingest'):
        """
        Load all datasets in obj

        Parameters
        ----------
        obj : h5py
            h5py object
        dataload : str | None, optional
            How to "load" data, by default "ingest"
        """
        for name in obj:
            dobj = obj[name]
            class_name = dobj.__class__.__name__
            if class_name in ("Dataset", "Table"):
                if dobj.dtype.metadata and 'vlen' in dobj.dtype.metadata:
                    is_vlen = True
                else:
                    is_vlen = False

                if dobj.name in self._hsds:
                    logging.warning('{} already exists and will be skipped'
                                    .format(dobj.name))
                    dset = self.create_dataset(name, dobj)
                    if dataload == "link" and not is_vlen:
                        logging.info("skip datacopy for link reference")
                    elif dataload == "ingest":
                        logging.debug("calling write_dataset for dataset: {}"
                                      .format(dobj.name))
                        self.write_dataset(dobj, dset)

            elif class_name == "Group":
                self.load_datasets(dobj)

    def create_dataset(self, name, dobj):
        """
        create a dataset using the properties of the passed in h5py dataset.
        If successful, proceed to copy attributes and data.
        """
        msg = ("creating dataset {}, shape: {}, type: {}"
               .format(name, dobj.shape, dobj.dtype))
        logging.info(msg)
        if self.verbose:
            print(msg)

        fillvalue = None
        try:
            # can trigger a runtime error if fillvalue is undefined
            fillvalue = dobj.fillvalue
        except RuntimeError:
            pass  # ignore

        if dobj.dtype.metadata and 'vlen' in dobj.dtype.metadata:
            is_vlen = True
        else:
            is_vlen = False

        chunks = None
        if self._dataload == "link" and not is_vlen:
            dset_dims = dobj.shape
            logging.debug("dset_dims: {}".format(dset_dims))
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
                chunks["file_uri"] = self._s3_path
                chunks["offset"] = dsetid.get_offset()
                # TBD - check the size is not too large
                chunks["size"] = dsetid.get_storage_size()
                logging.info("using chunk layout: {}".format(chunks))

            elif num_chunks < 10:
                # construct map of chunks
                chunk_map = ChunkInfo.get(self._h5.filename, dobj.name,
                                          max_workers=1)

                chunks["class"] = 'H5D_CHUNKED_REF'
                chunks["file_uri"] = self._s3_path
                chunks["dims"] = dobj.chunks
                chunks["chunks"] = chunk_map
                logging.info("using chunk layout: {}".format(chunks))

            else:
                # create anonymous dataset to hold chunk info
                chunkinfo_arr = ChunkInfo.get(self._h5.filename, dobj.name)

                anon_dset = self._hsds.create_dataset(None,
                                                      chunkinfo_arr.shape,
                                                      chunkinfo_arr.dtype)
                anon_dset[...] = chunkinfo_arr
                logging.debug("anon_dset: {}".format(anon_dset))
                logging.debug("anon_dset.chunks: {}".format(anon_dset.chunks))
                logging.debug("anon_values: {}".format(chunkinfo_arr))
                logging.debug('- flushing anon dataset to s3')
                self._hsds.flush()

                chunks["class"] = 'H5D_CHUNKED_REF_INDIRECT'
                chunks["file_uri"] = self._s3_path
                chunks["dims"] = dobj.chunks
                chunks["chunk_table"] = anon_dset.id.id
                logging.info("using chunk layout: {}".format(chunks))

        if chunks is None and dobj.chunks:
            chunks = tuple(dobj.chunks)

        try:
            tgt_dtype = self.convert_dtype(dobj.dtype)
            if not dobj.shape or (is_vlen and self.is_h5py(self._hsds)):
                # don't use compression/chunks for scalar datasets
                compression_filter = None
                compression_opts = None
                chunks = None
                shuffle = None
                fletcher32 = None
                maxshape = None
                scaleoffset = None
            else:
                compression_filter = dobj.compression
                compression_opts = dobj.compression_opts
                compression_check = (self._compression_filter is not None
                                     and compression_filter is None)
                if compression_check:
                    compression_filter = self._compression_filter
                    compression_opts = self._compression_opts
                    if compression_filter and self.verbose:
                        print("applying {} filter with level: {}"
                              .format(compression_filter, compression_opts))

                shuffle = dobj.shuffle
                fletcher32 = dobj.fletcher32
                maxshape = dobj.maxshape
                scaleoffset = dobj.scaleoffset

            if is_vlen:
                fillvalue = None

            dset = self._hsds.create_dataset(name, shape=dobj.shape,
                                             dtype=tgt_dtype, chunks=chunks,
                                             compression=compression_filter,
                                             shuffle=shuffle,
                                             fletcher32=fletcher32,
                                             maxshape=maxshape,
                                             compression_opts=compression_opts,
                                             fillvalue=fillvalue,
                                             scaleoffset=scaleoffset)
            msg = ("dataset {} created, uuid: {}, chunk_size: {}"
                   .format(name, dset.id.id, str(dset.chunks)))
            logging.info(msg)
            if self.verbose:
                print(msg)

            logging.debug("adding dataset id {} to {} in srcid_desobj_map"
                          .format(dobj.id.id, dset))
            self._srcid_desobj_map[dobj.id.__hash__()] = dset

            logging.info("Copying dataset {} attributes".format(name))
            for da in dobj.attrs:
                self.copy_attribute(dset, da, dobj)

            logging.debug('- flushing {} to s3'.format(name))
            self._hsds.flush()

        except (IOError, TypeError, KeyError) as e:
            logging.exception("ERROR: failed to create dataset: {}"
                              .format(str(e)))

        return dset

    @staticmethod
    def write_dataset(src, tgt, verbose=False):
        """ write values from src dataset to target dataset.
        """
        msg = ("write_dataset src: {} to tgt: {}, shape: {}, type: {}"
               .format(src.name, tgt.name, src.shape, src.dtype))
        logging.info(msg)
        if verbose:
            print(msg)

        if src.shape is None:
            # null space dataset
            msg = "no data for null space dataset: {}".format(src.name)
            logging.info(msg)
            if verbose:
                print(msg)
            return  # no data

        if len(src.shape) == 0:
            # scalar dataset
            x = src[()]
            msg = "writing for scalar dataset: {}".format(src.name)
            logging.info(msg)
            if verbose:
                print(msg)

            tgt[()] = x
            return

        if src.dtype.metadata and 'vlen' in src.dtype.metadata:
            is_vlen = True
        else:
            is_vlen = False

        fillvalue = None
        if not is_vlen:
            try:
                # can trigger a runtime error if fillvalue is undefined
                fillvalue = src.fillvalue
            except RuntimeError:
                pass  # ignore

        msg = "iterating over chunks for {}".format(src.name)
        logging.info(msg)
        if verbose:
            print(msg)
        try:
            it = ChunkIterator(tgt)

            logging.debug("src dtype: {}".format(src.dtype))
            logging.debug("des dtype: {}".format(tgt.dtype))

            for s in it:
                arr = src[s]
                # don't write arr if it's all zeros
                # (or the fillvalue if defined)
                empty_arr = np.zeros(arr.shape, dtype=arr.dtype)
                if fillvalue:
                    empty_arr.fill(fillvalue)
                if np.array_equal(arr, empty_arr):
                    msg = "skipping chunk for slice: {}".format(str(s))
                else:
                    msg = "writing dataset data for slice: {}".format(s)
                    tgt[s] = arr
                logging.info(msg)
                if verbose:
                    print(msg)

        except (IOError, TypeError) as e:
            msg = "ERROR : failed to copy dataset data : {}".format(str(e))
            logging.error(msg)
            print(msg)

        msg = "done with dataload for {}".format(src.name)
        logging.info(msg)
        if verbose:
            print(msg)

        logging.info("flush fout")

    def create_group(self, name, gobj):
        """
        Create Group object
        """
        msg = "creating group {}".format(name)
        logging.info(msg)
        if self.verbose:
            print(msg)

        grp = self._hsds.create_group(name)
        logging.debug("adding group id {} to {} in srcid_desobj_map"
                      .format(gobj.id.id, grp))
        self._srcid_desobj_map[gobj.id.__hash__()] = grp

        return grp

    def create_datatype(self, obj):
        """
        Create dtype object
        """
        msg = "creating datatype {}".format(obj.name)
        logging.info(msg)
        if self.verbose:
            print(msg)

        self._hsds[obj.name] = obj.dtype
        logging.debug("adding datatype id {} to {} in srcid_desobj_map"
                      .format(obj.id.id, self._hsds[obj.name]))
        self._srcid_desobj_map[obj.id.__hash__()] = self._hsds[obj.name]


class HSLoad:
    """
    Base class to load .h5 file into HSDS for direct read from S3
    """

    def __init__(self, h5, hsds):
        """
        Parameters
        ----------
        h5 : h5py.File
            Source h5 file handler
        hsds : h5pyd.File
            HSDS file handler
        """
        self._h5 = h5
        self._hsds = hsds
        self._srcid_desobj_map = {}

    def __repr__(self):
        msg = "{} for {} to {}".format(self.__class__.__name__,
                                       self._h5.filename, self._hsds.filename)
        return msg

    def load_file(self, dataload="ingest", s3_path=None,
                  compression_filter=None, compression_opts=None,
                  verbose=False):
        """
        Load source file into HSDS

        Parameters
        ----------
        dataload : str | None, optional
            ingest, s3link, None, by default "ingest"
        s3_path : str, optional
            Path to .h5 file on S3, by default None
        compression_filter : str, optional
            Compression filter to use for datasets, by default None
        compression_opts : int | str, optional
            Compression filter level | options, by default None
        verbose : bool, optional
            Verbose logging using print, by default False
        """
        logging.info('Loading {} into {}'
                     .format(self._h5.filename, self._hsds.filename))
        ts = time.time()
        object_helper = ObjectHelper(self._h5, self._hsds, dataload=dataload,
                                     s3_path=s3_path,
                                     compression_filter=compression_filter,
                                     compression_opts=compression_opts,
                                     srcid_desobj_map=self._srcid_desobj_map,
                                     verbose=verbose)
        for ga in self._h5.attrs:
            object_helper.copy_attribute(self._hsds, ga, self._h5)

        object_helper.create_links(self._h5, self._hsds,
                                   self._srcid_desobj_map)
        self._h5.visititems(object_helper)
        logging.debug('- flushing hsds to s3')
        self._hsds.flush()

        object_helper.load_datasets(self._h5, dataload=dataload)

        tt = time.time() - ts
        logging.info("- File loaded in {:.4f} minutes".format(tt / 60))

    @classmethod
    def run(cls, h5, hsds, dataload="ingest", s3path=None,
            compression_filter=None, compression_opts=None, verbose=False):
        """
        Load .h5 file into HSDS using S3 source file

        Parameters
        ----------
        h5 : h5py.File
            Source h5 file handler
        hsds : h5pyd.File
            HSDS file handler
        dataload : str | None, optional
            ingest, s3link, None, by default "ingest"
        s3_path : str, optional
            Path to .h5 file on S3, by default None
        compression_filter : str, optional
            Compression filter to use for datasets, by default None
        compression_opts : int | str, optional
            Compression filter level | options, by default None
        verbose : bool, optional
            Verbose logging using print, by default False
        """
        if dataload != "ingest":
            if not dataload:
                logging.info("no data load")
            elif dataload == "link":
                if not s3path:
                    logging.error("s3path expected to be set")
                    sys.exit(1)

                logging.info("using s3path")
            else:
                logging.error("unexpected dataload value: {}".format(dataload))
                sys.exit(1)

        load = cls(h5, hsds)
        load.load_file(dataload=dataload, s3_path=s3path,
                       compression_filter=compression_filter,
                       compression_opts=compression_opts,
                       verbose=verbose)

        return 0
