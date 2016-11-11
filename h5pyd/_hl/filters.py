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

"""
    Implements support for HDF5 compression filters via the high-level
    interface.  The following types of filter are available:

    "gzip"
        Standard DEFLATE-based compression, at integer levels from 0 to 9.
        Built-in to all public versions of HDF5.  Use this if you want a
        decent-to-good ratio, good portability, and don't mind waiting.

    "lzf"
        Custom compression filter for h5py.  This filter is much, much faster
        than gzip (roughly 10x in compression vs. gzip level 4, and 3x faster
        in decompressing), but at the cost of a worse compression ratio.  Use
        this if you want cheap compression and portability is not a concern.

    "szip"
        Access to the HDF5 SZIP encoder.  SZIP is a non-mainstream compression
        format used in space science on integer and float datasets.  SZIP is
        subject to license requirements, which means the encoder is not
        guaranteed to be always available.  However, it is also much faster
        than gzip.

    The following constants in this module are also useful:

    decode
        Tuple of available filter names for decoding

    encode
        Tuple of available filter names for encoding
"""

from __future__ import absolute_import, division

import numpy as np
# from .. import h5z, h5p, h5d


_COMP_FILTERS = {'gzip': 'H5Z_FILTER_DEFLATE',
                'szip': 'H5Z_FILTER_SZIP',
                'lzf': 'H5Z_FILTER_LZF',
                'shuffle': 'H5Z_FILTER_SHUFFLE',
                'fletcher32': 'H5Z_FILTER_FLETCHER32',
                'scaleoffset': 'H5Z_FILTER_SCALEOFFSET' }

DEFAULT_GZIP = 4
DEFAULT_SZIP = ('nn', 8)
SO_INT_MINBITS_DEFAULT = 0

def _gen_filter_tuples():
    """ Bootstrap function to figure out what filters are available. """
    dec = []
    enc = []
    for name, code in _COMP_FILTERS.items():
        # TBD: Provide a REST operation to query which filters are available
        #info = h5z.get_filter_info(code)
        enc.append(name)
        dec.append(name)

    return tuple(dec), tuple(enc)



decode, encode = _gen_filter_tuples()

def generate_dcpl(shape, dtype, chunks, compression, compression_opts,
                  shuffle, fletcher32, maxshape, scaleoffset):
    """ Generate a dataset creation property list.

    Undocumented and subject to change without warning.
    """

    plist = {}

    if shape == ():
        if any((chunks, compression, compression_opts, shuffle, fletcher32,
                scaleoffset is not None)):
            raise TypeError("Scalar datasets don't support chunk/filter options")
        if maxshape and maxshape != ():
            raise TypeError("Scalar datasets cannot be extended")
        return plist

    def rq_tuple(tpl, name):
        """ Check if chunks/maxshape match dataset rank """
        if tpl in (None, True):
            return
        try:
            tpl = tuple(tpl)
        except TypeError:
            raise TypeError('"%s" argument must be None or a sequence object' % name)
        if len(tpl) != len(shape):
            raise ValueError('"%s" must have same rank as dataset shape' % name)

    rq_tuple(chunks, 'chunks')
    rq_tuple(maxshape, 'maxshape')

    if compression is not None:

        if compression not in encode and not isinstance(compression, int):
            raise ValueError('Compression filter "%s" is unavailable' % compression)

        if compression == 'gzip':
            if compression_opts is None:
                gzip_level = DEFAULT_GZIP
            elif compression_opts in range(10):
                gzip_level = compression_opts
            else:
                raise ValueError("GZIP setting must be an integer from 0-9, not %r" % compression_opts)

        elif compression == 'lzf':
            if compression_opts is not None:
                raise ValueError("LZF compression filter accepts no options")

        elif compression == 'szip':
            if compression_opts is None:
                compression_opts = DEFAULT_SZIP

            err = "SZIP options must be a 2-tuple ('ec'|'nn', even integer 0-32)"
            try:
                szmethod, szpix = compression_opts
            except TypeError:
                raise TypeError(err)
            if szmethod not in ('ec', 'nn'):
                raise ValueError(err)
            if not (0<szpix<=32 and szpix%2 == 0):
                raise ValueError(err)

    elif compression_opts is not None:
        # Can't specify just compression_opts by itself.
        raise TypeError("Compression method must be specified")

    if scaleoffset is not None:
        # scaleoffset must be an integer when it is not None or False,
        # except for integral data, for which scaleoffset == True is
        # permissible (will use SO_INT_MINBITS_DEFAULT)

        if scaleoffset < 0:
            raise ValueError('scale factor must be >= 0')

        if dtype.kind == 'f':
            if scaleoffset is True:
                raise ValueError('integer scaleoffset must be provided for '
                                 'floating point types')
        elif dtype.kind in ('u', 'i'):
            if scaleoffset is True:
                scaleoffset = SO_INT_MINBITS_DEFAULT
        else:
            raise TypeError('scale/offset filter only supported for integer '
                            'and floating-point types')

        # Scale/offset following fletcher32 in the filter chain will (almost?)
        # always triggera a read error, as most scale/offset settings are
        # lossy. Since fletcher32 must come first (see comment below) we
        # simply prohibit the combination of fletcher32 and scale/offset.
        if fletcher32:
            raise ValueError('fletcher32 cannot be used with potentially lossy'
                             ' scale/offset filter')
    # End argument validation

    if (chunks is True) or \
    (chunks is None and any((shuffle, fletcher32, compression, maxshape,
                             scaleoffset is not None))):
        chunks = guess_chunk(shape, maxshape, dtype.itemsize)

    if maxshape is True:
        maxshape = (None,)*len(shape)

    if chunks is not None:
        #plist.set_chunk(chunks)
        # set layout key
        layout = { 'class': 'H5D_CHUNKED'}
        layout['dims'] = chunks
        plist['layout'] = layout
        plist['fillTime'] = 'H5D_FILL_TIME_ALLOC'  # prevent resize glitch

    filters = []
    # MUST be first, to prevent 1.6/1.8 compatibility glitch
    if fletcher32:
        filter_fletcher32 = { 'class': 'H5Z_FLETCHER_DEFLATE' }
        filter_fletcher32['id'] = 3
        filters.append(filter_fletcher32)

    # scale-offset must come before shuffle and compression
    if scaleoffset is not None:
        filter_scaleoffset = { 'class': 'H5Z_FILTER_SCALEOFFSET' }
        filter_scaleoffset['id'] = 6
        filter_scaleoffset['scaleOffset'] = scaleoffset
        if dtype.kind in ('u', 'i'):
            #plist.set_scaleoffset(h5z.SO_INT, scaleoffset)
            filter_scaleoffset['scaleType'] = 'H5Z_SO_INT'
        else: # dtype.kind == 'f'
            #plist.set_scaleoffset(h5z.SO_FLOAT_DSCALE, scaleoffset)
            filter_scaleoffset['scaleType'] = 'H5Z_SO_FLOAT_DSCALE'
        filters.append(filter_scaleoffset)

    if shuffle:
        filter_shuffle = { 'class': 'H5Z_FILTER_SHUFFLE' }
        filter_shuffle['id'] = 2
        filters.append(filter_shuffle)

    if compression == 'gzip':
        #plist.set_deflate(gzip_level)
        filter_gzip = { 'class': 'H5Z_FILTER_DEFLATE' }
        filter_gzip['id'] = 1
        filter_gzip['level'] = gzip_level
        filters.append(filter_gzip)
    elif compression == 'lzf':
        #plist.set_filter(h5z.FILTER_LZF, h5z.FLAG_OPTIONAL)
        filter_lzf = { 'class': 'H5Z_FILTER_LZF' }
        filter_lzf['id'] = 32000
        filters.append(filter_lzf)

    elif compression == 'szip':
        opts = {'ec': 'H5Z_SZIP_EC_OPTION_MASK', 'nn': 'H5Z_SZIP_NN_OPTION_MASK' }
        # plist.set_szip(opts[szmethod], szpix)
        filter_szip = { 'class': 'H5Z_FILTER_SZIP' }
        filter_szip['id'] = 4
        filter_szip['coding'] = opts
        if szmethod == 'ec':
            filter_szip['coding'] = 'H5_SZIP_EC_OPTION_MASK'
        else:   # 'nn'
            filter_szip['coding'] = 'H5_SZIP_NN_OPTION_MASK'
        filter_szip['bitsPerPixel'] = szpix
        filters.append(filter_szip)

    elif isinstance(compression, int):
        # TBD - don't have a way to query available filters via REST API
        # just throw ValueError for now
        raise ValueError("Unsupported compression filter: {}".format(compression))
        """
        if not h5z.filter_avail(compression):
            raise ValueError("Unknown compression filter number: %s" % compression)
        filter_ext = { 'id': compression }
        for k in compression_opts:
            filter_ext[k] = compression_opts[k]
        filters.append(filter_ext)
        plist.set_filter(compression, h5z.FLAG_OPTIONAL, compression_opts)
        """

    if len(filters) > 0:
        plist["filters"] = filters

    return plist

def get_filters(plist):
    """ Extract a dictionary of active filters from a DCPL, along with
    their settings.

    Undocumented and subject to change without warning.
    """

    filter_names = {'H5Z_FILTER_DEFLATE': 'gzip',
               'H5Z_FILTER_SZIP': 'szip',
               'H5Z_FILTER_SHUFFLE': 'shuffle',
               'H5Z_FILTER_FLETCHER32': 'fletcher32',
               'H5Z_FILTER_LZF': 'lzf',
               'H5Z_FILTER_SCALEOFFSET': 'scaleoffset' }

    vals = None
    pipeline = {}
    if 'filters' not in plist:
        return pipeline


    filters = plist['filters']

    for filter in filters:

        if filter['class'] == 'H5Z_FILTER_DEFLATE':
            vals = filter['level'] # gzip level

        elif filter['class'] == 'H5Z_FILTER_SZIP':
            mask = None
            if filter['coding'] == "H5Z_SZIP_EC_OPTION_MASK":
                mask = 'ec'
            elif filter['coding'] == "H5Z_SZIP_NN_OPTION_MASK":
                mask = 'nn'
            else:
                raise TypeError("Unknown SZIP configuration")
            pixels = filter['bitsPerPixel']
            vals = (mask, pixels)
        elif filter['class'] == 'H5Z_FILTER_LZF':
            vals = None
        else:
            if vals and len(vals) == 0:
               vals = None
        filter_name = "Extension"
        if filter['class'] in filter_names:
            filter_name = filter_names[filter['class']]

        if filter['class'] in filter_names.keys():
            filter_name = filter_names[filter['class']]
            pipeline[filter_name] = vals

    return pipeline

CHUNK_BASE = 16*1024    # Multiplier by which chunks are adjusted
CHUNK_MIN = 8*1024      # Soft lower limit (8k)
CHUNK_MAX = 1024*1024   # Hard upper limit (1M)

def guess_chunk(shape, maxshape, typesize):
    """ Guess an appropriate chunk layout for a dataset, given its shape and
    the size of each element in bytes.  Will allocate chunks only as large
    as MAX_SIZE.  Chunks are generally close to some power-of-2 fraction of
    each axis, slightly favoring bigger values for the last index.

    Undocumented and subject to change without warning.
    """
    # pylint: disable=unused-argument

    # For unlimited dimensions we have to guess 1024
    shape = tuple((x if x!=0 else 1024) for i, x in enumerate(shape))

    ndims = len(shape)
    if ndims == 0:
        raise ValueError("Chunks not allowed for scalar datasets.")

    chunks = np.array(shape, dtype='=f8')
    if not np.all(np.isfinite(chunks)):
        raise ValueError("Illegal value in chunk tuple")

    # Determine the optimal chunk size in bytes using a PyTables expression.
    # This is kept as a float.
    dset_size = np.product(chunks)*typesize
    target_size = CHUNK_BASE * (2**np.log10(dset_size/(1024.*1024)))

    if target_size > CHUNK_MAX:
        target_size = CHUNK_MAX
    elif target_size < CHUNK_MIN:
        target_size = CHUNK_MIN

    idx = 0
    while True:
        # Repeatedly loop over the axes, dividing them by 2.  Stop when:
        # 1a. We're smaller than the target chunk size, OR
        # 1b. We're within 50% of the target chunk size, AND
        #  2. The chunk is smaller than the maximum chunk size

        chunk_bytes = np.product(chunks)*typesize

        if (chunk_bytes < target_size or \
         abs(chunk_bytes-target_size)/target_size < 0.5) and \
         chunk_bytes < CHUNK_MAX:
            break

        if np.product(chunks) == 1:
            break  # Element size larger than CHUNK_MAX

        chunks[idx%ndims] = np.ceil(chunks[idx%ndims] / 2.0)
        idx += 1

    return tuple(int(x) for x in chunks)








