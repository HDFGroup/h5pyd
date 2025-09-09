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


import numpy as np
# trying to import these results in circular references,
# so just use is_reference, is_regionreference helpers to identify
# from .base import Reference, RegionReference

import weakref
import codecs
from collections import namedtuple
from h5json import hdf5dtype


def is_reference(val):
    try:
        if val.__class__.__name__ == "Reference":
            return True
    except AttributeError:
        pass  # ignore
    try:
        if val.__name__ == "Reference":
            return True
    except AttributeError:
        pass  # ignore

    return False


def is_regionreference(val):
    try:
        if val.__class__.__name__ == "RegionReference":
            return True
    except AttributeError:
        pass  # ignore
    try:
        if val.__name__ == "RegionReference":
            return True
    except AttributeError:
        pass  # ignore

    return False


def special_dtype(**kwds):
    """ Create a new h5py "special" type.  Only one keyword may be given.

    Legal keywords are:

    vlen = basetype
        Base type for HDF5 variable-length datatype. This can be Python
        str type or instance of np.dtype.
        Example: special_dtype( vlen=str )

    enum = (basetype, values_dict)
        Create a NumPy representation of an HDF5 enumerated type.  Provide
        a 2-tuple containing an (integer) base dtype and a dict mapping
        string names to integer values.

    ref = Reference | RegionReference
        Create a NumPy representation of an HDF5 object or region reference
        type.    """

    return hdf5dtype.special_dtype(**kwds)


def check_vlen_dtype(dt):
    """If the dtype represents an HDF5 vlen, returns the Python base class.

    Returns None if the dtype does not represent an HDF5 vlen.
    """
    try:
        return dt.metadata.get('vlen', None)
    except AttributeError:
        return None


string_info = namedtuple('string_info', ['encoding', 'length'])


def check_string_dtype(dt):
    """If the dtype represents an HDF5 string, returns a string_info object.

    The returned string_info object holds the encoding and the length.
    The encoding can only be 'utf-8' or 'ascii'. The length may be None
    for a variable-length string, or a fixed length in bytes.

    Returns None if the dtype does not represent an HDF5 string.
    """
    vlen_kind = check_vlen_dtype(dt)
    if vlen_kind is str:
        return string_info('utf-8', None)
    elif vlen_kind is bytes:
        return string_info('ascii', None)
    elif dt.kind == 'S':
        enc = (dt.metadata or {}).get('h5py_encoding', 'ascii')
        return string_info(enc, dt.itemsize)
    else:
        return None


def check_enum_dtype(dt):
    """If the dtype represents an HDF5 enumerated type, returns the dictionary
    mapping string names to integer values.

    Returns None if the dtype does not represent an HDF5 enumerated type.
    """
    try:
        return dt.metadata.get('enum', None)
    except AttributeError:
        return None


def check_opaque_dtype(dt):
    """Return True if the dtype given is tagged to be stored as HDF5 opaque data
    """
    try:
        return dt.metadata.get('h5py_opaque', False)
    except AttributeError:
        return False


def check_ref_dtype(dt):
    """If the dtype represents an HDF5 reference type, returns the reference
    class (either Reference or RegionReference).

    Returns None if the dtype does not represent an HDF5 reference type.
    """
    try:
        return dt.metadata.get('ref', None)
    except AttributeError:
        return None


def check_dtype(**kwds):
    """ Check a dtype for h5py special type "hint" information.  Only one
    keyword may be given.

    vlen = dtype
        If the dtype represents an HDF5 vlen, returns the Python base class.
        Currently only builting string vlens (str) are supported.  Returns
        None if the dtype does not represent an HDF5 vlen.

    enum = dtype
        If the dtype represents an HDF5 enumerated type, returns the dictionary
        mapping string names to integer values.  Returns None if the dtype does
        not represent an HDF5 enumerated type.

    ref = dtype
        If the dtype represents an HDF5 reference type, returns the reference
        class (either Reference or RegionReference).  Returns None if the dtype
        does not represent an HDF5 reference type.
    """

    return hdf5dtype.check_dtype(**kwds)


def vlen_dtype(basetype):
    """Make a numpy dtype for an HDF5 variable-length datatype

    For variable-length string dtypes, use :func:`string_dtype` instead.
    """
    return np.dtype('O', metadata={'vlen': basetype})


def string_dtype(encoding='utf-8', length=None):
    """Make a numpy dtype for HDF5 strings

    encoding may be 'utf-8' or 'ascii'.

    length may be an integer for a fixed length string dtype, or None for
    variable length strings. String lengths for HDF5 are counted in bytes,
    not unicode code points.

    For variable length strings, the data should be passed as Python str objects
    (unicode in Python 2) if the encoding is 'utf-8', and bytes if it is 'ascii'.
    For fixed length strings, the data should be numpy fixed length *bytes*
    arrays, regardless of the encoding. Fixed length unicode data is not
    supported.
    """
    # Normalize encoding name:
    try:
        encoding = codecs.lookup(encoding).name
    except LookupError:
        pass  # Use our error below

    if encoding not in {'ascii', 'utf-8'}:
        raise ValueError("Invalid encoding (%r); 'utf-8' or 'ascii' allowed"
                         % encoding)

    if isinstance(length, int):
        # Fixed length string
        return np.dtype("|S" + str(length), metadata={'h5py_encoding': encoding})
    elif length is None:
        vlen = str if (encoding == 'utf-8') else bytes
        return np.dtype('O', metadata={'vlen': vlen})
    else:
        raise TypeError("length must be integer or None (got %r)" % length)


def enum_dtype(values_dict, basetype=np.uint8):
    """Create a NumPy representation of an HDF5 enumerated type

    *values_dict* maps string names to integer values. *basetype* is an
    appropriate integer base dtype large enough to hold the possible options.
    """
    dt = np.dtype(basetype)
    if not np.issubdtype(dt, np.integer):
        raise TypeError("Only integer types can be used as enums")

    return np.dtype(dt, metadata={'enum': values_dict})


def getQueryDtype(dt):
    """
    Return dtype with field added for Index values
    """
    field_names = dt.names
    #  make up a index field name that doesn't conflict with existing names
    index_name = "index"
    for i in range(len(field_names)):
        if index_name in field_names:
            index_name = "_" + index_name
        else:
            break

    dt_fields = [(index_name, 'uint64'),]
    for i in range(len(dt)):
        dt_fields.append((dt.names[i], dt[i]))
    query_dt = np.dtype(dt_fields)

    return query_dt
