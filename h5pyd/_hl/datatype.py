# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Implements high-level access to committed datatypes in the file. """

from __future__ import absolute_import

import posixpath as pp

from h5json.hdf5dtype import createDataType

from .base import HLObject

from .objectid import TypeID


class Datatype(HLObject):

    """
        Represents an HDF5 named datatype stored in a file.

        To store a datatype, simply assign it to a name in a group:

        >>> MyGroup["name"] = numpy.dtype("f")
        >>> named_type = MyGroup["name"]
        >>> assert named_type.dtype == numpy.dtype("f")  """

    @property
    def dtype(self):
        """Numpy dtype equivalent for this datatype"""
        return self._dtype

    def __init__(self, bind):
        """ Create a new Datatype object by binding to a low-level TypeID.
        """
        if not isinstance(bind, TypeID):
            # todo: distinguish type from other hl objects
            raise ValueError(f"{bind} is not a TypeID")
        HLObject.__init__(self, bind)

        self._dtype = createDataType(self.id.type_json)

    def __repr__(self):
        if not self.id:
            return "<Closed HDF5 named type>"
        if self.name is None:
            namestr = '("anonymous")'
        else:
            name = pp.basename(pp.normpath(self.name))
            namestr = f"{name if name != '' else '/'}"
            if name:
                namestr = f'"{name}"'
            else:
                namestr = '"/"'
        return f'<HDF5 named type {namestr} (dtype {self.dtype.str})>'
