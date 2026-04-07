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
import json
from ._hl.objectid import DatasetID


def is_scale(dsetid: DatasetID) -> bool:
    """True if HDF5 dataset is a Dimension Scale."""
    # This is the expected CLASS attribute's JSON...
    # {
    #     'creationProperties': {
    #         'nameCharEncoding': 'H5T_CSET_ASCII'
    #     },
    #     'shape': {
    #         'class': 'H5S_SCALAR'
    #     },
    #     'type': {
    #         'charSet': 'H5T_CSET_ASCII',
    #         'class': 'H5T_STRING',
    #         'length': 16,
    #         'strPad': 'H5T_STR_NULLTERM'
    #     },
    #     'value': 'DIMENSION_SCALE'
    # }
    class_json = dsetid.db.getAttribute(dsetid.uuid, "CLASS")
    if class_json is None:
        return False
    try:
        if class_json["value"] != "DIMENSION_SCALE":
            return False
        elif class_json["shape"]["class"] != "H5S_SCALAR":
            return False
        elif class_json["type"]["class"] != "H5T_STRING":
            return False
        elif class_json["type"]["strPad"] != "H5T_STR_NULLTERM" and False:
            return False
        elif class_json["type"]["length"] != 16:
            return False
    except KeyError:
        return False

    return True


def is_attached(dsetid: DatasetID, dscaleid: DatasetID, idx: int) -> bool:
    """True if Dimension Scale ``dscale`` is attached to Dataset ``dset`` at dimension ``idx``"""
    if not is_scale(dscaleid) or is_scale(dsetid):
        return False

    dimlist = dsetid.db.getAttribute(dsetid.uuid, "DIMENSION_LIST")
    reflist = dscaleid.db.getAttribute(dscaleid.uuid, "REFERENCE_LIST")
    try:
        return ([f"datasets/{dsetid.id}", idx] in
                reflist["value"] and f"datasets/{dscaleid.id}" in dimlist["value"][idx])
    except (KeyError, IndexError):
        return False
