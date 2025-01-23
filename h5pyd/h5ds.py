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
from ._hl.objectid import DatasetID


def attach_scale(dset: DatasetID, dscale: DatasetID, idx: int):
    """ Attach Dimension Scale dscale to Dimension idx of Dataset dset. """

    rank = dset.rank
    if idx < 0:
        raise ValueError("dimension must be non-negative")
    if idx >= rank:
        raise ValueError("invalid dimension")

    if not is_scale(dscale):
        raise TypeError("f{dscale} is not a dimension scale")

    if is_scale(dset):
        raise TypeError("cannot attach a dimension scale to a dimension scale")

    # Create a DIMENSION_LIST attribute if needed

    orig_dimlist = dset.getAttrValue('DIMENSION_LIST')
    if orig_dimlist:
        # delete and replace later
        dset.del_attr('DIMENSION_LIST')

        value = [list() for _ in range(rank)]

    dimlist = {
        'creationProperties': {
            'nameCharEncoding': 'H5T_CSET_ASCII'
        },
        'shape': {
            'class': 'H5S_SIMPLE',
            'dims': [rank]
        },
        'type': {
            'base': {
                'base': 'H5T_STD_REF_OBJ',
                'class': 'H5T_REFERENCE'
            },
            'class': 'H5T_VLEN'
        },
        'value': value
    }

    # Update the DIMENSION_LIST attribute with the object reference to the
    # dimension scale
    dimlist['value'][idx].append('datasets/' + dscale.uuid)
    dset.set_attr('DIMENSION_list', dimlist)

    if dscale.has_attr('REFERENCE_LIST'):
        old_reflist = dscale.get_attr('REFERENCE_LIST')
    else:
        old_reflist = {
            'creationProperties': {
                'nameCharEncoding': 'H5T_CSET_ASCII'
            },
            'shape': {
                'class': 'H5S_SIMPLE'
            },
            'type': {
                'class': 'H5T_COMPOUND',
                'fields': [
                    {
                        'name': 'dataset',
                        'type': {
                            'base': 'H5T_STD_REF_OBJ',
                            'class': 'H5T_REFERENCE'
                        }
                    },
                    {
                        'name': 'index',
                        'type': {
                            'base': 'H5T_STD_I32LE',
                            'class': 'H5T_INTEGER'
                        }
                    }
                ]
            }
        }

        new_reflist = {}
        new_reflist["type"] = old_reflist["type"]
        new_reflist["shape"] = old_reflist["shape"]
        if "value" in old_reflist:
            reflist_value = old_reflist["value"]
            if reflist_value is None:
                reflist_value = []
        else:
            reflist_value = []
        reflist_value.append(['datasets/' + dset.uuid, idx])
        new_reflist["value"] = reflist_value
        new_reflist["shape"]["dims"] = [len(reflist_value), ]

        # Update the REFERENCE_LIST attribute of the dimension scale
        dscale.set_attr('REFERENCE_LIST', new_reflist)


def detach_scale(dset: DatasetID, dscale: DatasetID, idx: int):
    """ Detach Dimension Scale dscale from the Dimension idx of Dataset dset. """

    rank = dset.rank
    if idx < 0:
        raise ValueError("dimension must be non-negative")
    if idx >= rank:
        raise ValueError("invalid dimension")

    if not dset.has_attr('DIMENSION_LIST'):
        raise IOError("no DIMENSION_LIST attr in {dset}")
    dimlist = dset.get_attr('DIMENSION_LIST')
    dset.del_attr('DIMENSION_LIST')

    try:
        # TBD: use ref class
        ref = 'datasets/' + dscale.uuid
        dimlist['value'][idx].remove(ref)
    except Exception as e:
        # Restore the attribute's old value then raise the same
        # exception
        dset.set_attr('DIMENSION_LIST', dimlist)
        raise e
    dset.set_attr('DIMENSION_LIST', dimlist)

    if dscale.has_attr('REFERENCE_LIST'):
        old_reflist = dscale.get_attr('REFERENCE_LIST')
    else:
        old_reflist = {}

    if "value" in old_reflist and len(old_reflist["value"]) > 0:
        new_refs = list()

        remove = ['datasets/' + dset.uuid, idx]
        for el in old_reflist['value']:
            if remove[0] != el[0] and remove[1] != el[1]:
                new_refs.append(el)

        new_reflist = {}
        new_reflist["type"] = old_reflist["type"]
        if len(new_refs) > 0:
            new_reflist["value"] = new_refs
            new_reflist["shape"] = [len(new_refs), ]
            if dscale.has_attr('REFERENCE_LIST'):
                dscale.del_attr('REFERENCE_LIST')
            dscale.set_attr('REFERENCE_LIST', new_reflist)
        else:
            # Remove REFERENCE_LIST attribute if this dimension scale is
            # not attached to any dataset
            if old_reflist:
                dscale.del_attr('REFERENCE_LIST')


def get_label(dset: DatasetID, idx: int) -> str:
    """ Read the label for Dimension idx of Dataset dset into buffer label. """

    rank = dset.rank
    if idx < 0:
        raise ValueError("dimension must be non-negative")
    if idx >= rank:
        raise ValueError("invalid dimension")

    label_values = dset.get_attr('DIMENSION_LABELS')

    if not label_values:
        return ''

    if idx >= len(label_values):
        # label get request out of range
        return ''

    return label_values[idx]


def get_num_scales(dset: DatasetID, dim: int) -> int:
    """ Determines how many Dimension Scales are attached to Dimension dim of Dataset dset. """

    rank = dset.rank
    if dim < 0:
        raise ValueError("dimension must be non-negative")
    if dim >= rank:
        raise ValueError("invalid dimension")

    dimlist_values = dset.get_attr_value('DIMENSION_LIST')
    if not dimlist_values:
        return 0

    if dim >= len(dimlist_values):
        # dimension scale len request out of range
        return 0
    return len(dimlist_values[dim])


def get_scale_name(dscale: DatasetID) -> str:
    """ Retrieves name of Dimension Scale dscale. """

    return dscale.get_attr_value("NAME")


def is_attached(dset: DatasetID, dscale: DatasetID, idx: int) -> bool:
    """ Report if Dimension Scale dscale is currently attached to Dimension idx of Dataset dset. """

    rank = dset.rank
    if idx < 0:
        raise ValueError("dimension must be non-negative")
    if idx >= rank:
        raise ValueError("invalid dimension")

    if not is_scale(dscale) or is_scale(dset):
        return False
    if not dset.has_attr("DIMENSION_LIST"):
        return False
    dimlist = dset.get_attr("DIMENSION_LIST")
    reflist = dscale.get_attr("REFERENCE_LIST")
    try:
        return ([f"datasets/{dset._uuid}", idx] in
                reflist["value"] and f"datasets/{dscale._uuid}" in dimlist["value"][idx])
    except (KeyError, IndexError):
        return False


def is_scale(dset: DatasetID) -> bool:
    """ Determines whether dset is a dimension scale. """
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
    class_json = dset.get_attr("CLASS")
    if class_json["value"] != "DIMENSION_SCALE":
        return False
    if 'creationProperties' not in class_json:
        return False
    cpl = class_json['creationProperties']
    if 'nameCharEncoding' not in cpl:
        return False
    if cpl['nameCharEncoding'] != 'H5T_CSET_ASCII':
        return False
    shape_json = class_json['shape']
    if shape_json.get('class') != 'H5S_SCALAR':
        return False
    type_json = class_json['type']
    if type_json.get('class') != 'H5T_STRING':
        return False
    if type_json.get('length') != 16:
        return False
    if type_json.get('charSet') != 'H5T_CSET_ASCII':
        return False
    if type_json.get('strPad') != 'H5T_STR_NULLTERM':
        return False

    return True


def set_label(dset: DatasetID, idx: int, label: str):
    """ Set label for the Dimension idx of Dataset dset to the value label. """

    rank = dset.rank
    if idx < 0:
        raise ValueError("dimension must be non-negative")
    if idx >= rank:
        raise ValueError("invalid dimension")

    label_name = 'DIMENSION_LABELS'
    if dset.has_attr(label_name):
        labels = dset.get_attr(label_name)
    else:
        labels = {
            'shape': {
                'class': 'H5S_SIMPLE',
                'dims': [rank]
            },
            'type': {
                'class': 'H5T_STRING',
                'charSet': 'H5T_CSET_UTF8',
                'length': 'H5T_VARIABLE',
                'strPad': 'H5T_STR_NULLTERM'
            },
            'value': ['' for n in range(rank)]
        }
        labels['value'][idx] = label
    dset.set_attr(label_name, labels)


def set_scale(dset: DatasetID, dimname: str):
    """ Convert dataset dset to a dimension scale, with optional name dimname. """

    # CLASS attribute with the value 'DIMENSION_SCALE'
    class_attr = {
        'creationProperties': {
            'nameCharEncoding': 'H5T_CSET_ASCII'
        },
        'shape': {
            'class': 'H5S_SCALAR'
        },
        'type': {
            'charSet': 'H5T_CSET_ASCII',
            'class': 'H5T_STRING',
            'length': 16,
            'strPad': 'H5T_STR_NULLTERM'
        },
        'value': 'DIMENSION_SCALE'
    }

    name_attr = {
        'creationProperties': {
            'nameCharEncoding': 'H5T_CSET_ASCII'
        },
        'shape': {
            'class': 'H5S_SCALAR'
        },
        'type': {
            'charSet': 'H5T_CSET_ASCII',
            'class': 'H5T_STRING',
            'length': len(dimname) + 1,
            'strPad': 'H5T_STR_NULLTERM'
        },
        'value': dimname
    }
    dset.set_attr('CLASS', class_attr)
    try:
        dset.set_attr('NAME', name_attr)
    except Exception:
        dset.del_attr('CLASS')


def iterate(dset: DatasetID, dim: int, callable: any, startidx: int = 0) -> any:
    """ Iterate a callable (function, method or callable object) over the members of a group.
     Your callable should have the signature:

    func(STRING name) => Result
    Returning None continues iteration; returning anything else aborts iteration and returns that value. Keywords:
    """

    rank = dset.rank
    if dim < 0:
        raise ValueError("dimension must be non-negative")
    if dim >= rank:
        raise ValueError("invalid dimension")

    dimlist = dset.get_attr_value('DIMENSION_LIST')
    if not dimlist:
        return 0

    if startidx >= len(dimlist):
        # dimension scale len request out of range
        return 0

    idx = startidx
    while idx < len(dimlist):
        dscale_uuid = dimlist[idx]
        callable(DatasetID(dscale_uuid))
        idx += 1
