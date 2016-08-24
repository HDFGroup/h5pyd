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

# import weakref
# import sys
# import os
# import uuid

# import six

# import requests
# import json

# from base import HLObject
# from . import base
from .base import phil, parse_lastmodified
# from .. import version


class ObjectID:

    """
        Uniquely identifies an h5serv resource
    """

    @property
    def uuid(self):
        return self._uuid

    @property
    def id(self):
        return self._uuid

    @property
    def domain(self):
        """domain resource"""
        return self._domain

    @property
    def endpoint(self):
        """service endpoint"""
        return self._endpoint

    @property
    def objtype_code(self):
        """ return one char code to denote what type of object
        g: group
        d: dataset
        t: committed datatype
        """
        return self._objtype_code

    @property
    def parent(self):
        """parent obj - none for anonymous obj"""
        return self._parent

    @property
    def mode(self):
        """mode domain was opened in"""
        return self._mode

    @property
    def obj_json(self):
        """json representation of the object"""
        return self._obj_json

    @property
    def modified(self):
        """last modified timestamp"""
        return self._modified

    def __init__(self, parent, item, objtype_code=None, domain=None,
                 endpoint=None, mode='r', **kwds):
        """Create a new objectId.
        """
        # print "object init:", item
        if type(item) is not dict:
            raise IOError("Unexpected Error")

        if "id" not in item:
            raise IOError("Unexpected Error")

        self._uuid = item['id']

        self._modified = parse_lastmodified(item['lastModified'])

        self._obj_json = item

        self._endpoint = None

        self._objtype_code = objtype_code

        with phil:
            if parent is not None:
                self._domain = parent.id.domain
                self._endpoint = parent.id.endpoint
                self._mode = parent.id.mode
                # self._parent = parent
            else:
                self._domain = domain
                self._endpoint = endpoint
                self._mode = mode

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uuid == other._uuid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def close(self):
        """Remove handles to id.
        """
        self._old_uuid = self._uuid  # for debugging
        self._uuid = 0
        self._obj_json = None
        self._endpoint = None


class TypeID(ObjectID):

    @property
    def type_json(self):
        return self.obj_json['type']

    def __init__(self, parent, item, domain=None, endpoint=None, **kwds):
        """Create a new TypeID.
        """

        with phil:
            ObjectID.__init__(self, parent, item, objtype_code='t',
                              domain=domain, endpoint=endpoint)


class DatasetID(ObjectID):

    @property
    def type_json(self):
        return self._obj_json['type']

    @property
    def shape_json(self):
        return self._obj_json['shape']

    @property
    def dcpl_json(self):
        dcpl = self._obj_json['creationProperties']
        return dcpl

    @property
    def rank(self):
        rank = 0
        shape = self._obj_json['shape']
        if shape['class'] == 'H5S_SIMPLE':
            dims = shape['dims']
            rank = len(dims)
        return rank

    def __init__(self, parent, item, domain=None, endpoint=None, **kwds):
        """Create a new DatasetID.
        """

        with phil:
            ObjectID.__init__(self, parent, item, objtype_code='d',
                              domain=domain, endpoint=endpoint)


class GroupID(ObjectID):

    def __init__(self, parent, item, domain=None, endpoint=None, mode=None,
                 **kwds):
        """Create a new GroupID.
        """

        with phil:
            ObjectID.__init__(self, parent, item, objtype_code='g',
                              domain=domain, mode=mode, endpoint=endpoint)
