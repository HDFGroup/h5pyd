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
from datetime import datetime
import json
import pytz
import time
from h5json.objid import getCollectionForId, isValidUuid
from .h5type import createDataType


def parse_lastmodified(datestr):
    """Turn last modified datetime string into a datetime object."""
    if isinstance(datestr, str):
        # format: 2016-06-30T06:17:16.563536Z
        # format: "2016-08-04T06:44:04Z"
        dt = datetime.strptime(
            datestr, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    else:
        # if the time is an int or float, interpet as seconds since epoch
        dt = datetime.fromtimestamp(time.time())

    return dt


class ObjectID:

    """
        Uniquely identifies an HDF5 resource
    """

    @property
    def uuid(self):
        return self._uuid

    @property
    def id(self):
        return self.uuid

    def __hash__(self):
        return self.uuid

    @property
    def domain(self):
        """ domain for this obj """
        filepath = self.db.reader.filepath
        if not filepath:
            filepath = self.db.writer.filepath
        return filepath

    @property
    def obj_json(self):
        """json representation of the object"""
        return self.db.getObjectById(self.uuid)

    @property
    def modified(self):
        """last modified timestamp"""
        obj_json = self.obj_json
        if "lastModified" in obj_json:
            return obj_json["lastModified"]
        else:
            return None

    @property
    def db(self):
        """ db connector """
        return self._db

    @property
    def collection_type(self):
        """ Return collection type based on uuid """
        return getCollectionForId(self.uuid)

    def __init__(self, parent, obj_id, obj_json=None, db=None, **kwds):

        """Create a new objectId.
        """
        print(f"object init - id: {obj_id}, obj_json={obj_json}")
        parent_id = None
        if parent is not None:
            if isinstance(parent, ObjectID):
                parent_id = parent
            else:
                # assume we were passed a Group/Dataset/datatype
                parent_id = parent.id

        if obj_json and type(obj_json) is not dict:
            raise IOError("Unexpected Error")

        if not isValidUuid(obj_id):
            raise IOError(f"obj_id: {obj_id} is not valid")

        self._uuid = obj_id

        if db is not None:
            self._db = db
        elif parent_id is not None and parent_id.db is not None:
            self._db = parent_id.db
        else:
            raise IOError("Expected parent to have db connector")

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uuid == other._uuid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def refresh(self):
        """ get the latest obj_json data from server """

        # get the latest version of the object
        self.db.getObjectById(self.uuid, refresh=True)

    def close(self):
        """Remove handles to id.
        """
        self._old_uuid = self._uuid  # for debugging
        self._uuid = 0
        self._db = None

    def __bool__(self):
        return bool(self._uuid)

    def __del__(self):
        """ cleanup """
        self.close()


class TypeID(ObjectID):

    @property
    def type_json(self):
        return self.obj_json['type']

    def get_type(self):
        type_json = self.obj_json["type"]
        dtype = createDataType(type_json)
        return dtype

    @property
    def tcpl_json(self):
        if 'creationProperties' in self.obj_json:
            tcpl = self._obj_json['creationProperties']
        else:
            tcpl = {}
        return tcpl

    def __init__(self, parent, obj_id, obj_json=None, **kwds):
        """Create a new TypeID.
        """

        ObjectID.__init__(self, parent, obj_id, obj_json=obj_json, **kwds)

        if self.collection_type != "datatypes":
            raise IOError(f"Unexpected collection_type: {self._collection_type}")


class DatasetID(ObjectID):

    @property
    def type_json(self):
        return self._obj_json['type']

    @property
    def shape_json(self):
        return self._obj_json['shape']

    def get_type(self):
        type_json = self._obj_json["type"]
        dtype = createDataType(type_json)
        return dtype

    @property
    def dcpl_json(self):
        if 'creationProperties' in self.obj_json:
            dcpl = self.obj_json['creationProperties']
        else:
            dcpl = {}
        return dcpl

    @property
    def rank(self):
        rank = 0
        shape = self.obj_json['shape']
        if shape['class'] == 'H5S_SIMPLE':
            dims = shape['dims']
            rank = len(dims)
        return rank

    @property
    def layout(self):
        layout = None

        if 'layout' in self.obj_json:
            layout = self.obj_json['layout']
        else:
            dcpl = self.dcpl_json
            if dcpl and 'layout' in dcpl:
                layout = dcpl['layout']

        return layout

    @property
    def chunks(self):

        chunks = None
        layout = self.layout

        if layout and layout['class'] in ('H5D_CHUNKED', 'H5D_CHUNKED_REF', 'H5D_CHUNKED_REF_INDIRECT'):
            if "dims" in layout:
                chunks = layout['dims']

        return chunks

    def __init__(self, parent, obj_id, obj_json=None, **kwds):
        """Create a new DatasetID.
        """

        ObjectID.__init__(self, parent, obj_id, obj_json=obj_json, **kwds)

        if self.collection_type != "datasets":
            raise IOError(f"Unexpected collection_type: {self._collection_type}")


class GroupID(ObjectID):

    def __init__(self, parent, obj_id, obj_json=None, **kwds):
        """Create a new GroupID.
        """

        ObjectID.__init__(self, parent, obj_id, obj_json=obj_json, **kwds)

        if self.collection_type != "groups":
            raise IOError(f"Unexpected collection_type: {self._collection_type}")

    @property
    def gcpl_json(self):
        if 'creationProperties' in self._obj_json:
            gcpl = self._obj_json['creationProperties']
        else:
            gcpl = {}
        return gcpl
