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
        Uniquely identifies an h5serv resource
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
        return self.http_conn.domain

    @property
    def obj_json(self):
        """json representation of the object"""
        return self._obj_json

    @property
    def modified(self):
        """last modified timestamp"""
        return self._modified

    @property
    def http_conn(self):
        """ http connector """
        return self._http_conn

    @property
    def collection_type(self):
        """ Return collection type based on uuid """
        if self._uuid.startswith("g-"):
            collection_type = "groups"
        elif self._uuid.startswith("t-"):
            collection_type = "datatypes"
        elif self._uuid.startswith("d-"):
            collection_type = "datasets"
        else:
            raise IOError(f"Unexpected uuid: {self._uuid}")
        return collection_type

    def __init__(self, parent, item, http_conn=None, **kwds):

        """Create a new objectId.
        """
        parent_id = None
        if parent is not None:
            if isinstance(parent, ObjectID):
                parent_id = parent
            else:
                # assume we were passed a Group/Dataset/datatype
                parent_id = parent.id

        if type(item) is not dict:
            raise IOError("Unexpected Error")

        if "id" not in item:
            raise IOError("Unexpected Error")

        self._uuid = item['id']

        self._modified = parse_lastmodified(item['lastModified'])

        self._obj_json = item

        if http_conn is not None:
            self._http_conn = http_conn
        elif parent_id is not None and parent_id.http_conn is not None:
            self._http_conn = parent_id.http_conn
        else:
            raise IOError("Expected parent to have http connector")

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uuid == other._uuid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def refresh(self):
        """ get the latest obj_json data from server """

        # will need to get JSON from server
        req = f"/{self.collection_type}/{self.id}"
        # make server request
        rsp = self._http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(f"refresh request got status: {rsp.satus_code}")
        item = json.loads(rsp.text)

        self._obj_json = item
        self._modified = parse_lastmodified(item['lastModified'])

        objdb = self.http_conn._objdb
        if objdb and self.id in objdb:
            # delete any cached data from objdb so that gets will reflect server state
            del objdb[self.id]

    def close(self):
        """Remove handles to id.
        """
        self._old_uuid = self._uuid  # for debugging
        self._uuid = 0
        self._obj_json = None
        self._http_conn = None

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
        type_json = self._obj_json["type"]
        dtype = createDataType(type_json)
        return dtype

    def __init__(self, parent, item, **kwds):
        """Create a new TypeID.
        """

        ObjectID.__init__(self, parent, item, **kwds)

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
        if 'creationProperties' in self._obj_json:
            dcpl = self._obj_json['creationProperties']
        else:
            dcpl = {}
        return dcpl

    @property
    def rank(self):
        rank = 0
        shape = self._obj_json['shape']
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

    def __init__(self, parent, item, **kwds):
        """Create a new DatasetID.
        """

        ObjectID.__init__(self, parent, item, **kwds)

        if self.collection_type != "datasets":
            raise IOError(f"Unexpected collection_type: {self._collection_type}")


class GroupID(ObjectID):

    def __init__(self, parent, item, http_conn=None, **kwds):
        """Create a new GroupID.
        """

        ObjectID.__init__(self, parent, item, http_conn=http_conn, **kwds)

        if self.collection_type != "groups":
            raise IOError(f"Unexpected collection_type: {self._collection_type}")
