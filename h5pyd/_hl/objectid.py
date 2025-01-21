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
import pytz
import time
import weakref
from .h5type import createDataType


def parse_lastmodified(datestr):
    """Turn last modified datetime string into a datetime object."""
    if isinstance(datestr, str):
        # format: 2016-06-30T06:17:16.563536Z
        # format: "2016-08-04T06:44:04Z"
        dt = datetime.strptime(
            datestr, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    else:
        # if the time is an int or float, interpret as seconds since epoch
        dt = datetime.fromtimestamp(time.time())

    return dt


def isUUID(name):
    # return True if name looks like an object id
    # There are some additional checks we could add to reduce false positives
    # (like checking for hyphens in the right places)
    if isinstance(name, str) and len(name) >= 38:
        if name.startswith("groups/") or name.startswith("g-"):
            return True
        elif name.startswith("datatypes/") or name.startswith("t-"):
            return True
        elif name.startswith("datasets/") or name.startswith("d-"):
            return True
        else:
            return False
    else:
        return False


def get_UUID(name):
    """ return just the uuid part if the ref starts with 'groups/', 'datasets/' etc. """
    if not isUUID(name):
        raise IOError(f"expected a uuid, but got: {name}")
    if name.startswith("groups/"):
        obj_uuid = name[len("groups/"):]
    elif name.startswith("datatypes/"):
        obj_uuid = name[len("datatypes/")]
    elif name.startswith("datasets/"):
        obj_uuid = name[len("datasets/"):]
    else:
        obj_uuid = name
    return obj_uuid


def get_class_for_uuid(uuid):
    """ Return class based on uuid """
    if not uuid:
        return None
    obj_uuid = get_UUID(uuid)
    if obj_uuid.startswith("g-"):
        return GroupID
    elif obj_uuid.startswith("d-"):
        return DatasetID
    elif obj_uuid.startswith("t-"):
        return TypeID
    else:
        raise TypeError(f"unexpected uuid string: {obj_uuid}")


class ObjectID:

    """
        Uniquely identifies a resource
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
        objdb = self.http_conn.objdb
        obj_json = objdb[self.uuid]
        return obj_json

    @property
    def modified(self):
        """last modified timestamp"""
        obj_json = self.obj_json

        last_modified = obj_json['lastModified']
        """Turn last modified datetime string into a datetime object."""
        if isinstance(last_modified, str):
            # format: 2016-06-30T06:17:16.563536Z
            # format: "2016-08-04T06:44:04Z"
            dt = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
        else:
            # if the time is an int or float, interpret as seconds since epoch
            dt = datetime.fromtimestamp(time.time())

        return dt

    @property
    def http_conn(self):
        # access weak ref
        if isinstance(self._http_conn, weakref.ReferenceType):
            conn = self._http_conn()
            if conn is None:
                raise RuntimeError("http connection has been garbage collected")
        else:
            return self._http_conn
        return conn

    @property
    def objdb(self):
        # get ref to ObjDB instance
        http_conn = self.http_conn
        return http_conn.objdb

    @property
    def collection_type(self):
        """ Return collection type based on uuid """
        if self.uuid.startswith("g-"):
            collection_type = "groups"
        elif self.uuid.startswith("t-"):
            collection_type = "datatypes"
        elif self.uuid.startswith("d-"):
            collection_type = "datasets"
        else:
            raise IOError(f"Unexpected uuid: {self.uuid}")
        return collection_type

    @property
    def cpl(self):
        # return creation property list
        if 'creationProperties' in self.obj_json:
            cpl = self.obj_json['creationProperties']
        else:
            cpl = {}
        return cpl

    @property
    def track_order(self):
        """ Return the track_order state """
        track_order = None
        cpl = self.cpl
        if "CreateOrder" in cpl:
            createOrder = cpl["CreateOrder"]
            if not createOrder or createOrder == "0":
                track_order = False
            else:
                track_order = True
        return track_order

    def get(self, obj_uuid):
        """ Return id obj for given uuid """
        obj_class = get_class_for_uuid(obj_uuid)
        if obj_class is GroupID:
            obj = GroupID(obj_uuid, http_conn=self.http_conn)
        elif obj_class is TypeID:
            obj = TypeID(obj_uuid, http_conn=self.http_conn)
        elif obj_class is DatasetID:
            obj = DatasetID(obj_uuid, http_conn=self.http_conn)
        else:
            raise TypeError(f"Unexpected type: {obj_uuid}")

        return obj

    @property
    def attrs(self):
        obj_json = self.obj_json
        if "attributes" not in obj_json:
            raise IOError(f"expected to find attributes key in obj_json for {self._uuid}")
        return obj_json['attributes']

    def set_attr(self, name, attr):
        """ Create the given attribute """
        self.objdb.set_attr(self._uuid, name, attr)

    def get_attr(self, name):
        """ Return the given attribute """
        attrs = self.attrs
        if name not in attrs:
            raise KeyError(f"Unable to get attribute (can't locate attribute: '{name}'")
        attr = attrs[name]

        return attr

    def del_attr(self, name):
        """ Delete the named attribute """
        self.objdb.del_attr(self._uuid, name)

    def has_attr(self, name):
        """ Test if an attribute name exists """
        attrs = self.attrs
        if name in attrs:
            return True
        else:
            return False

    @property
    def attr_count(self):
        """ Get the number of attributes """
        attrs = self.attrs
        return len(attrs)

    def get_attr_names(self, track_order=None):
        """ Get a list of attribute names """
        attrs = self.attrs
        if track_order is None:
            track_order = self.track_order

        # convert to a list of dicts
        attr_list = []
        for title in attrs:
            attr_json = attrs[title]
            item = {}
            item['title'] = title
            item['created'] = attr_json['created']
            attr_list.append(item)

        if track_order:
            attr_list.sort(key=lambda d: d['created'])
        else:
            attr_list.sort(key=lambda d: d['title'])
        names = [x['title'] for x in attr_list]
        return names

    def __init__(self, obj_uuid, http_conn=None):

        """Create a new objectId.
        """
        self._uuid = get_UUID(obj_uuid)

        if http_conn:
            # use a weakref here so we don't keep a potentially large
            # objdb in memory accidentally
            self._http_conn = weakref.ref(http_conn)
        else:
            raise IOError("Expected parent to have http connector")

        if self._uuid not in self.objdb:
            self.objdb.fetch(self._uuid)  # will throw IOError if not found

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uuid == other.uuid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def refresh(self):
        """ get the latest obj_json data from server """
        self.objdb.fetch(self.uuid)

    def close(self):
        """Remove handles to id.
        """
        self._old_uuid = self.uuid  # for debugging
        self._uuid = 0
        self._http_conn = None

    def __bool__(self):
        """ Return true if the weak ref to http_conn is still valid """
        return bool(self._http_conn())

    def __del__(self):
        """ cleanup """
        self.close()

    def __repr__(self):
        class_name = self.__class__.__name__
        if self._uuid and self._http_conn():
            r = f"<{class_name}({self._uuid})>"
        else:
            r = f"<Closed {class_name}>"

        return r


class TypeID(ObjectID):

    @property
    def type_json(self):
        obj_json = self.obj_json
        return obj_json['type']

    def get_type(self):
        obj_json = self.obj_json
        type_json = obj_json["type"]
        dtype = createDataType(type_json)
        return dtype

    def __init__(self, obj_id, http_conn=None):
        """Create a new TypeID.
        """

        if get_class_for_uuid(obj_id) != TypeID:
            raise IOError(f"unexpected id for TypeID: {obj_id}")

        super().__init__(obj_id, http_conn=http_conn)


class DatasetID(ObjectID):

    @property
    def type_json(self):
        obj_json = self.obj_json
        return obj_json['type']

    @property
    def shape_json(self):
        obj_json = self.obj_json
        return obj_json['shape']

    def get_type(self):
        type_json = self.type_json
        dtype = createDataType(type_json)
        return dtype

    @property
    def type_class(self):
        return self.type_json['class']

    @property
    def rank(self):
        rank = 0
        shape = self.shape_json
        if shape['class'] == 'H5S_SIMPLE':
            dims = shape['dims']
            rank = len(dims)
        return rank

    @property
    def layout(self):
        layout = None
        obj_json = self.obj_json
        if 'layout' in obj_json:
            layout = obj_json['layout']
        else:
            cpl = self.cpl
            if 'layout' in cpl:
                layout = cpl['layout']

        return layout

    @property
    def chunks(self):

        chunks = None
        layout = self.layout

        if layout and layout['class'] in ('H5D_CHUNKED', 'H5D_CHUNKED_REF', 'H5D_CHUNKED_REF_INDIRECT'):
            if "dims" in layout:
                chunks = layout['dims']

        return chunks

    def __init__(self, obj_id, http_conn=None):
        """Create a new DatasetID.
        """
        if get_class_for_uuid(obj_id) != DatasetID:
            raise IOError(f"unexpected id for DatasetID: {obj_id}")
        super().__init__(obj_id, http_conn=http_conn)

    def getVerboseInfo(self):
        req = f"/datasets/{self._uuid}"
        params = {'verbose': 1}
        rsp = self.http_conn.GET(req, params=params)
        if rsp.status_code != 200:
            raise RuntimeError(f"get status: {rsp.status_code} for {req}")
        rsp_json = rsp.json()
        return rsp_json

    def resize(self, dims):
        """ update the shape of the dataset """
        # send the request to the server
        objdb = self.http_conn.objdb
        objdb.resize(self._uuid, dims)


class GroupID(ObjectID):

    def __init__(self, obj_id, http_conn=None):
        """Create a new GroupID.
        """
        if get_class_for_uuid(obj_id) != GroupID:
            raise IOError(f"unexpected id for GroupIID: {obj_id}")

        super().__init__(obj_id, http_conn=http_conn)

    @property
    def links(self):
        obj_json = self.obj_json
        if "links" not in obj_json:
            raise IOError(f"expected to find links key in obj_json for {self._uuid}")
        return obj_json['links']

    def make_obj(self, title, type_json=None, shape=None, cpl=None, track_order=None, maxdims=None):
        obj_json = self.obj_json
        if title:
            links = obj_json['links']
            if title in links:
                raise IOError("Unable to create object (name already exists)")
        objdb = self.http_conn.objdb
        kwds = {}

        if shape is not None:
            kwds['shape'] = shape
        if type_json:
            kwds['type_json'] = type_json
        if cpl:
            kwds['cpl'] = cpl
        if track_order:
            kwds['track_order'] = track_order
        if maxdims:
            kwds['maxdims'] = maxdims
        obj_uuid = objdb.make_obj(self._uuid, title, **kwds)
        obj_id = self.get(obj_uuid)
        return obj_id

    def get_link(self, title):
        """ return link json given it's title """
        links = self.links
        if title not in links:
            raise KeyError(f"link {title} not found")
        link_json = links[title]
        return link_json

    def set_link(self, title, link_json, replace=False):
        """ set the given link """
        links = self.links
        if not replace and title in links:
            raise IOError("Unable to create link (name already exists)")
        objdb = self.http_conn.objdb

        objdb.set_link(self.uuid, title, link_json, replace=replace)

    def del_link(self, title):
        """ delete the given link """
        links = self.links
        if title not in links:
            # not found
            raise KeyError(f"link '{title}' not found")
        objdb = self.http_conn.objdb
        objdb.del_link(self.uuid, title)

    @property
    def link_count(self):
        """ return number of links """
        links = self.links
        return len(links)

    def get_link_titles(self, track_order=None):
        links = self.links
        if track_order is None:
            track_order = self.track_order

        # convert to a list of dicts
        link_list = []
        for title in links:
            link_json = links[title]
            item = {}
            item['title'] = title
            item['created'] = link_json['created']
            link_list.append(item)

        if track_order:
            link_list.sort(key=lambda d: d['created'])
        else:
            link_list.sort(key=lambda d: d['title'])
        titles = [x['title'] for x in link_list]
        return titles

    def has_link(self, title):
        """ Test if a link name exists """
        links = self.links
        if title in links:
            return True
        else:
            return False


class FileID(GroupID):

    def __init__(self, root_uuid, http_conn=None):
        super().__init__(root_uuid, http_conn=http_conn)
        self._file_conn = http_conn  # keep a strong ref here

    def __bool__(self):
        if self._file_conn:
            return True
        else:
            return False

    def close(self):
        """Remove handles to id.
        """

        self._file_conn = None
        super().close()
