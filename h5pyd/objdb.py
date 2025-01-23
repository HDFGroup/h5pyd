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

import time
import weakref
from . import config
from .objectid import get_collection


class ObjDB():
    """ Domain level object map """
    def __init__(self, http_conn, use_cache=True):
        self._http_conn = weakref.ref(http_conn)
        self._objdb = {}
        self._loadtime = {}
        self._use_cache = use_cache
        self.log = http_conn.logging

    @property
    def http_conn(self):
        # access weark ref
        conn = self._http_conn()
        if conn is None:
            raise RuntimeError("http connection has been garbage collected")
        return conn

    def fetch(self, obj_uuid):
        """ get obj_json for given obj_uuid from the server """

        self.log.debug(f"ObjDB.fetch({obj_uuid})")

        if obj_uuid.startswith("g-"):
            collection_type = "groups"
        elif obj_uuid.startswith("t-"):
            collection_type = "datatypes"
        elif obj_uuid.startswith("d-"):
            collection_type = "datasets"
        else:
            msg = f"Unexpected obj_uuid: {obj_uuid}"
            self.log.error(msg)
            raise IOError(msg)

        req = f"/{collection_type}/{obj_uuid}"
        # make server request
        params = {"include_attrs": 1}
        if collection_type == "groups":
            # get links as well
            params["include_links"] = 1
        rsp = self.http_conn.GET(req, params=params)
        if rsp.status_code in (404, 410):
            self.log.warning(f"obj: {obj_uuid} not found")
            return None
        elif rsp.status_code != 200:
            raise IOError(f"Unexpected error on request {req}: {rsp.status_code}")
        obj_json = rsp.json()
        self.__set_item__(obj_uuid, obj_json)
        return obj_json

    def __set_item__(self, obj_uuid, obj_json):
        """ set the obj_json in the db with obj_uuid as the key """

        discard_keys = ('root', 'id', 'attributeCount', 'linkCount', 'hrefs', 'domain', 'bucket')
        # tbd: should bucket be supported?  Not being returned in GET request
        for k in discard_keys:
            if k in obj_json:
                del obj_json[k]

        if "attributes" not in obj_json:
            obj_json["attributes"] = {}
        if get_collection(obj_uuid) == "groups" and "links" not in obj_json:
            obj_json["links"] = {}

        # assign or replace current object
        self._objdb[obj_uuid] = obj_json
        self._loadtime[obj_uuid] = time.time()

        return obj_json

    def __getitem__(self, obj_uuid):
        if obj_uuid not in self._objdb:
            self.log.warning(f"id: {obj_uuid} not found in objDB")
            raise KeyError(obj_uuid)
        obj_json = self._objdb[obj_uuid]
        return obj_json

    def __delitem__(self, obj_uuid):
        if obj_uuid not in self._objdb:
            print(f"{obj_uuid} not in objdb, fetching")
            obj_json = self.fetch(obj_uuid)
            if not obj_json:
                self.log.warning(f"id: {obj_uuid} not found for deletion in objDB")
                raise KeyError(obj_uuid)
        collection = get_collection(obj_uuid)
        req = f"/{collection}/{obj_uuid}"
        self._http_conn.DELETE(req)
        del self._objdb[obj_uuid]
        del self._loadtime[obj_uuid]

    def __len__(self):
        return len(self._objdb)

    def __iter__(self):
        for obj_uuid in self._objdb:
            yield obj_uuid

    def __contains__(self, obj_uuid):
        if obj_uuid in self._objdb:
            return True
        else:
            return False

    def load(self, domain_objs):
        """ load content from hsds summary json """
        for obj_uuid in domain_objs:
            obj_json = domain_objs[obj_uuid]
            self.__set_item__(obj_uuid, obj_json)

    def reload(self):
        """ re-initialize objdb """
        self.log.info(f"objdb.reload {self.http_conn.domain}")
        self._objdb = {}
        self._loadtime = {}
        obj_uuids = set()
        obj_uuids.add(self.http_conn.root_uuid)
        while obj_uuids:
            obj_uuid = obj_uuids.pop()
            obj_json = self.fetch(obj_uuid)
            self.__set_item__(obj_uuid, obj_json)

            if "links" in obj_json:
                # add ids for any hard-links to our search if
                # not in the db already
                links = obj_json["links"]
                for title in links:
                    self.log.debug(f"got link: {title}")
                    link = links[title]
                    if "class" not in link:
                        self.log.error(f"expected to find class key in {link}")
                        continue
                    if link['class'] != 'H5L_TYPE_HARD':
                        continue  # only care about hard links
                    if "id" not in link:
                        self.log.error(f"expected to find id key in {link}")
                        continue
                    link_id = link['id']
                    if link_id in self._objdb:
                        # we've already fetched this object
                        continue
                    self.log.debug(f"adding hardlink id: {link_id}")
                    obj_uuids.add(link_id)

        self.log.info(f"objdb.reload complete, {len(self._objdb)} objects loaded")

    def get_bypath(self, parent_uuid, h5path, follow=False, getlink=False):
        """ Return obj_json for the given link path starting from parent_uuid """
        self.log.debug(f"get_bypath(parent_uuid: {parent_uuid}), h5path: {h5path}")
        if not parent_uuid.startswith("g-"):
            self.log.error("get_bypath - expected parent_uuid to be a group id")
            raise TypeError()
        if parent_uuid not in self._objdb:
            self.log.warning("get_bypath - parent_uuid not found")
            raise KeyError("parent_uuid: {parent_uuid} not found")

        obj_id = parent_uuid
        obj_json = self._objdb[obj_id]
        searched_ids = set(obj_id)

        link_names = h5path.split('/')
        self.log.debug(f"link_names: {link_names}")
        for link_name in link_names:
            if not link_name:
                continue
            link_tgt = None
            self.log.debug(f"link_name: {link_name}")
            if not obj_id:
                break
            if not obj_id.startswith("g-"):
                self.log.error(f"get_bypath, {link_name} is not a group")
                raise KeyError(h5path)
            if 'links' not in obj_json:
                self.log.error(f"expected to find links key in: {obj_json}")
                raise KeyError(h5path)
            links = obj_json['links']
            self.log.debug(f"links: {links}")
            if link_name not in links:
                self.log.warning(f"link: {link_name} not found in {obj_id}")
                self.log.debug(f"links: {links}")
                raise KeyError(h5path)
            link_tgt = links[link_name]
            self.log.debug(f"link_tgt: {link_tgt}")
            link_class = link_tgt['class']
            obj_id = None
            obj_json = None
            if link_class == 'H5L_TYPE_HARD':
                # hard link
                obj_id = link_tgt['id']
                if obj_id in searched_ids:
                    self.log.warning(f"circular reference using path: {h5path}")
                    raise KeyError(h5path)
                if obj_id not in self._objdb:
                    # TBD - fetch from the server in case this object has not
                    # been loaded yet?
                    self.log.warning(f"id: {obj_id} not found")
                    obj_json = None
                else:
                    searched_ids.add(obj_id)
                    obj_json = self._objdb[obj_id]
            elif link_class == 'H5L_TYPE_SOFT':
                if not follow:
                    continue
                # soft link
                slink_path = link_tgt['h5path']
                if not slink_path:
                    self.log.warning(f"id: {obj_id} has null h5path for link: {link_name}")
                    raise KeyError(h5path)
                if slink_path.startswith('/'):
                    slink_id = self.http_conn.root_uuid
                else:
                    slink_id = obj_id
                # recursive call
                try:
                    obj_json = self.get_bypath(slink_id, slink_path)
                except KeyError:
                    self.log.warning(f"Unable to find object in softpath: {slink_path}")
                    continue
                obj_id = obj_json['id']
            elif link_class == 'H5L_TYPE_EXTERNAL':
                if not follow:
                    continue
                # tbd
                self.log.error("external link not supported")
            else:
                self.log.error(f"link type: {link_class} not supported")

        if getlink:
            if not link_tgt:
                self.log.warning("get_bypath link at {h5path} not found")
                raise KeyError(h5path)
            self.log.info(f"get_bypath link at {h5path} found link: {link_tgt}")
            return link_tgt
        else:
            if not obj_id:
                self.log.warning(f"get_bypath {h5path} not found")
                raise KeyError(h5path)
            self.log.info(f"get_bypath link at {h5path} found target: {obj_id}")
            return obj_json

    def set_link(self, group_uuid, title, link_json, replace=False):
        """ create/update the given link """
        if not group_uuid.startswith("g-"):
            raise TypeError("objdb.set_link - expected a group identifier")
        if title.find('/') != -1:
            raise KeyError("objdb.setlink - link title can not be nested")
        obj_json = self.__getitem__(group_uuid)
        links = obj_json["links"]
        if title in links and replace:
            # TBD: hsds update to for link replacement?
            self.del_link(group_uuid, title)
        # make a http put
        req = f"/groups/{group_uuid}/links/{title}"
        self.http_conn.PUT(req, body=link_json)  # create the link
        link_json['created'] = time.time()
        links[title] = link_json

    def del_link(self, group_uuid, title):
        """ Delete the given link """

        if title.find('/') != -1:
            raise KeyError("objdb.del_link - link title can not be nested")
        obj_json = self.__getitem__(group_uuid)
        links = obj_json["links"]
        # tbd - validate link_json?
        if title in links:
            req = f"/groups/{group_uuid}/links/{title}"
            rsp = self.http_conn.DELETE(req)
            if rsp.status_code != 200:
                raise IOError(rsp.status_code, f"failed to delete link: {title}")
            # ok - so delete our cached copy
            del links[title]
        else:
            self.log.warning(f"title: {title} not found in objdb for id {id}")

    def make_obj(self, parent_uuid, title, type_json=None, shape=None, cpl=None, track_order=None, maxdims=None):
        """ create a new  object
          If type_json and shape_json are none - create a group
          If type_json and shape_json - create a dataset
          If type_json and not shape_json - create a datatype
        """
        cfg = config.get_config()  # pulls in state from a .hscfg file (if found).

        if track_order is None:
            track_order = cfg.track_order
        link_json = {}
        if parent_uuid and title:
            if title.find('/') != -1:
                raise KeyError("link title can not be nested")
            if parent_uuid not in self._objdb:
                raise KeyError(f"parent_uuid: {parent_uuid} not found")

            link_json["name"] = title

        body = {}
        if link_json:
            body["link"] = link_json

        if type_json:
            body['type'] = type_json
            if shape is not None:
                body['shape'] = shape
                if maxdims:
                    body['maxdims'] = maxdims
                req = "/datasets"
            else:
                req = "/datatypes"
        else:
            if shape:
                raise KeyError("shape set, but no type")
            req = "/groups"

        if track_order:
            if not cpl:
                cpl = {}
            cpl['CreateOrder'] = 1
        if cpl:
            body['creationProperties'] = cpl

        # self.log.debug(f"create group with body: {body}")
        rsp = self.http_conn.POST(req, body=body)
        self.log.info(f"got status code: {rsp.status_code} for POST req: {req}")

        if rsp.status_code not in (200, 201):
            raise IOError(f"req: {req} failed with status: {rsp.status.code}")

        obj_json = rsp.json()
        # mixin creation props if set
        if cpl:
            obj_json['creationProperties'] = cpl
        obj_uuid = obj_json['id']
        self.__set_item__(obj_uuid, obj_json)  # update group db
        if link_json:
            # tweak link_json to look like a link entry on objdb
            link_json['class'] = 'H5L_TYPE_HARD'
            link_json['created'] = time.time()
            link_json['id'] = obj_uuid
            del link_json['name']
            self.set_link(parent_uuid, title, link_json)

        return obj_uuid

    def del_obj(self, obj_uuid):
        """ Delete the given object """
        collection = get_collection(obj_uuid)
        req = f"/{collection}/{obj_uuid}"

        rsp = self.http_conn.DELETE(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, f"failed to delete object: {obj_uuid}")
        # ok - so delete our cached copy
        if obj_uuid in self._objdb:
            del self._objdb[obj_uuid]

    def set_attr(self, obj_uuid, name, attr_json):
        """ create update attribute  """
        obj_json = self.__getitem__(obj_uuid)
        attrs = obj_json["attributes"]
        params = {}
        if name in attrs:
            self.log.debug(f"replacing attr {name} of {obj_uuid}")
            params['replace'] = 1

        collection = get_collection(obj_uuid)
        req = f"/{collection}/{obj_uuid}/attributes/{name}"
        rsp = self.http_conn.PUT(req, body=attr_json, params=params)

        if rsp.status_code not in (200, 201):
            self.log.error(f"got {rsp.status_code} for put req: {req}")
            raise RuntimeError(f"Unexpected error on put request {req}: {rsp.status_code}")
        self.log.info(f"got {rsp.status_code} for req: {req}")
        attr_json['created'] = time.time()
        attrs[name] = attr_json

    def del_attr(self, obj_uuid, name):
        """ delete the given attribute """
        obj_json = self.__getitem__(obj_uuid)
        attrs = obj_json["attributes"]
        if name not in attrs:
            self.log.warning(f"attr {name} of {obj_uuid} not found for delete")
            raise KeyError("Unable to delete attribute (can't locate attribute)")

        collection = get_collection(obj_uuid)
        req = f"/{collection}/{obj_uuid}/attributes/{name}"
        rsp = self.http_conn.DELETE(req)

        if rsp.status_code != 200:
            self.log.error(f"got {rsp.status_code} for delete req: {req}")
            raise RuntimeError(f"Unexpected error on delete request {req}: {rsp.status_code}")
        # remove from the objdb
        del attrs[name]

    def resize(self, dset_uuid, dims):
        """ update the shape of the dataset """
        # send the request to the server
        body = {"shape": dims}
        req = f"/datasets/{dset_uuid}/shape"
        rsp = self.http_conn.PUT(req, body=body)
        if rsp.status_code not in (200, 201):
            msg = "unable to resize dataset shape, error"
            raise IOError(rsp.status_code, msg)
        # TBD Have HSDS return updated shape in response to avoid
        # this GET request
        self.fetch(dset_uuid)
