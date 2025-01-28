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

MAX_PENDING_ITEMS = 500  # TBD: make this a config?


class ObjDB():
    """ Domain level object map """
    def __init__(self, http_conn, expire_time=0.0, max_age=0.0, max_objects=None):
        self._http_conn = weakref.ref(http_conn)
        self._objdb = {}
        self._load_times = {}
        if max_age > 0.0:
            self._pending = {}
        else:
            self._pending = None
        self._pending_count = 0
        self._missing_uuids = set()
        self._expire_time = expire_time
        self._max_age = max_age
        self._max_objects = max_objects
        self.log = http_conn.logging
        if http_conn.mode == 'r':
            self._read_only = True
        else:
            self._read_only = False

    @property
    def http_conn(self):
        # access weak ref
        conn = self._http_conn()
        if conn is None:
            raise RuntimeError("http connection has been garbage collected")
        return conn

    def _is_expired(self, obj_uuid):
        """ Get expired state of the uuid.  Return:
            None - if the item is not loaded
            True - if the item is loaded but expired
            False - if the item is loaded but not expired
        """
        if obj_uuid not in self._load_times:
            return None
        if self._expire_time > 0.0:
            age = time.time() - self._load_times[obj_uuid]

            return age > self._expire_time
        else:
            return False

    def _flush_pending(self):
        """ commit any pending updates"""
        if not self._pending:
            self.log.debug("flush_pending - no pending objects")
            return

        # flush attributes
        obj_ids = {}
        for obj_uuid in self._pending:
            pending_attrs = self._pending[obj_uuid]['attrs']
            if pending_attrs:
                if obj_uuid not in obj_ids:
                    obj_ids[obj_uuid] = {}
                obj_id = obj_ids[obj_uuid]
                obj_id["attributes"] = pending_attrs

        if obj_ids:
            body = {"obj_ids": obj_ids}
            root_uuid = self.http_conn.root_uuid
            req = f"/groups/{root_uuid}/attributes"
            rsp = self.http_conn.PUT(req, body=body)
            if rsp.status_code not in (200, 201):
                raise IOError(rsp.status_code, "Failed to update attributes")
            else:
                # clear items from pending queue
                for obj_id in obj_ids:
                    self._pending[obj_id]['attrs'] = {}

        # flush links
        obj_ids = {}
        for obj_uuid in self._pending:
            pending_links = self._pending[obj_uuid]['links']
            if pending_links:
                if obj_uuid not in obj_ids:
                    obj_ids[obj_uuid] = {}
                obj_id = obj_ids[obj_uuid]
                obj_id["links"] = pending_links

        if obj_ids:
            body = {"grp_ids": obj_ids}
            root_uuid = self.http_conn.root_uuid
            req = f"/groups/{root_uuid}/links"
            rsp = self.http_conn.PUT(req, body=body)
            if rsp.status_code not in (200, 201):
                raise IOError(rsp.status_code, "Failed to update links")
            else:
                # clear items from pending queue
                for obj_id in obj_ids:
                    self._pending[obj_id]['links'] = {}

        self._pending_count = 0

    def flush(self):
        """ commit all pending items """
        self._flush_pending()

    def fetch(self, obj_uuid):
        """ get obj_json for given obj_uuid from the server """

        self.log.debug(f"ObjDB.fetch({obj_uuid})")

        if obj_uuid in self._missing_uuids:
            msg = f"returning None for fetch since object {obj_uuid} is in missing_uuids set"
            self.log.warning(msg)
            return None

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
            self._missing_uuids.add(obj_uuid)
            return None
        elif rsp.status_code != 200:
            raise IOError(f"Unexpected error on request {req}: {rsp.status_code}")
        obj_json = rsp.json()
        return obj_json

    def __setitem__(self, obj_uuid, obj_json):
        """ set the obj_json in the db with obj_uuid as the key """
        if self._max_objects is not None:
            if len(self._objdb) >= self._max_objects:
                # over limit, skip set
                return
        discard_keys = ('root', 'id', 'attributeCount', 'linkCount', 'hrefs', 'domain', 'bucket')
        for k in discard_keys:
            if k in obj_json:
                del obj_json[k]

        if "attributes" not in obj_json:
            obj_json["attributes"] = {}
        if get_collection(obj_uuid) == "groups" and "links" not in obj_json:
            obj_json["links"] = {}

        # assign or replace current object
        self._objdb[obj_uuid] = obj_json
        self._load_times[obj_uuid] = time.time()

    def __getitem__(self, obj_uuid):
        """ get item from objdb, fetching from server if necessary """

        if self._is_expired(obj_uuid) in (None, True):
            # fetch latest json
            obj_json = self.fetch(obj_uuid)
            if obj_json is not None:
                self.__setitem__(obj_uuid, obj_json)
        else:
            obj_json = self._objdb[obj_uuid]

        if obj_json is None:
            self.log.warning(f"id: {obj_uuid} not found in objDB")
            raise KeyError(obj_uuid)
        return obj_json

    def free(self, obj_uuid):
        """ free from objdb """
        if obj_uuid in self._objdb:
            del self._objdb[obj_uuid]
        if obj_uuid in self._load_times:
            del self._load_times[obj_uuid]

    def __delitem__(self, obj_uuid):
        """ delete object frm server and free from objdb"""
        if self._read_only:
            raise IOError("no write intent on domain")

        if obj_uuid not in self._objdb:
            obj_json = self.fetch(obj_uuid)
            if not obj_json:
                self.log.warning(f"id: {obj_uuid} not found for deletion in objDB")
                raise KeyError(obj_uuid)
        collection = get_collection(obj_uuid)
        req = f"/{collection}/{obj_uuid}"
        self.http_conn.DELETE(req)
        self.free(obj_uuid)

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
            self.__setitem__(obj_uuid, obj_json)

    def reload(self, load_all=False):
        """ re-initialize objdb """
        self.log.info(f"objdb.reload {self.http_conn.domain}")
        self._objdb = {}
        self._loadtime = {}
        obj_uuids = set()
        obj_uuids.add(self.http_conn.root_uuid)
        if not load_all:
            return

        while obj_uuids:
            obj_uuid = obj_uuids.pop()
            obj_json = self.fetch(obj_uuid)
            self.__setitem__(obj_uuid, obj_json)

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

        obj_json = self.__getitem__(parent_uuid)
        if obj_json is None:
            self.log.warning("get_bypath - parent_uuid not found")
            raise KeyError("parent_uuid: {parent_uuid} not found")

        obj_id = parent_uuid
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
                obj_json = self.__getitem__(obj_id)
                if not obj_json:
                    self.log.warning(f"id: {obj_id} not found")
                    obj_json = None
                else:
                    searched_ids.add(obj_id)
                    obj_json = self._objdb[obj_id]
            elif link_class == 'H5L_TYPE_SOFT':
                self.log.warning("objdb.get_bypath can't follow soft links")
            elif link_class == 'H5L_TYPE_EXTERNAL':
                self.log.warning("objdb.get_bypath can't follow external links")
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

    def _get_pending(self, obj_uuid):
        """ get pending items """
        if obj_uuid not in self._pending:
            self._pending[obj_uuid] = {"links": {}, "attrs": {}}
        return self._pending[obj_uuid]

    def set_link(self, group_uuid, title, link_json, replace=False):
        """ create/update the given link """
        if not group_uuid.startswith("g-"):
            raise TypeError("objdb.set_link - expected a group identifier")
        if title.find('/') != -1:
            raise KeyError("objdb.setlink - link title can not be nested")
        if self._read_only:
            raise IOError("no write intent on domain")
        obj_json = self.__getitem__(group_uuid)
        links = obj_json["links"]
        link_json['created'] = time.time()

        if title in links and replace:
            # TBD: hsds update to for link replacement?
            self.del_link(group_uuid, title)
        if self._max_age > 0.0:
            pending_links = self._get_pending(group_uuid)["links"]
            pending_links[title] = link_json
            self._pending_count += 1
            if self._pending_count > MAX_PENDING_ITEMS:
                self._flush_pending()
        else:
            # do a PUT immediately
            # make a http put
            req = f"/groups/{group_uuid}/links/{title}"
            self.http_conn.PUT(req, body=link_json)  # create the link
        links[title] = link_json

    def del_link(self, group_uuid, title):
        """ Delete the given link """

        if title.find('/') != -1:
            raise KeyError("objdb.del_link - link title can not be nested")
        if self._read_only:
            raise IOError("no write intent on domain")
        obj_json = self.__getitem__(group_uuid)
        links = obj_json["links"]
        # tbd - validate link_json?
        if self._max_age > 0.0:
            pending_links = self._get_pending(group_uuid)["links"]
            if title in pending_links:
                del pending_links[title]

        if title in links:
            pending_links = self._get_pending(group_uuid)["links"]
            if title in pending_links:
                del pending_links[title]

            # TBD - support deferred delete
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
        if self._read_only:
            raise IOError("no write intent on domain")
        cfg = config.get_config()  # pulls in state from a .hscfg file (if found).

        if track_order is None:
            track_order = cfg.track_order
        link_json = {}
        if parent_uuid and title:
            if title.find('/') != -1:
                raise KeyError("link title can not be nested")
            if parent_uuid not in self._objdb:
                self.log.warning(f"make_obj: {parent_uuid} not in objdb")

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
        self.__setitem__(obj_uuid, obj_json)  # update group db
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
        if self._read_only:
            raise IOError("no write intent on domain")
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
        if self._read_only:
            raise IOError("no write intent on domain")
        obj_json = self.__getitem__(obj_uuid)
        attrs = obj_json["attributes"]
        attr_json['created'] = time.time()

        if self._max_age > 0.0:
            pending_links = self._get_pending(obj_uuid)["attrs"]
            pending_links[name] = attr_json
            self._pending_count += 1
            if self._pending_count > MAX_PENDING_ITEMS:
                self._flush_pending()
        else:
            # do a PUT immediately

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
        attrs[name] = attr_json

    def del_attr(self, obj_uuid, name):
        """ delete the given attribute """
        if self._read_only:
            raise IOError("no write intent on domain")
        obj_json = self.__getitem__(obj_uuid)
        attrs = obj_json["attributes"]
        if name not in attrs:
            self.log.warning(f"attr {name} of {obj_uuid} not found for delete")
            raise KeyError("Unable to delete attribute (can't locate attribute)")
        if self._max_age > 0.0:
            pending_attrs = self._get_pending(obj_uuid)["attrs"]
            if name in pending_attrs:
                del pending_attrs[name]

        # tbd - support deferred deletion
        collection = get_collection(obj_uuid)
        req = f"/{collection}/{obj_uuid}/attributes/{name}"
        rsp = self.http_conn.DELETE(req)

        if rsp.status_code != 200:
            self.log.error(f"got {rsp.status_code} for delete req: {req}")
            raise RuntimeError(f"Unexpected error on delete request {req}: {rsp.status_code}")
        # remove from the objdb
        del attrs[name]

    def shape_refresh(self, dset_uuid):
        """ Get the latest dataset shape """
        if dset_uuid not in self._objdb:
            # just need to do a fetch...
            self.fetch(dset_uuid)
        else:
            obj_json = self._objdb[dset_uuid]
            req = f"/datasets/{dset_uuid}/shape"
            rsp = self.http_conn.GET(req)
            if rsp.status_code != 200:
                msg = "unable to get dataset shape"
                raise IOError(rsp.status_code, msg)
            rsp_json = rsp.json()
            if "shape" not in rsp_json:
                raise RuntimeError(f"Unexpected response for {req}")
            shape_json = rsp_json['shape']
            obj_json['shape'] = shape_json

    def resize(self, dset_uuid, dims):
        """ update the shape of the dataset """
        if self._read_only:
            raise IOError("no write intent on domain")
        # send the request to the server
        body = {"shape": dims}
        req = f"/datasets/{dset_uuid}/shape"
        rsp = self.http_conn.PUT(req, body=body)
        if rsp.status_code not in (200, 201):
            msg = "unable to resize dataset shape, error"
            raise IOError(rsp.status_code, msg)
        # TBD Have HSDS return updated shape in response to avoid
        # this GET request
        self.shape_refresh(dset_uuid)
