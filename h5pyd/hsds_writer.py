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
import logging
import time

from h5json.objid import getCollectionForId

from h5json.hdf5dtype import isVlen
from h5json.array_util import arrayToBytes, bytesArrayToList
from h5json.shape_util import getNumElements, getShapeDims
from h5json import selections
from h5json.h5writer import H5Writer
from .httpconn import HttpConn


class HSDSWriter(H5Writer):
    """
    This class can be used by HDF5DB to read content from an hdf5-json file
    """

    def __init__(
        self,
        domain_path,
        append=False,
        no_data=False,
        app_logger=None,
        endpoint=None,
        username=None,
        password=None,
        bucket=None,
        api_key=None,
        use_session=True,
        swmr=False,
        expire_time=0,
        max_objects=0,
        max_age=0,
        retries=3,
        timeout=30.0,
        track_order=None,
        owner=None,
        linked_domain=None

    ):
        if app_logger:
            self.log = app_logger
        else:
            self.log = logging.getLogger()

        if append:
            self._init = False
        else:
            self._init = True

        if no_data:
            self._no_data = True
        else:
            self._no_data = False

        self._swmr = swmr

        self.log.debug("HSDSWriter init")

        kwargs = {}
        self.log.debug(f"    domain_path: {domain_path}")
        self.log.debug(f"    append: {append}")
        if endpoint:
            self.log.debug(f"    endpoint: {endpoint}")
            kwargs["endpoint"] = endpoint
        if username:
            self.log.debug(f"    username: {username}")
            kwargs["username"] = username
        if password:
            self.log.debug(f"    password: {'*' * len(password)}")
            kwargs["password"] = password
        if bucket:
            self.log.debug(f"    bucket: {bucket}")
            kwargs["bucket"] = bucket
        if api_key:
            self.log.debug(f"    apI_key: {'*' * len(api_key)}")
            kwargs["api_key"] = api_key
        if use_session:
            self.log.debug(f"    use_session: {use_session}")
            kwargs["user_session"] = use_session
        if expire_time:
            self.log.debug(f"    expire_time: {expire_time}")
            kwargs["expire_time"] = expire_time
        if max_objects:
            self.log.debug(f"    max_objects: {max_objects}")
            kwargs["max_objects"] = max_objects
        if max_age:
            self.log.debug(f"    max_age: {max_age}")
            kwargs["max_age"] = max_age
        if retries:
            self.log.debug(f"    retries: {retries}")
            kwargs["retries"] = retries
        if timeout:
            self.log.debug(f"    timeout: {timeout}")
            kwargs["timeout"] = timeout
        self._http_kwargs = kwargs  # save for when we create the connection

        super().__init__(domain_path, app_logger=app_logger)

        self._http_conn = None
        self._root_id = None
        self._append = append
        self._track_order = track_order
        self._owner = owner
        self._linked_domain = linked_domain
        self._last_flush_time = 0
        self._stats = {"created": 0, "lastModified": 0, "owner": ""}

    def open(self):
        """ setup domain for writing """
        if not self._db_ref:
            # no db set yet
            raise IOError("DB not set")

        if self._http_conn and not self._http_conn.isClosed():
            return self._root_id

        if not self._http_conn:
            kwargs = self._http_kwargs
            kwargs["retries"] = 1  # tbd: test setting
            http_conn = HttpConn(self.filepath, **kwargs)
            if self._append:
                http_conn._mode = "a"
                self.log.debug("hsdswriter - set http_conn mode to a")
            self._http_conn = http_conn

        http_conn = self._http_conn
        self.log.debug("hsdswriter - open http conn")
        http_conn.open()

        hsds_info = self._http_conn.serverInfo()
        self.log.debug(f"got hsds info: {hsds_info}")
        for k in hsds_info:
            self._stats[k] = hsds_info[k]

        # fetch the domain json

        # try to do a GET from the domain
        req = "/"
        params = {}
        """
        if max_objects is None or max_objects > 0:
            # get object meta objects
            # TBD: have hsds support a max limit of objects to return
            params["getobjs"] = 1
            params["include_attrs"] = 1
            params["include_links"] = 1
        """

        create_domain = True
        rsp = http_conn.GET(req, params=params)
        self.log.debug(f"hsdswriter initial get status_code: {rsp.status_code}")

        if rsp.status_code not in (200, 404, 410):
            msg = f"Got status code: {rsp.status_code} on initial domain get"
            self.log.warning(msg)
            raise IOError(msg)

        if rsp.status_code == 200:
            if self._append:
                # domain exists already
                domain_json = rsp.json()
                if "root" not in domain_json:
                    # this a folder not a domain
                    self.log.warning(f"folder: {self.filepath} has no root property")
                    http_conn.close()
                    raise IOError(404, "Location is a folder, not a file")
                # for append, verify we have 'update' permission on the domain
                # try doing a PUT on the domain
                self.log.debug("hsds_writer> verify append permissions by POST group")
                params = {"flush": 1}
                post_rsp = self.http_conn.PUT("/", params=params)
                if post_rsp.status_code in (200, 204):
                    self.log.debug("append is ok")
                else:
                    msg = "no append permision on domain"
                    self.log.warning(msg)
                    raise IOError(post_rsp.status_code, msg)
                create_domain = False
            else:
                # not append - delete existing domain
                self.log.info("hsds_writer - delete domain")
                self.log.info(f"sending delete request for {self.filepath}")
                delete_rsp = http_conn.DELETE(req, params=params)
                if delete_rsp.status_code not in (200, 410):
                    # failed to delete
                    http_conn.close()
                    raise IOError(delete_rsp.status_code, rsp.reason)

        if create_domain:
            # domain doesn't exist, create it
            self.log.debug("hsds_writer create domain")
            body = {}
            if self.db.root_id:
                # initialize domain using the db's root_id
                body["root_id"] = self.db.root_id
            if self._owner:
                body["owner"] = self._owner
            if self._linked_domain:
                body["linked_domain"] = self._linked_domain
            if self._track_order is not None:
                create_order = 1 if self._track_order else 0
                create_props = {"CreateOrder": create_order}
                group_body = {"creationProperties": create_props}
                body["group"] = group_body
            rsp = http_conn.PUT(req, params=params, body=body)
            if rsp.status_code != 201:
                http_conn.close()
                raise IOError(rsp.status_code, rsp.reason)
            domain_json = rsp.json()
            self.log.info(f"got rsp on PUT domain: {domain_json}")
            if "root" not in domain_json:
                http_conn.close()
                raise IOError(404, "Unexpected error")

        self.log.debug(f"got domain_json: {domain_json}")

        if "root" not in domain_json:
            http_conn.close()
            raise IOError(404, "Location is a folder, not a file")

        # update stats
        for key in ("created", "lastModified", "owner", "limits", "version", "compressors"):
            if key in domain_json:
                self._stats[key] = domain_json[key]

        root_id = domain_json["root"]
        self.log.debug(f"hsds_writer got root_id: {root_id}")

        self._root_id = root_id

        # update stats
        for key in ("created", "lastModified", "owner", "limits", "version", "compressors"):
            if key in domain_json:
                self._stats[key] = domain_json[key]

        return self._root_id

    @property
    def http_conn(self):
        return self._http_conn

    def getDatasetSize(self, dset_id):
        """ Return the size of the given dataset """

        dset_json = self.db.getObjectById(dset_id)
        num_elements = getNumElements(dset_json)
        dtype = self.db.getDtype(dset_json)
        if isVlen(dtype):
            item_size = 1024  # random guess at size of variable length types
        else:
            item_size = dtype.itemsize
        return num_elements * item_size

    def createObjects(self, obj_ids):
        """ create the objects referenced in obj_ids """

        MAX_INIT_SIZE = 4096  # max size to include init values in dataset creation

        def multiPost(items):
            self.log.debug(f"hsds_writer> POST request {collection} for {len(items)} objects")
            for item in items:
                self.log.debug(f"hsds_writer> POST item: {item}")
            post_rsp = self.http_conn.POST("/" + collection, items)
            self.log.debug(f"hsds_writer> POST post_rsp.status_code: {post_rsp.status_code}")
            if post_rsp.status_code not in (200, 201):
                msg = f"createObjects POST to {collection} failed with status: {post_rsp.status_code}"
                self.log.error(msg)
                raise IOError(msg)
            items.clear()

        self.log.debug(f"hsds_writer> createObjects, {len(obj_ids)} objects")
        MAX_OBJECTS_PER_REQUEST = 300
        collections = ("groups", "datasets", "datatypes")
        col_items = {}
        dset_value_update_ids = set()
        for collection in collections:
            col_items[collection] = []

        for obj_id in obj_ids:
            if obj_id == self._root_id:
                continue  # this was created when the domain was
            collection = getCollectionForId(obj_id)
            obj_json = self.db.getObjectById(obj_id)
            item = {"id": obj_id}
            self.log.debug(f"create id: {obj_id}")
            for key in obj_json:  # ("links", "attributes"):
                if key == "updates":
                    # not part of the obj json
                    continue
                if key == "attributes":
                    # will update attribute later
                    continue
                if key == "links":
                    # links will also be updated later
                    continue
                if key == "shape":
                    # just send the dims, not the shape json
                    shape_json = obj_json["shape"]
                    if shape_json["class"] == "H5S_SIMPLE":
                        dims = shape_json["dims"]
                        item[key] = dims
                    if "maxdims" in shape_json:
                        maxdims = shape_json["maxdims"]
                        item["maxdims"] = maxdims
                else:
                    # just copy the key value directly
                    item[key] = obj_json[key]

            # initialize dataset values if provided and not too large
            if collection == "datasets":
                dset_dims = getShapeDims(obj_json)  # will be None for null space datasets
                dset_size = self.getDatasetSize(obj_id)  # number of bytes defined by the shape
                init_arr = None  # data to be passed to post create method
                updates = obj_json.get("updates")
                if updates and len(updates) == 1 and dset_size < MAX_INIT_SIZE:
                    sel, arr = updates[0]
                    if sel.select_type == selections.H5S_SELECT_ALL:
                        init_arr = arr
                        updates.clear()  # reset the update list
                if self._init and init_arr is None and dset_dims is not None:
                    # get all values from dataset if small enough
                    if dset_size < MAX_INIT_SIZE:
                        sel_all = selections.select(dset_dims, ...)
                        init_arr = self.db.getDatasetValues(obj_id, sel_all)
                if init_arr is not None:
                    value = bytesArrayToList(init_arr)
                    item["value"] = value
                elif updates or self._init:
                    dset_value_update_ids.add(obj_id)  # will set dataset value below

            # add to the list of new items for the given collection
            items = col_items[collection]
            items.append(item)

            if len(items) == MAX_OBJECTS_PER_REQUEST:
                multiPost(items)

        # handle any remainder items
        for collection in collections:
            items = col_items[collection]
            if items:
                multiPost(items)

        # write any initial dataset values
        if dset_value_update_ids:
            self.updateValues(dset_value_update_ids)

    def deleteObjects(self, obj_ids):
        """ remove the given obj ids from the HSDS store """

        # no multi-delete operation yet, so delete one by one
        for obj_id in obj_ids:
            collection = getCollectionForId(obj_id)
            req = f"/{collection}/{obj_id}"
            http_rsp = self.http_conn.DELETE(req)
            if http_rsp.status_code not in (200, 410):
                self.log.error(f"got {http_rsp.status_code} for DELETE {req}")

    def resizeDatasets(self, dset_ids):
        self.log.debug("hsds_writer> resizeDatasets")

        # HSDS doesn't yet support multi-object resize so send put request one by one

        for dset_id in dset_ids:
            dset_json = self.db.getObjectById(dset_id)
            shape_dims = getShapeDims(dset_json)
            body = {"shape": shape_dims}
            req = f"/datasets/{dset_id}/shape"
            put_rsp = self.http_conn.PUT(req, body=body)
            if put_rsp.status_code not in (200, 201):
                msg = f"update shape for {dset_id} to {shape_dims} "
                msg += f"failed with status code: {put_rsp.status_code}"
                self.log.error(msg)
                raise IOError(msg)

    def updateLinks(self, grp_ids):
        """ update any modified links of the given objects """

        self.log.debug("hsds_writer> updateLinks")
        items = {}  # dict which will hold a map of grp ids to links to create
        removals = {}  # map of grp_ids to link titles to be deleted
        count = 0

        for grp_id in grp_ids:
            if getCollectionForId(grp_id) != "groups":
                continue  # ignore datasets and datatypes
            grp_json = self.db.getObjectById(grp_id)
            grp_links = grp_json["links"]
            link_titles = list(grp_links.keys())
            for link_title in link_titles:
                link_json = grp_links[link_title]
                if "created" not in link_json:
                    self.log.error(f"hsds_writer> expected created timestamp in link: {link_json}")
                created = link_json["created"]
                if "DELETED" in link_json:
                    if created > self._last_flush_time:
                        # link hasn't been created yet
                        msg = f"hsds_writer> {grp_id}: link: {link_title} deleted before flush"
                        self.log.debug(msg)
                    else:
                        # link has been persisted, remove
                        if grp_id not in removals:
                            removals[grp_id] = set()
                        removals[grp_id].add(link_title)
                elif created > self._last_flush_time:
                    self.log.debug(f"hsds_writer> {grp_id}: new link: {link_title}")
                    count += 1
                    # new link, add to our list
                    if grp_id not in items:
                        items[grp_id] = {"links": {}}
                    links = items[grp_id]["links"]
                    link_class = link_json["class"]
                    new_link = {"class": link_class, "created": created}
                    # convert to hsds representation
                    if link_class == "H5L_TYPE_HARD":
                        new_link["id"] = link_json["id"]
                    elif link_class == "H5L_TYPE_SOFT":
                        new_link["h5path"] = link_json["h5path"]
                    elif link_class == "H5L_TYPE_EXTERNAL":
                        new_link["h5path"] = link_json["h5path"]
                        new_link["h5domain"] = link_json["file"]  # use h5domain for file key
                    elif link_class == "H5L_TYPE_USER_DEFINED":
                        self.log.warning(f"ignoring user-defined link: {link_title}")
                        continue
                    else:
                        raise IOError(f"unexpected link class: {link_class}")
                    links[link_title] = new_link
                    self.log.debug(f"setting link {link_title} to {new_link}")
                else:
                    self.log.debug(f"link {link_title} has already been persisted")

        if removals:
            # TBD: hsds doesn't have a multiple object link deletion operation yet
            # so make one request per object id
            for grp_id in removals:
                titles = removals[grp_id]
                params = {"titles": "/".join(titles)}
                del_rsp = self.http_conn.DELETE("/groups/" + grp_id + links, params=params)
                if del_rsp.status_code != 200:
                    self.log.error("failed to delete links for grp: {grp_id} titles: {titles}")
                    raise IOError("hsds_writer failed to delete links")
                else:
                    self.log.debug(f"hsds_writer> {grp_id} deleted {len(titles)} links")
                    self._lastModified = time.time()
                    # remove links from link_json in db
                    grp_json = self.db.getObjectById(grp_id)
                    grp_links = grp_json["links"]
                    for title in titles:
                        del grp_links[title]

        if items:
            body = {"grp_ids": items}
            put_rsp = self.http_conn.PUT("/groups/" + self._root_id + "/links", body=body)
            if put_rsp.status_code not in (200, 201):
                self.log.error(f"failed to update links for request: {body}")
                raise IOError("hsds_writer unable to update links")
            else:
                self.log.debug(f"hsds_writer> {grp_id} {count} links updated")
                self._lastModified = time.time()

    def _deleteAttribute(self, obj_id, attr_name):
        # delete the given attribute

        col_name = getCollectionForId(obj_id)
        req = f"/{col_name}/{obj_id}/attributes/{attr_name}"
        http_rsp = self.http_conn.DELETE(req)
        if http_rsp.status_code != 200:
            self.log.error("failed to delete attribute for obj: {obj_id} name: {attr_name}")
            raise IOError("hsds_writer failed to delete attribute")

    def updateAttributes(self, obj_ids):
        """ update any modified attributes of the given objects """

        self.log.debug("hsds_writer> updateAttributes")
        items = {}  # dict which will hold a map of objects ids to attributes to create
        removals = {}  # map of obj_ids to attributes to be deleted
        separator = '|'  # use this character to join attribute names for deletion

        count = 0

        for obj_id in obj_ids:
            obj_json = self.db.getObjectById(obj_id)
            obj_attrs = obj_json["attributes"]
            for attr_name in obj_attrs:
                attr_json = obj_attrs[attr_name]

                if "created" not in attr_json:
                    msg = f"expected created timestamp in attr: {attr_json}"
                    self.log.error(f"hsds_writer> {msg}")
                    raise IOError(msg)
                created = attr_json["created"]
                if "DELETED" in attr_json:
                    if created > self._last_flush_time:
                        # attribute hasn't been created yet
                        msg = f"hsds_writer> {obj_id}: attr: {attr_name} deleted before flush"
                        self.log.debug(msg)
                    else:
                        # attribute has been persisted, remove
                        if attr_name.find(separator) != -1:
                            # need to delete individually
                            self._deleteAttribute(obj_id, attr_name)
                        else:
                            # can delete in a batch
                            if obj_id not in removals:
                                removals[obj_id] = set()
                            removals[obj_id].add(attr_name)
                elif created > self._last_flush_time:
                    self.log.debug(f"hsds_writer> {obj_id} attribute {attr_name} created")
                    count += 1
                    # new attribute, add to our list
                    if obj_id not in items:
                        items[obj_id] = {"attributes": {}}
                    attrs = items[obj_id]["attributes"]
                    attrs[attr_name] = attr_json
                else:
                    self.log.debug(f"hsds_writer> {obj_id}: attr: {attr_name} has already been deleted")

        if removals:
            # TBD: hsds doesn't have a multiple object attribute deletion operation yet
            # so make one request per object id
            # Delete with custom separator

            for obj_id in removals:
                attr_names = removals[obj_id]
                params = {"attr_names": separator.join(attr_names)}
                params["separator"] = separator
                collection = getCollectionForId(obj_id)
                req = f"/{collection}/{obj_id}/attributes"
                rsp = self.http_conn.DELETE(req, params=params)
                if rsp.status_code != 200:
                    self.log.error("failed to delete attribute for obj: {obj_id}")
                    raise IOError("hsds_writer failed to delete attributes")

        if items:
            body = {"obj_ids": items}
            req = f"/groups/{self._root_id}/attributes"
            put_rsp = self.http_conn.PUT(req, body=body)
            if put_rsp.status_code not in (200, 201):
                msg = f"put {req} failed, status: {put_rsp.status_code}"
                self.log.error(f"hsds_writer> {msg}")
                raise IOError(msg)
            else:
                self.log.debug(f"hsds_writer> {count} attributes updated")
                self._lastModified = time.time()

    def updateValue(self, dset_id, sel, arr):
        """ update the given dataset using selection and array """
        self.log.debug("hsds_writer> updateValue")
        params = {}
        data = arrayToBytes(arr)
        self.log.debug(f"writing binary data, {len(data)} bytes")

        if sel.select_type != selections.H5S_SELECT_ALL:
            select_param = sel.getQueryParam()
            self.log.debug(f"got select query param: {select_param}")
            params["select"] = select_param

        req = f"/datasets/{dset_id}/value"
        rsp = self.http_conn.PUT(req, body=data, params=params, format="binary")
        if rsp.status_code != 200:
            self.log.error(f"PUT {req} returned error: {rsp.status_code}")
        else:
            self.log.debug(f"PUT {len(data)} bytes successful")
            self._lastModified = time.time()

    def updateValues(self, dset_ids):
        """ write any pending dataset values """

        self.log.debug("hsds_writer> updateValues")
        for dset_id in dset_ids:
            if getCollectionForId(dset_id) != "datasets":
                continue  # ignore groups and datatypes
            dset_json = self.db.getObjectById(dset_id)
            dset_dims = getShapeDims(dset_json)
            if dset_dims is None:
                # no data to update
                continue
            if self._init:
                # get all data for the dataset
                # TBD: do this by chunks
                sel_all = selections.select(dset_dims, ...)
                arr = self.db.getDatasetValues(dset_id, sel_all)
                if arr is not None:
                    self.updateValue(dset_id, sel_all, arr)
            else:
                # TBD: use multi if there are multiple updates
                updates = self.db._getDatasetUpdates(dset_id)

                for (sel, val) in updates:
                    for (sel, arr) in updates:
                        self.updateValue(dset_id, sel, arr)

    def putACL(self, acl):
        """ create an ACL for the domain """

        if self.closed:
            # no db set yet
            self.log.warning("hsds_writer> putACL called but no db")
            return IOError("writer is closed")
        if not self._http_conn:
            self.log.warning("hsds_writer no http connection")
            raise IOError("no http connection")

        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            if k not in acl:
                raise IOError(404, "Missing ACL field: {}".format(k))
            perm[k] = acl[k]

        req = "/acls/" + acl["userName"]
        rsp = self.http_conn.PUT(req, body=perm)
        if rsp.status_code not in (200, 201):
            self.log.warning(f"PUT ACL failed with status code: {rsp.status_code}")
            raise IOError(rsp.status_code, "Error setting ACL")

    def flush(self):
        """ Write dirty items """
        if self.closed:
            # no db set yet
            self.log.warning("hsds_writer> flush called but no db")
            return IOError("writer is closed")
        if not self._http_conn:
            self.log.warning("hsds_writer no http connection")
            raise IOError("no http connection")
        self.log.info("hsds_writer.flush()")
        self.log.debug(f"    new object count: {len(self.db.new_objects)}")
        self.log.debug(f"    dirty object count: {len(self.db.dirty_objects)}")
        self.log.debug(f"    deleted object count: {len(self.db.deleted_objects)}")
        root_id = self._root_id
        dirty_ids = self.db.dirty_objects.copy()
        resized_dset_ids = self.db.resized_datasets.copy()
        if self._init:
            # initialize objects
            self.log.debug(f"hsds_writer> flush -- init is True self.db: {len(self.db.db)} objects")
            self.db.readAll()
            self.log.debug(f"hsds_writer> flush, init after readAll, {len(self.db.db)} objects")
            obj_ids = set(self.db.db.keys())
            obj_ids.remove(root_id)  # root group created when domain was
            self.log.debug(f"init createObjects: {obj_ids}")
            self.createObjects(obj_ids)
            dirty_ids.update(obj_ids)
            dirty_ids.add(root_id)  # add back root for attribute and link creation
            if not self._no_data:
                # initialize dataset values
                pass
                # self.updateValues(obj_ids)
            self._init = False
        elif self.db.new_objects:
            self.log.debug(f"hsds_writer> {len(self.db.new_objects)} objects to create")
            for obj_id in self.db.new_objects:
                self.log.debug(f"hsds_writer> new obj id: {obj_id}")
            self.createObjects(self.db.new_objects)
            dirty_ids.update(self.db.new_objects)
        else:
            self.log.debug("no new objects to persist")

        if resized_dset_ids:
            self.log.debug(f"hsds_writer> resized ids: {resized_dset_ids}")
            self.resizeDatasets(resized_dset_ids)

        if dirty_ids:
            self.log.debug(f"hsds_writer> dirty ids: {dirty_ids}")
            self.updateLinks(dirty_ids)
            self.updateAttributes(dirty_ids)
            if not self._no_data:
                self.updateValues(dirty_ids)

        if self.db.deleted_objects:
            self.log.debug(f"deleted ids: {self.db.deleted_objects}")
            self.deleteObjects(self.db.deleted_objects)

        self._last_flush_time = time.time()
        self.log.debug("hsds_writer> flush successful")
        # all objects written successfully
        return True

    def close(self):
        # over-ride of H5Writer method
        self.flush()

    def isClosed(self):
        """ return closed status """
        return False if self._http_conn else True

    def get_root_id(self):
        """ Return root id """
        return self._root_id

    def getStats(self):
        """ return a dictionary object with at minimum the following keys:
            'created': creation time
            'lastModified': modificationTime
            'owner': owner name
        """
        return self._stats

    def getFilters(self, compressors_only=False):
        """ return list of filters supported by the server """

        h5py_filters = ["H5Z_FILTER_DEFLATE",
                        "H5Z_FILTER_LZF",
                        "H5Z_FILTER_BLOSC",
                        "H5Z_FILTER_LZ4",
                        "H5Z_FILTER_LZ4HC"]

        if not compressors_only:
            h5py_filters.append("H5Z_FILTER_SHUFFLE")
            h5py_filters.append("H5Z_FILTER_BITSHUFFLE")
            h5py_filters.append("H5Z_FILTER_FLETCHER32")
            h5py_filters.append("H5Z_FILTER_SZIP")
            h5py_filters.append("H5Z_FILTER_NBIT")
            h5py_filters.append("H5Z_FILTER_SCALEOFFSET")

        # TBD: add blosc, etc.

        return tuple(h5py_filters)
