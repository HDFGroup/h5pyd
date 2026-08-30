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
import time
import base64
import numpy as np

from h5json.objid import getCollectionForId, isValidUuid
from h5json.hdf5dtype import isVlen
from h5json.array_util import jsonToArray, bytesToArray, arrayToBytes, bytesArrayToList
from h5json.shape_util import getShapeDims, getNumElements
from h5json import selections
from h5json.storage_plugin import StoragePlugin

from .httpconn import HttpConn
from ._hl.acl_manager import ACLManager


class HsdsPlugin(StoragePlugin):
    """
    This class reads from and writes to an HSDS domain over the HDF REST API.  A single
    instance holds a single HttpConn connection used for both operations, so a read always
    reflects whatever this same instance has most recently flushed.
    """

    def __init__(
        self,
        domain_path,
        append=False,
        no_data=False,
        read_only=False,
        app_logger=None,
        endpoint=None,
        username=None,
        password=None,
        bucket=None,
        api_key=None,
        use_session=True,
        swmr=False,
        getobjs=False,
        expire_time=0,
        max_objects=0,
        max_age=0,
        retries=3,
        timeout=30.0,
        track_order=None,
        owner=None,
        linked_domain=None,
        **kwargs,
    ):
        super().__init__(domain_path, append=append, no_data=no_data, read_only=read_only, app_logger=app_logger)

        self.log.debug("HsdsPlugin init()")

        http_kwargs = {}
        self.log.debug(f"    domain_path: {domain_path}")
        self.log.debug(f"    append: {append}")
        self.log.debug(f"    read_only: {read_only}")
        if endpoint:
            self.log.debug(f"    endpoint: {endpoint}")
            http_kwargs["endpoint"] = endpoint
        if username:
            self.log.debug(f"    username: {username}")
            http_kwargs["username"] = username
        if password:
            self.log.debug(f"    password: {'*' * len(password)}")
            http_kwargs["password"] = password
        if bucket:
            self.log.debug(f"    bucket: {bucket}")
            http_kwargs["bucket"] = bucket
        if api_key:
            self.log.debug(f"    apI_key: {'*' * len(api_key)}")
            http_kwargs["api_key"] = api_key
        if use_session:
            self.log.debug(f"    use_session: {use_session}")
            http_kwargs["user_session"] = use_session
        if expire_time:
            self.log.debug(f"    expire_time: {expire_time}")
            http_kwargs["expire_time"] = expire_time
        if max_objects:
            self.log.debug(f"    max_objects: {max_objects}")
            http_kwargs["max_objects"] = max_objects
        if max_age:
            self.log.debug(f"    max_age: {max_age}")
            http_kwargs["max_age"] = max_age
        if retries:
            self.log.debug(f"    retries: {retries}")
            http_kwargs["retries"] = retries
        if timeout:
            self.log.debug(f"    timeout: {timeout}")
            http_kwargs["timeout"] = timeout
        if swmr:
            self.log.warning("swmr/no cache feature is not yet supported")

        self._swmr = swmr
        self._getobjs = getobjs  # get consolidated metadata if true
        self._domain_objs = {}   # consolidated metadata objects
        self._http_kwargs = http_kwargs
        self._http_conn = None
        self._track_order = track_order
        self._owner = owner
        self._linked_domain = linked_domain
        self._root_id = None
        self._last_flush_time = 0
        # True until the first flush() completes - matches H5pyPlugin's convention: a
        # read_only or append plugin never needs to force a full initial write
        self._init = False if (append or read_only) else True
        self._stats = {"created": 0, "lastModified": 0, "owner": ""}
        self._acl_mgr = ACLManager(self._get_acl_http_conn, log=self.log)

    def _get_acl_http_conn(self):
        """ return the live http connection for ACL requests, raising if not open """
        if self.closed:
            self.log.warning("hsds_plugin no http connection")
            raise IOError("plugin is closed")
        return self.http_conn

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def open(self):
        """ open connection to the HSDS domain, verifying or creating it as needed """
        if self._http_conn and not self._http_conn.isClosed():
            return self._root_id  # already open

        if self.db is None:
            self.log.warning("no self.db db_ref")
            raise ValueError("no db")

        if self._http_conn:
            http_conn = self._http_conn
        else:
            kwargs = dict(self._http_kwargs)
            if self.read_only:
                kwargs["mode"] = "r"
            else:
                kwargs["mode"] = "a"
                kwargs["retries"] = 1  # tbd: test setting
            http_conn = HttpConn(self.filepath, **kwargs)

        self.log.debug("hsds_plugin - open http conn")
        http_conn.open()

        hsds_info = http_conn.serverInfo()
        self.log.debug(f"got hsds info: {hsds_info}")
        for k in hsds_info:
            self._stats[k] = hsds_info[k]

        req = "/"
        params = {}
        if self._getobjs:
            params["getobjs"] = 1

        if self.read_only:
            rsp = http_conn.GET(req, params=params)
            if rsp.status_code != 200:
                # file must exist
                http_conn.close()
                if rsp.status_code in (404, 410):
                    # domain doesn't exist - use FileNotFoundError for
                    # consistency with how h5py handles this case
                    raise FileNotFoundError(rsp.status_code, rsp.reason)
                else:
                    raise IOError(rsp.status_code, rsp.reason)
            domain_json = rsp.json()
        else:
            rsp = http_conn.GET(req, params=params)
            self.log.debug(f"hsds_plugin initial get status_code: {rsp.status_code}")

            if rsp.status_code not in (200, 404, 410):
                msg = f"Got status code: {rsp.status_code} on initial domain get"
                self.log.warning(msg)
                raise IOError(msg)

            create_domain = True

            if rsp.status_code == 200:
                if self.append:
                    # domain exists already
                    domain_json = rsp.json()
                    if "root" not in domain_json:
                        # this a folder not a domain
                        self.log.warning(f"folder: {self.filepath} has no root property")
                        http_conn.close()
                        raise IOError(404, "Location is a folder, not a file")
                    # verify we have 'update' permission on the domain by doing a PUT
                    self.log.debug("hsds_plugin> verify append permissions by PUT flush")
                    verify_params = {"flush": 1}
                    put_rsp = http_conn.PUT("/", params=verify_params)
                    if put_rsp.status_code in (200, 204):
                        self.log.debug("append is ok")
                    else:
                        msg = "no append permission on domain"
                        self.log.warning(msg)
                        raise IOError(put_rsp.status_code, msg)
                    create_domain = False
                else:
                    # not append - delete existing domain
                    self.log.info(f"hsds_plugin - delete domain, sending delete request for {self.filepath}")
                    delete_rsp = http_conn.DELETE(req, params=params)
                    if delete_rsp.status_code not in (200, 410):
                        # failed to delete
                        http_conn.close()
                        raise IOError(delete_rsp.status_code, rsp.reason)

            if create_domain:
                # domain doesn't exist (or was just deleted above), create it
                self.log.debug("hsds_plugin create domain")
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
                    body["group"] = {"creationProperties": create_props}
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
        self._root_id = root_id

        if "domain_objs" in domain_json:
            domain_objs = domain_json["domain_objs"]
            if not isinstance(domain_objs, dict):
                raise TypeError("Unexpected type")
            for obj_id in domain_objs:
                if not isValidUuid(obj_id):
                    self.log.warning(f"HsdsPlugin domain_objs - unexpected id: {obj_id}")
                    continue
                if obj_id in self.db.db:
                    continue  # already loaded
                if obj_id in self._domain_objs:
                    continue  # already in the prefetch cache
                self._domain_objs[obj_id] = domain_objs[obj_id]

        self._http_conn = http_conn

        return self._root_id

    @property
    def http_conn(self):
        return self._http_conn

    def close(self):
        """ close storage handle.

        Doesn't flush - Hdf5db.close() (the only caller) always calls Hdf5db.flush()
        immediately beforehand, which itself calls this plugin's flush(); re-flushing
        here would be redundant. """
        if self._http_conn:
            self._http_conn.close()

    def isClosed(self):
        """ return closed status """
        if not self._http_conn:
            return True
        else:
            return self._http_conn.isClosed()

    def get_root_id(self):
        """ Return root id """
        return self._root_id

    # ------------------------------------------------------------------
    # read-side object/attribute/dataset retrieval
    # ------------------------------------------------------------------

    def getObjectById(self, obj_id, include_attrs=True, include_links=True):
        """ return object with given id """

        collection = getCollectionForId(obj_id)
        if obj_id in self._domain_objs:
            # this was included in the consolidated metadata returned
            # with the domain request
            obj_json = self._domain_objs[obj_id]
            # TBD - need to add a invalidate cache method to remove this from the cache
            #  when the object is modified
        else:
            # fetch from the server
            req = f"/{collection}/{obj_id}"
            self.log.debug(f"sending req: {req}")

            params = {}
            if include_attrs:
                params["include_attrs"] = 1
            if include_links:
                params["include_links"] = 1

            rsp = self.http_conn.GET(req, params=params)

            if rsp.status_code != 200:
                raise IOError(rsp.status_code, rsp.reason)

            obj_json = rsp.json()
            for k in ("id", "root", "linkCount", "attributeCount", "domain", "hrefs"):
                if k in obj_json:
                    del obj_json[k]  # don't need these

        # remove any unneeded keys
        redundant_keys = ("hrefs", "root", "domain", "bucket", "linkCount", "attributeCount")
        for key in redundant_keys:
            if key in obj_json:
                del obj_json[key]

        self.log.debug(f"got json for id: {obj_id}: {obj_json}")
        return obj_json

    def getAttribute(self, obj_id, name, includeData=True):
        """
        Get attribute given an object id and name
        returns: JSON object
        """
        self.log.debug(f"getAttribute({obj_id}), [{name}], include_data={includeData})")
        collection = getCollectionForId(obj_id)
        req = f"/{collection}/{obj_id}/attributes/{name}"

        params = {}
        params["IncludeData"] = 1 if includeData else 0

        rsp = self.http_conn.GET(req, params=params)

        if rsp.status_code in (404, 410):
            self.log.warning(f"attribute {name} not found")
            return None

        if rsp.status_code != 200:
            self.log.error(f"GET {req} failed with status_code: {rsp.status_code}")
            raise IOError(rsp.status_code, rsp.reason)
        attr_json = rsp.json()

        if "hrefs" in attr_json:
            del attr_json["hrefs"]

        return attr_json

    def getDatasetValues(self, obj_id, sel=None, dtype=None, query=None):
        """
        Get values from dataset identified by obj_id.
        If a slices list or tuple is provided, it should have the same
        number of elements as the rank of the dataset.
        If query is provided, it should be a string with a query expression.
        """

        self.log.debug(f"getDatasetValues({obj_id}), sel={sel}")
        collection = getCollectionForId(obj_id)
        if collection != "datasets":
            msg = f"unexpected id: {obj_id} for getDatasetValues"
            self.log.warning(msg)
            return ValueError(msg)
        dset_id = obj_id

        if sel is None or sel.select_type == selections.H5S_SEL_ALL or sel.shape == sel.mshape:
            query_param = None  # just return the entire array
        elif sel.select_type == selections.H5S_SEL_POINTS:
            query_param = None  # sent via POST body below, not a query param
        elif isinstance(sel, selections.SimpleSelection):
            query_param = sel.query_string
        else:
            raise NotImplementedError(f"selection type: {type(sel)} not supported")

        mtype = dtype  # TBD - support read time dtype
        mshape = sel.mshape
        arr = None
        rank = len(sel.shape)

        # check to see if we have the dataset value cached in the domain_objs
        if dset_id in self._domain_objs and not query:
            # this was included in the consolidated metadata returned
            # with the domain request
            self.log.debug(f"dataset {dset_id} value found in domain_objs cache")
            dset_json = self._domain_objs[dset_id]
            if "value" in dset_json:
                self.log.debug("dataset value found in domain_objs cache")
                dims = getShapeDims(dset_json)
                dset_arr = jsonToArray(dims, mtype, dset_json["value"])
                if sel is None or sel.select_type == selections.H5S_SEL_ALL:
                    arr = dset_arr
                else:
                    arr = dset_arr[sel.slices]

                # TBD: need to add a invalidate cache method to remove this from the cache
                #  when the dataset is modified
                return arr

        req = f"/{collection}/{dset_id}/value"
        params = {}

        if query_param:
            params["select"] = query_param

        if mtype.names != dtype.names:
            params["fields"] = ":".join(mtype.names)
        if query:
            params["query"] = query

        MAX_SELECT_QUERY_LEN = 100

        if sel.select_type == selections.H5S_SEL_POINTS:
            # Use a POST to send point selection data
            pt_arr = np.zeros((sel.nselect, rank), dtype=np.uint64)
            for i in range(sel.nselect):
                for d in range(rank):
                    s = sel.slices[d]
                    # a mixed int+list selection (e.g. ds[0, [1, 2]]) leaves a
                    # bare int (not a per-point list) for the int-indexed dim -
                    # that coordinate is the same for every point
                    pt_arr[i, d] = s[i] if isinstance(s, list) else s

            body = pt_arr.tobytes()
            try:
                rsp = self.http_conn.POST(req, body=body, format="binary")
            except IOError as ioe:
                self.log.info(f"got IOError: {ioe.errno}")
                raise IOError(ioe.errno, "Error retrieving data")
        elif query_param and len(query_param) > MAX_SELECT_QUERY_LEN:
            # use a post method to avoid possible long query strings
            try:
                rsp = self.http_conn.POST(req, body=params, format="binary")
            except IOError as ioe:
                self.log.info(f"got IOError: {ioe.errno}")
                raise IOError(f"Error retrieving data: {ioe.errno}")
        else:
            # make a http GET
            try:
                rsp = self.http_conn.GET(req, params=params, format="binary")
            except IOError as ioe:
                self.log.info(f"got IOError: {ioe.errno}")
                raise IOError(ioe.errno, "Error retrieving data")

        if rsp.status_code != 200:
            self.log.info(f"got http error: {rsp.status_code}")
            raise IOError(rsp.status_code, "Error retrieving data")

        if rsp.is_binary:
            # got binary response
            self.log.info(f"binary response, {len(rsp.text)} bytes")
            # a query response is a 1D array of matching elements whose length
            # isn't known until the data comes back, so let it infer its own shape
            arr = bytesToArray(rsp.text, mtype, None if query else mshape)
        else:
            # got JSON response
            # need some special conversion for compound types --
            # each element must be a tuple, but the JSON decoder
            # gives us a list instead.
            self.log.info("json response")

            data = rsp.json()["value"]
            # self.log.debug(data)

            arr = jsonToArray((len(data),) if query else mshape, mtype, data)
            self.log.debug(f"jsonToArray returned: {arr}")

        return arr

    def getACL(self, username):
        """ Return the ACL for the given username """
        return self._acl_mgr.getACL(username)

    def getACLs(self):
        """ Return all the ACLs for the domain"""
        return self._acl_mgr.getACLs()

    def getStats(self, verbose=False):
        """ return a dictionary object with at minimum the following keys:
            'created': creation time
            'lastModified': modificationTime
            'owner': owner name
        """

        params = {}
        if verbose:
            params["verbose"] = 1

        req = "/"

        try:
            rsp = self.http_conn.GET(req, params=params)
        except IOError as ioe:
            self.log.info(f"got IOError: {ioe.errno}")
            raise IOError(ioe.errno, "Error fetching stats")
        if rsp.status_code != 200:
            self.log.info(f"get http error on getStats: {rsp.status_code}")
            raise IOError(rsp.status_code, "Error fetching stats")

        rsp_json = rsp.json()

        for k in (
            "num_objects",
            "num_datatypes",
            "num_groups",
            "num_datasets",
            "num_chunks",
            "num_linked_chunks",
            "allocated_bytes",
            "metadata_bytes",
            "linked_bytes",
            "total_size",
            "lastModified",
            "md5_sum",
        ):
            if k in rsp_json:
                self._stats[k] = rsp_json[k]

        return self._stats

    def getFilters(self, compressors_only=False):
        """ return list of filters supported by the server """

        hsds_filters = ["H5Z_FILTER_DEFLATE",
                        "H5Z_FILTER_LZF",
                        "H5Z_FILTER_BLOSC",
                        "H5Z_FILTER_LZ4",
                        "H5Z_FILTER_LZ4HC"]

        if not compressors_only:
            hsds_filters.append("H5Z_FILTER_SHUFFLE")
            hsds_filters.append("H5Z_FILTER_BITSHUFFLE")
            hsds_filters.append("H5Z_FILTER_FLETCHER32")
            hsds_filters.append("H5Z_FILTER_SZIP")
            hsds_filters.append("H5Z_FILTER_NBIT")
            hsds_filters.append("H5Z_FILTER_SCALEOFFSET")

        return tuple(hsds_filters)

    # ------------------------------------------------------------------
    # write-side object/attribute/link/value updates
    # ------------------------------------------------------------------

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
            self.log.debug(f"hsds_plugin> POST request {collection} for {len(items)} objects")
            for item in items:
                self.log.debug(f"hsds_plugin> POST item: {item}")
            post_rsp = self.http_conn.POST("/" + collection, items)
            self.log.debug(f"hsds_plugin> POST post_rsp.status_code: {post_rsp.status_code}")
            if post_rsp.status_code not in (200, 201):
                msg = f"createObjects POST to {collection} failed with status: {post_rsp.status_code}"
                self.log.error(msg)
                raise IOError(msg)
            items.clear()

        self.log.debug(f"hsds_plugin> createObjects, {len(obj_ids)} objects")
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
            for key in obj_json:
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
                    if sel.select_type == selections.H5S_SEL_ALL or sel.shape == sel.mshape:
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
        self.log.debug("hsds_plugin> resizeDatasets")

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

        self.log.debug("hsds_plugin> updateLinks")
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
                    self.log.error(f"hsds_plugin> expected created timestamp in link: {link_json}")
                created = link_json["created"]
                if "DELETED" in link_json:
                    if created > self._last_flush_time:
                        # link hasn't been created yet
                        msg = f"hsds_plugin> {grp_id}: link: {link_title} deleted before flush"
                        self.log.debug(msg)
                    else:
                        # link has been persisted, remove
                        if grp_id not in removals:
                            removals[grp_id] = set()
                        removals[grp_id].add(link_title)
                elif created > self._last_flush_time:
                    self.log.debug(f"hsds_plugin> {grp_id}: new link: {link_title}")
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
                del_rsp = self.http_conn.DELETE("/groups/" + grp_id + "/links", params=params)
                if del_rsp.status_code != 200:
                    self.log.error(f"failed to delete links for grp: {grp_id} titles: {titles}")
                    raise IOError("hsds_plugin failed to delete links")
                else:
                    self.log.debug(f"hsds_plugin> {grp_id} deleted {len(titles)} links")
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
                raise IOError("hsds_plugin unable to update links")
            else:
                self.log.debug(f"hsds_plugin> {grp_id} {count} links updated")
                self._lastModified = time.time()

    def _deleteAttribute(self, obj_id, attr_name):
        # delete the given attribute

        col_name = getCollectionForId(obj_id)
        req = f"/{col_name}/{obj_id}/attributes/{attr_name}"
        http_rsp = self.http_conn.DELETE(req)
        if http_rsp.status_code != 200:
            self.log.error(f"failed to delete attribute for obj: {obj_id} name: {attr_name}")
            raise IOError("hsds_plugin failed to delete attribute")

    def updateAttributes(self, obj_ids):
        """ update any modified attributes of the given objects """

        self.log.debug("hsds_plugin> updateAttributes")
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
                    self.log.error(f"hsds_plugin> {msg}")
                    raise IOError(msg)
                created = attr_json["created"]
                if "DELETED" in attr_json:
                    if created > self._last_flush_time:
                        # attribute hasn't been created yet
                        msg = f"hsds_plugin> {obj_id}: attr: {attr_name} deleted before flush"
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
                    self.log.debug(f"hsds_plugin> {obj_id} attribute {attr_name} created")
                    count += 1
                    # new attribute, add to our list
                    if obj_id not in items:
                        items[obj_id] = {"attributes": {}}
                    attrs = items[obj_id]["attributes"]
                    attrs[attr_name] = attr_json
                else:
                    self.log.debug(f"hsds_plugin> {obj_id}: attr: {attr_name} has already been deleted")

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
                    self.log.error(f"failed to delete attribute for obj: {obj_id}")
                    raise IOError("hsds_plugin failed to delete attributes")

        if items:
            body = {"obj_ids": items}
            req = f"/groups/{self._root_id}/attributes"
            put_rsp = self.http_conn.PUT(req, body=body)
            if put_rsp.status_code not in (200, 201):
                msg = f"put {req} failed, status: {put_rsp.status_code}"
                self.log.error(f"hsds_plugin> {msg}")
                raise IOError(msg)
            else:
                self.log.debug(f"hsds_plugin> {count} attributes updated")
                self._lastModified = time.time()

    def updateValue(self, dset_id, sel, arr):
        """ update the given dataset using selection and array """
        self.log.debug("hsds_plugin> updateValue")
        if arr.size == 0:
            # nothing to write - and HSDS rejects an empty-body PUT with 400
            self.log.debug("hsds_plugin> updateValue - skipping empty array")
            return
        params = {}
        data = arrayToBytes(arr)
        self.log.debug(f"writing binary data, {len(data)} bytes")
        req = f"/datasets/{dset_id}/value"
        rank = len(sel.shape)

        if sel.select_type == selections.H5S_SEL_POINTS:
            # send put request with point update
            pt_arr = np.zeros((sel.nselect, rank), dtype=np.uint64)
            for i in range(sel.nselect):
                for d in range(rank):
                    s = sel.slices[d]
                    # a mixed int+list selection (e.g. ds[0, [1, 2]] = ...)
                    # leaves a bare int (not a per-point list) for the
                    # int-indexed dim - that coordinate is the same for
                    # every point
                    pt_arr[i, d] = s[i] if isinstance(s, list) else s

            points = bytesArrayToList(pt_arr)
            value_base64 = base64.b64encode(data)
            value_base64 = value_base64.decode("ascii")

            body = {"points": points, "value_base64": value_base64}
            format = "json"
        else:

            if sel.select_type != selections.H5S_SEL_ALL and sel.shape != sel.mshape:
                select_param = sel.query_string
                self.log.debug(f"got select query param: {select_param}")
                params["select"] = select_param
            body = data  # do a binary put
            format = "binary"

        if sel.fields:
            # sel.fields is a set, so its iteration order is arbitrary -
            # order the "fields" param to match how `data` was actually
            # serialized (arr.dtype.names), or the server would map the
            # raw bytes to the wrong field names for a multi-field write
            if len(arr.dtype) > 1:
                field_order = [f for f in arr.dtype.names if f in sel.fields]
            else:
                field_order = list(sel.fields)
            params["fields"] = ":".join(field_order)

        rsp = self.http_conn.PUT(req, body=body, params=params, format=format)
        if rsp.status_code != 200:
            self.log.error(f"PUT {req} returned error: {rsp.status_code}")
            raise IOError(f"PUT {req} failed with status code: {rsp.status_code}")
        else:
            self.log.debug(f"PUT {len(data)} bytes successful")
            self._lastModified = time.time()

    def updateValues(self, dset_ids):
        """ write any pending dataset values """

        self.log.debug("hsds_plugin> updateValues")
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
                updates = self.db._getDatasetUpdates(dset_id)

                for (sel, arr) in updates:
                    self.updateValue(dset_id, sel, arr)

    def putACL(self, acl):
        """ create an ACL for the domain """
        self._acl_mgr.putACL(acl)

    def flush(self):
        """ Write dirty items """
        if self.closed:
            # no db set yet
            self.log.warning("hsds_plugin - flush called but no db")
            return False
        if not self._http_conn:
            self.log.warning("hsds_plugin no http connection")
            raise IOError("open not called")

        if self.read_only:
            if self.db.new_objects or self.db.dirty_objects:
                # a read_only plugin must never write to storage, but in-memory-only
                # edits made against it are fine to just leave un-flushed
                self.log.warning("read_only plugin: not persisting pending in-memory changes")
                return False
            return True  # nothing to persist, and never anything to initialize

        self.log.info("hsds_plugin.flush()")
        self.log.debug(f"    new object count: {len(self.db.new_objects)}")
        self.log.debug(f"    dirty object count: {len(self.db.dirty_objects)}")
        self.log.debug(f"    deleted object count: {len(self.db.deleted_objects)}")
        root_id = self._root_id
        dirty_ids = self.db.dirty_objects.copy()
        resized_dset_ids = self.db.resized_datasets.copy()
        if self._init:
            # initialize objects
            self.log.debug(f"hsds_plugin> flush -- init is True self.db: {len(self.db.db)} objects")
            self.db.readAll()
            self.log.debug(f"hsds_plugin> flush, init after readAll, {len(self.db.db)} objects")
            obj_ids = set(self.db.db.keys())
            obj_ids.remove(root_id)  # root group created when domain was
            self.log.debug(f"init createObjects: {obj_ids}")
            self.createObjects(obj_ids)
            dirty_ids.update(obj_ids)
            dirty_ids.add(root_id)  # add back root for attribute and link creation
            self._init = False
        elif self.db.new_objects:
            self.log.debug(f"hsds_plugin> {len(self.db.new_objects)} objects to create")
            for obj_id in self.db.new_objects:
                self.log.debug(f"hsds_plugin> new obj id: {obj_id}")
            self.createObjects(self.db.new_objects)
            dirty_ids.update(self.db.new_objects)
        else:
            self.log.debug("no new objects to persist")

        if resized_dset_ids:
            self.log.debug(f"hsds_plugin> resized ids: {resized_dset_ids}")
            self.resizeDatasets(resized_dset_ids)

        if dirty_ids:
            self.log.debug(f"hsds_plugin> dirty ids: {dirty_ids}")
            self.updateLinks(dirty_ids)
            self.updateAttributes(dirty_ids)
            if not self.no_data:
                self.updateValues(dirty_ids)

        if self.db.deleted_objects:
            self.log.debug(f"deleted ids: {self.db.deleted_objects}")
            self.deleteObjects(self.db.deleted_objects)

        self._last_flush_time = time.time()
        self.log.debug("hsds_plugin> flush successful")
        # all objects written successfully
        return True
