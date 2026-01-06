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

from h5json.objid import getCollectionForId, getUuidFromId

from h5json.hdf5dtype import createDataType
from h5json.array_util import jsonToArray, bytesToArray
from h5json import selections
from h5json.h5reader import H5Reader
from .httpconn import HttpConn


class HSDSReader(H5Reader):
    """
    This class can be used by HDF5DB to read content from an hdf5-json file
    """

    def __init__(
        self,
        domain_path,
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
    ):
        if app_logger:
            self.log = app_logger
        else:
            self.log = logging.getLogger()

        self.log.debug("HSDSReader init(")

        kwargs = {}
        self.log.debug(f"    domain_path: {domain_path}")
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
        # save these for when we create the connection
        if swmr:
            self.log.warning("no cache feature is not yet supported")
        self._swmr = swmr
        self._http_kwargs = kwargs
        self._http_conn = None
        self._stats = {"created": 0, "lastModified": 0, "owner": ""}

        super().__init__(domain_path, app_logger=app_logger)

    def open(self):
        if self._http_conn and not self._http_conn.isClosed():
            return self._root_id  # open already called

        if self._http_conn:
            http_conn = self._http_conn
        else:
            kwargs = self._http_kwargs
            http_conn = HttpConn(self.filepath, **kwargs)

        http_conn.open()

        hsds_info = http_conn.serverInfo()
        self.log.debug(f"got hsds info: {hsds_info}")

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

        rsp = http_conn.GET(req, params=params)

        if rsp.status_code != 200:
            # file must exist
            http_conn.close()
            raise IOError(rsp.status_code, rsp.reason)

        domain_json = rsp.json()
        self.log.debug(f"got domain_json: {domain_json}")

        # update stats
        for key in ("created", "lastModified", "owner", "limits", "version", "compressors"):
            if key in domain_json:
                self._stats[key] = domain_json[key]

        if "root" not in domain_json:
            http_conn.close()
            raise IOError(404, "Location is a folder, not a file")

        root_id = domain_json["root"]
        self._root_id = root_id

        """
        if "domain_objs" in root_json:
            domain_objs = root_json["domain_objs"]
            objdb.load(domain_objs)
        """

        self._http_conn = http_conn

        return self._root_id

    @property
    def http_conn(self):
        return self._http_conn

    def close(self):
        if self._http_conn:
            self._http_conn.close()

    def isClosed(self):
        if not self._http_conn:
            return True
        else:
            return self._http_conn.isClosed()

    def get_root_id(self):
        """ Return root id """
        return self._root_id

    def getObjectById(self, obj_id, include_attrs=True, include_links=True, include_values=False):
        """ return object with given id """

        collection = getCollectionForId(obj_id)

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

    def getDtype(self, obj_json):
        """ Return the dtype for the type given by obj_json """
        if "type" not in obj_json:
            raise KeyError("no type item found")
        type_item = obj_json["type"]
        if isinstance(type_item, str) and type_item.startswith("datatypes/"):
            # this is a reference to a committed type
            ctype_id = "t-" + getUuidFromId(type_item)
            ctype_json = self.getObjectById(ctype_id)
            if "type" not in ctype_json:
                raise KeyError(f"Unexpected datatype: {ctype_json}")
            # Use the ctype's item json
            type_item = ctype_json["type"]
        dtype = createDataType(type_item)
        return dtype

    def getDatasetValues(self, dset_id, sel=None, dtype=None):
        """
        Get values from dataset identified by obj_id.
        If a slices list or tuple is provided, it should have the same
        number of elements as the rank of the dataset.
        """

        self.log.debug(f"getDatasetValues({dset_id}), sel={sel}")
        collection = getCollectionForId(dset_id)
        if collection != "datasets":
            msg = f"unexpected id: {dset_id} for getDatasetValues"
            self.log.warning(msg)
            return ValueError(msg)

        if sel is None or sel.select_type == selections.H5S_SELECT_ALL:
            query_param = None  # just return the entire array
        elif isinstance(sel, (selections.SimpleSelection, selections.FancySelection)):
            query_param = sel.getQueryParam()
        else:
            raise NotImplementedError(f"selection type: {type(sel)} not supported")

        mtype = dtype  # TBD - support read time dtype
        mshape = sel.mshape

        req = f"/{collection}/{dset_id}/value"
        params = {}

        if query_param:
            params["select"] = query_param

        if mtype.names != dtype.names:
            params["fields"] = ":".join(mtype.names)

        MAX_SELECT_QUERY_LEN = 100
        if query_param and len(query_param) > MAX_SELECT_QUERY_LEN:
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
            arr = bytesToArray(rsp.text, mtype, mshape)
        else:
            # got JSON response
            # need some special conversion for compound types --
            # each element must be a tuple, but the JSON decoder
            # gives us a list instead.
            self.log.info("json response")

            data = rsp.json()["value"]
            # self.log.debug(data)

            arr = jsonToArray(mshape, mtype, data)
            self.log.debug(f"jsonToArray returned: {arr}")

        return arr

    def getStats(self):
        """ return a dictionary object with at minimum the following keys:
            'created': creation time
            'lastModified': modificationTime
            'owner': owner name
        """
        return self._stats
