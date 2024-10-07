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

import io
import os
import json
import pathlib
import time

from .objectid import GroupID
from .group import Group
from .httpconn import HttpConn
from .config import Config

VERBOSE_REFRESH_TIME = 1.0  # 1 second


def is_hdf5(domain, **kwargs):
    """Determine if domain is valid HSDS domain.
    kwargs can be endpoint, username, password, etc. (same as with File)
    """
    found = False
    try:
        # set use_cache to False to avoid extensive load time
        f = File(domain, use_cache=False, **kwargs)
        if f:
            found = True
    except IOError:
        pass  # ignore any non-200 error
    return found


class H5Image(io.RawIOBase):
    """ file-like-object class that treats bytes of an HSDS dataset as an HDF5 file image
        Can be used as a subsitute for a file path in h5py.File(filepath).  E.g.:
        f = h5py.File(H5Image("hdf5:/myhsds_domain"))   """

    def __init__(self, domain_path, h5path="h5image", chunks_per_page=1, logger=None):
        """ verify dataset can be accessed and set logger if supplied """
        self._cursor = 0
        if domain_path.startswith("hdf5::/"):
            self._domain_path = domain_path
        else:
            self._domain_path = "hdf5:/" + domain_path
        f = File(domain_path)
        if h5path not in f:
            raise IOError(f"Expected '{h5path}' dataset")
        dset = f[h5path]
        if len(dset.shape) != 1:
            raise IOError("Expected one-dimensional dataset")
        self._dset = dset
        num_chunks = -(dset.shape[0] // -dset.chunks[0])
        if chunks_per_page < 1:
            chunks_per_page = 1
        elif chunks_per_page > num_chunks:
            chunks_per_page = num_chunks  # use the entire file as one page
        else:
            pass  # accept requested values
        num_pages = -(num_chunks // -chunks_per_page)
        self._page_cache = [None,] * num_pages
        self._chunks_per_page = chunks_per_page
        self._logger = logger
        if self._logger:
            self._logger.info(f"domain {self._domain_path} opened")

    def __repr__(self):
        """ Just rturn the domain path"""
        return f'<{self._domain_path}>'

    def readable(self):
        """ it is """
        return True

    def seekable(self):
        """ seek is ok """
        return True

    @property
    def size(self):
        """ return size of HDF5 image in bytes """
        return self._dset.shape[0]

    @property
    def page_size(self):
        """ return page_size in element count"""
        return self._dset.chunks[0] * self._chunks_per_page

    def tell(self):
        """ return the current cursor position """
        return self._cursor

    def seek(self, offset, whence=io.SEEK_SET):
        """ set the seek pointer """
        if whence == io.SEEK_SET:
            if self._logger:
                self._logger.debug(f"SEEK_SET({offset})")
            self._cursor = offset
        elif whence == io.SEEK_CUR:
            if self._logger:
                self._logger.debug(f"SEEK_CUR({offset})")
            self._cursor += offset
        elif whence == io.SEEK_END:
            if self._logger:
                self._logger.debug(f"SEEK_END({offset})")
            self._cursor = self.size + offset
        else:
            raise ValueError(f'{whence}: Unknown whence value')
        if self._logger:
            self._logger.debug(f"cursor: {self._cursor}")
        return self._cursor

    def _get_page(self, page_number):
        """ Return bytes for the given page.
            Read a page from the HSDS dataset if not already in the cache """
        if self._page_cache[page_number] is None:
            if self._logger:
                self._logger.info(f"reading page {page_number} from server")
            offset = page_number * self.page_size
            arr = self._dset[offset:offset + self.page_size]
            self._page_cache[page_number] = arr.tobytes()
        if self._logger:
            self._logger.debug(f"fetching page {page_number} from cache")
        return self._page_cache[page_number]

    def read(self, size=-1):
        """ Read size bytes from the cursor """
        start = self._cursor
        if size < 0 or self._cursor + size >= self.size:
            stop = self.size
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            stop = start + size
            self.seek(offset=size, whence=io.SEEK_CUR)

        if self._logger:
            self._logger.debug(f">>GET {start}:{stop}")

        buffer = bytearray(stop - start)
        offset = start
        while offset < stop:
            page_number = offset // self.page_size
            page_bytes = self._get_page(page_number)
            n = offset % self.page_size
            if stop // self.page_size > page_number:
                # just read to the end of the page
                m = self.page_size
            else:
                # remaing bytes don't cross page boundry
                m = n + (stop - offset)

            num_bytes = m - n
            buffer_start = offset - start
            buffer_stop = buffer_start + num_bytes
            buffer[buffer_start:buffer_stop] = page_bytes[n:m]

            offset += num_bytes

        if self._logger:
            self._logger.debug(f"returning: {len(buffer)} bytes")
        return buffer

    def readinto(self, buff):
        if self._logger:
            self._logger.debug(f"readinto({len(buff)})")
        data = self.read(len(buff))
        buff[:len(data)] = data
        return len(data)


class File(Group):

    """
    Represents an HDF5 file.
    """

    @property
    def attrs(self):
        """Attributes attached to this object"""
        # hdf5 complains that a file identifier is an invalid location for an
        # attribute. Instead of self, pass the root group to AttributeManager:
        from . import attrs

        # parent_obj = {"id": self.id.uuid}
        # return attrs.AttributeManager(self['/'])
        return attrs.AttributeManager(self)

    @property
    def filename(self):
        """File name on disk"""
        return self.id.http_conn.domain

    @property
    def driver(self):
        return "rest_driver"

    @property
    def mode(self):
        """Python mode used to open file"""
        return self.id.http_conn.mode

    @property
    def fid(self):
        """File ID (backwards compatibility)"""
        return self.id.domain

    @property
    def libver(self):
        """File format version bounds (2-tuple: low, high)"""
        return ("0.0.1", "0.0.1")

    @property
    def serverver(self):
        return self._version

    @property
    def userblock_size(self):
        """User block size (in bytes)"""
        return 0

    @property
    def created(self):
        """Creation time of the domain"""
        return self.id.http_conn.created

    @property
    def owner(self):
        """Username of the owner of the domain"""
        return self.id.http_conn.owner

    @property
    def limits(self):
        return self._limits

    @property
    def swmr_mode(self):
        """ Controls use of cached metadata """
        return self._swmr_mode

    @swmr_mode.setter
    def swmr_mode(self, value):
        # enforce the same rule as h5py - swrm_mode can't be changed after opening the file
        mode = self.id.http_conn.mode
        if mode == "r":
            # read only mode
            msg = "SWMR mode can't be changed after file open"
            raise ValueError(msg)
        if self._swmr_mode and not value:
            msg = "SWMR mode can only be set to off by closing the file"
            raise ValueError(msg)
        self._swmr_mode = True

    def __init__(
        self,
        domain,
        mode=None,
        endpoint=None,
        username=None,
        password=None,
        bucket=None,
        api_key=None,
        use_session=True,
        use_cache=True,
        swmr=False,
        libver=None,
        logger=None,
        owner=None,
        linked_domain=None,
        track_order=False,
        retries=10,
        timeout=180,
        **kwds,
    ):
        """Create a new file object.

        See the h5py user guide for a detailed explanation of the options.

        domain
            URI of the domain name to access. E.g.: /home/username/tall.h5.  Can also
            use DNS style:  tall.username.home
        mode
            Access mode: 'r', 'r+', 'w', or 'a'
        endpoint
            Server endpoint.   Defaults to "http://localhost:5101"
        username
            username for authentication
        password
            password for authentication
        bucket
            bucket (or storage container) to use for domain.  If not set, server default bucket will be used
        api_key
            user's api key (for server configurations that use api_key rather than username/password)
        use_session
            maintain http connect between calls
        use_cache
            save attribute and links values rather than retreiving from server each time they are accessed.
            Set to False if the storage content is expected to change due to another application
        swmr
            For compatibility with h5py - has the effect of overriding use_cache so that metadata
            will always be synchronized with the server
        libver
            For compatibility with h5py - library version bounds.  Has no effect other
            than returning given value as property
        logger
            supply log handler to be used
        owner
            set the owner to be used when new domain is created (defaults to username).  Only valid when used
            by admin users
        linked_domain
            Create new domain using the root of the linked domain
        track_order
            Whether to track dataset/group/attribute creation order within this file. Objects will be iterated
            in ascending creation order if this is enabled, otherwise in ascending alphanumeric order.
        retries
            Number of retry attempts to be used if a server request fails
        timeout
            Timeout value in seconds
        """

        groupid = None
        dn_ids = []
        # if we're passed a GroupId as domain, just initialize the file object
        # with that.  This will be faster and enable the File object to share the same http connection.
        no_endpoint_info = endpoint is None and username is None and password is None
        if (mode is None and no_endpoint_info and isinstance(domain, GroupID)):
            groupid = domain
        else:
            if mode and mode not in ("r", "r+", "w", "w-", "x", "a"):
                raise ValueError("Invalid mode; must be one of r, r+, w, w-, x, a")

            if mode is None:
                mode = "r"

            cfg = Config()  # pulls in state from a .hscfg file (if found).

            # accept domain values in the form:
            #   http://server:port/home/user/myfile.h5
            #    or
            #   https://server:port/home/user/myfile.h5
            #    or
            #   hdf5://home/user/myfile.h5
            #    or just
            #   /home/user/myfile.h5
            #
            #  For http prefixed values, extract the endpont and use the rest as domain path
            for protocol in ("http://", "https://", "hdf5://", "http+unix://"):
                if domain.startswith(protocol):
                    if protocol.startswith("http"):
                        domain = domain[len(protocol):]
                        # extract the endpoint
                        n = domain.find("/")
                        if n < 0:
                            raise IOError(400, "invalid url format")
                        endpoint = protocol + domain[:n]
                        domain = domain[n:]
                        break
                    else:  # hdf5://
                        domain = domain[(len(protocol) - 1):]

            if not domain:
                raise IOError(400, "no domain provided")

            domain_path = pathlib.PurePath(domain)
            if isinstance(domain_path, pathlib.PureWindowsPath):
                # Standardize path root to POSIX-style path
                domain = '/' + '/'.join(domain_path.parts[1:])

            if domain[0] != "/":
                raise IOError(400, "relative paths are not valid")

            if endpoint is None:
                if "hs_endpoint" in cfg:
                    endpoint = cfg["hs_endpoint"]

            # remove the trailing slash on endpoint if it exists
            if endpoint.endswith('/'):
                endpoint = endpoint.strip('/')

            if username is None:
                if "hs_username" in cfg:
                    username = cfg["hs_username"]

            if password is None:
                if "hs_password" in cfg:
                    password = cfg["hs_password"]

            if api_key is None and "hs_api_key" in cfg:
                api_key = cfg["hs_api_key"]

            if bucket is None:
                if "HS_BUCKET" in os.environ:
                    bucket = os.environ["HS_BUCKET"]
                elif "hs_bucket" in cfg:
                    bucket = cfg["hs_bucket"]

            if swmr:
                use_cache = False  # disable metadata caching in swmr mode

            http_conn = HttpConn(
                domain,
                endpoint=endpoint,
                username=username,
                password=password,
                bucket=bucket,
                mode=mode,
                api_key=api_key,
                use_session=use_session,
                use_cache=use_cache,
                logger=logger,
                retries=retries,
                timeout=timeout,
            )

            root_json = None

            # try to do a GET from the domain
            req = "/"
            params = {"getdnids": 1}  # return dn ids if available

            if use_cache and mode == "r":
                params["getobjs"] = "T"
                params["include_attrs"] = "T"
            if bucket:
                params["bucket"] = bucket

            params["CreateOrder"] = "1" if track_order else "0"

            # need some special logic for the first request in local mode
            # to give the sockets time to initialize

            if endpoint.startswith("local"):
                connect_backoff = [0.5, 1, 2, 4, 8, 16]
            else:
                connect_backoff = []

            connect_try = 0

            while True:
                try:
                    rsp = http_conn.GET(req, params=params)
                    break
                except IOError:
                    if connect_try < len(connect_backoff):
                        time.sleep(connect_backoff[connect_try])
                    else:
                        raise
                    connect_try += 1

            if rsp.status_code == 200:
                root_json = json.loads(rsp.text)
            if rsp.status_code != 200 and mode in ("r", "r+"):
                # file must exist
                http_conn.close()
                raise IOError(rsp.status_code, rsp.reason)
            if rsp.status_code == 200 and mode in ("w-", "x"):
                # Fail if exists
                http_conn.close()
                raise IOError(409, "domain already exists")
            if rsp.status_code == 200 and mode == "w":
                # delete existing domain
                rsp = http_conn.DELETE(req, params=params)
                if rsp.status_code not in (200, 410):
                    # failed to delete
                    http_conn.close()
                    raise IOError(rsp.status_code, rsp.reason)
                root_json = None
            if root_json and "root" not in root_json:
                http_conn.close()
                raise IOError(404, "Location is a folder, not a file")
            if root_json is None:
                # create the domain
                if mode not in ("w", "a", "x"):
                    http_conn.close()
                    raise IOError(404, "File not found")
                body = {}
                if owner:
                    body["owner"] = owner
                if linked_domain:
                    body["linked_domain"] = linked_domain
                rsp = http_conn.PUT(req, params=params, body=body)
                if rsp.status_code != 201:
                    http_conn.close()
                    raise IOError(rsp.status_code, rsp.reason)

                root_json = json.loads(rsp.text)
            if "root" not in root_json:
                http_conn.close()
                raise IOError(404, "Unexpected error")

            if "dn_ids" in root_json:
                dn_ids = root_json["dn_ids"]

            root_uuid = root_json["root"]

            if "limits" in root_json:
                self._limits = root_json["limits"]
            else:
                self._limits = None
            if "version" in root_json:
                self._version = root_json["version"]
            else:
                self._version = None

            if mode == "a":
                # for append, verify we have 'update' permission on the domain
                # try first with getting the acl for the current user, then as default
                for name in (username, "default"):
                    if not username:
                        continue
                    req = "/acls/" + name
                    rsp = http_conn.GET(req)
                    if rsp.status_code == 200:
                        rspJson = json.loads(rsp.text)
                        domain_acl = rspJson["acl"]
                        if not domain_acl["update"]:
                            http_conn.close()
                            raise IOError(403, "Forbidden")
                        else:
                            break  # don't check with "default" user in this case

            if mode in ("w", "w-", "x", "a"):
                http_conn._mode = "r+"

            group_json = None
            # do we already have the group_json?
            if "domain_objs" in root_json and mode == "r":
                objdb = root_json["domain_objs"]
                http_conn._objdb = objdb
                if root_uuid in objdb:
                    group_json = objdb[root_uuid]

            if not group_json:
                # get the group json for the root group
                req = "/groups/" + root_uuid

                rsp = http_conn.GET(req)

                if rsp.status_code != 200:
                    http_conn.close()
                    raise IOError(rsp.status_code, "Unexpected Error")
                group_json = json.loads(rsp.text)

            groupid = GroupID(None, group_json, http_conn=http_conn)
        # end else
        self._name = "/"
        self._id = groupid
        self._verboseInfo = None  # aditional state we'll get when requested
        self._verboseUpdated = None  # when the verbose data was fetched
        self._lastScan = None  # when summary stats where last updated by server
        self._dn_ids = dn_ids
        self._track_order = track_order
        self._swmr_mode = swmr

        Group.__init__(self, self._id, track_order=track_order)

    def _getVerboseInfo(self):
        now = time.time()
        if (
            self._verboseUpdated is None or now - self._verboseUpdated > VERBOSE_REFRESH_TIME
        ):
            # resynch the verbose data
            req = "/?verbose=1"
            rsp_json = self.GET(req, use_cache=False, params={"CreateOrder": "1" if self._track_order else "0"})

            self.log.debug("get verbose info")
            props = {}
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
                    props[k] = rsp_json[k]
            self._verboseInfo = props
            self._verboseUpdated = now
            if "scan_info" in rsp_json:
                scan_info = rsp_json["scan_info"]
                if "scan_complete" in scan_info:
                    self.log.debug("updating _lastScan")
                    self._lastScan = scan_info["scan_complete"]

        return self._verboseInfo

    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        props = self._getVerboseInfo()
        modified = self.id.http_conn.modified  # timestamp for the domain object
        # update with latest time of any domain object (if available)
        if "lastModified" in props:
            modified = props["lastModified"]
        return modified

    @property
    def num_objects(self):
        props = self._getVerboseInfo()
        num_objects = 0
        if "num_objects" in props:
            num_objects = props["num_objects"]
        return num_objects

    @property
    def num_datatypes(self):
        props = self._getVerboseInfo()
        num_datatypes = 0
        if "num_datatypes" in props:
            num_datatypes = props["num_datatypes"]
        return num_datatypes

    @property
    def num_groups(self):
        props = self._getVerboseInfo()
        num_groups = 0
        if "num_groups" in props:
            num_groups = props["num_groups"]
        return num_groups

    @property
    def num_chunks(self):
        props = self._getVerboseInfo()
        num_chunks = 0
        if "num_chunks" in props:
            num_chunks = props["num_chunks"]
        return num_chunks

    @property
    def num_linked_chunks(self):
        props = self._getVerboseInfo()
        num_linked_chunks = 0
        if "num_linked_chunks" in props:
            num_linked_chunks = props["num_linked_chunks"]
        return num_linked_chunks

    @property
    def num_datasets(self):
        props = self._getVerboseInfo()
        num_datasets = 0
        if "num_datasets" in props:
            num_datasets = props["num_datasets"]
        return num_datasets

    @property
    def allocated_bytes(self):
        props = self._getVerboseInfo()
        allocated_bytes = 0
        if "allocated_bytes" in props:
            allocated_bytes = props["allocated_bytes"]
        return allocated_bytes

    @property
    def metadata_bytes(self):
        props = self._getVerboseInfo()
        metadata_bytes = 0
        if "metadata_bytes" in props:
            metadata_bytes = props["metadata_bytes"]
        return metadata_bytes

    @property
    def linked_bytes(self):
        props = self._getVerboseInfo()
        linked_bytes = 0
        if "linked_bytes" in props:
            linked_bytes = props["linked_bytes"]
        return linked_bytes

    @property
    def total_size(self):
        props = self._getVerboseInfo()
        total_size = 0
        if "total_size" in props:
            total_size = props["total_size"]
        return total_size

    @property
    def md5_sum(self):
        props = self._getVerboseInfo()
        md5_sum = None
        if "md5_sum" in props:
            md5_sum = props["md5_sum"]
        return md5_sum

    @property
    def last_scan(self):
        self._getVerboseInfo()  # will update _lastScan
        return self._lastScan

    @property
    def compressors(self):
        """return list of compressors supported by this server"""
        if self.id:
            compressors = self.id.http_conn.compressors
        else:
            compressors = []
        return compressors

    # override base implemention of ACL methods to use the domain rather than update root group
    def getACL(self, username):
        req = "/acls/" + username
        rsp_json = self.GET(req)
        acl_json = rsp_json["acl"]
        return acl_json

    def getACLs(self):
        req = "/acls"
        rsp_json = self.GET(req)
        acls_json = rsp_json["acls"]
        return acls_json

    def putACL(self, acl):
        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            if k not in acl:
                raise IOError(404, "Missing ACL field: {}".format(k))
            perm[k] = acl[k]

        req = "/acls/" + acl["userName"]
        self.PUT(req, body=perm)

    def run_scan(self):
        MAX_WAIT = 10
        self._getVerboseInfo()
        prev_scan = self._lastScan
        if prev_scan is None:
            prev_scan = 0
        self.log.debug(f"run_scan - lastScan: {prev_scan}")

        # Tell server to re-run scan
        self.log.info("sending rescan request")
        params = {"rescan": 1}
        req = "/"
        self.PUT(req, params=params)

        for i in range(MAX_WAIT):
            self.log.debug("run_scan - sleeping")
            time.sleep(1)  # give the server a chance to run scan
            self._verboseUpdated = None  # clear verbose cache
            self._getVerboseInfo()
            self.log.debug(f"got new scan: {self._lastScan}")
            if self._lastScan and self._lastScan > prev_scan:
                self.log.info("scan has been updated")
                break

        if self._lastScan == prev_scan:
            self.log.warning("run_scan failed to update")

        return

    def flush(self):
        """Tells the service to complete any pending updates to permanent storage"""
        self.log.debug("flush")
        self.log.info("sending PUT flush request")
        req = "/"
        body = {"flush": 1, "getdnids": 1}
        rsp = self.PUT(req, body=body)
        if "dn_ids" in rsp:
            dn_ids = rsp["dn_ids"]
            orig_ids = set(self._dn_ids)
            current_ids = set(dn_ids)
            self._dn_ids = current_ids
            if orig_ids and orig_ids != current_ids:
                self.log.debug(f"original dn_ids: {orig_ids}")
                self.log.debug(f"current dn_ids: {current_ids}")
                self.log.warn("HSDS nodes have changed")
                raise IOError(500, "Unexpected Error")
        self.log.info("PUT flush complete")

    def close(self, flush=None):
        """Clears reference to remote resource."""
        # this will close the socket of the http_conn singleton

        self.log.debug(f"close, mode: {self.mode}")
        if flush is None:
            # set flush to true if this is a direct connect and file
            # is writable
            if self.mode == "r+" and self._id._http_conn._hsds:
                flush = True
            else:
                flush = False
        # do a PUT flush if this file is writable and the server is HSDS and flush is set
        if flush:
            self.flush()
        if self._id._http_conn:
            self._id._http_conn.close()
        self._id.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.id:
            self.close()

    def __repr__(self):
        if not self.id:
            r = "<Closed HDF5 file>"
        else:
            # Filename has to be forced to Unicode if it comes back bytes
            # Mode is always a "native" string
            filename = self.filename
            if isinstance(filename, bytes):  # Can't decode fname
                filename = filename.decode("utf8", "replace")
            full_path = os.path.basename(filename)
            r = f'<HDF5 file "{full_path}" (mode {self.mode})>'

        return r
