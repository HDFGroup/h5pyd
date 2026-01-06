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
import logging
import pathlib
import time

from h5json import Hdf5db
from h5json.filters import COMPRESSION_FILTER_NAMES

from .objectid import GroupID
from .group import Group
from ..hsds_reader import HSDSReader
from ..hsds_writer import HSDSWriter

from .. import config


VERBOSE_REFRESH_TIME = 1.0  # 1 second


def is_hdf5(domain, **kwargs):
    """Determine if domain is valid HSDS domain.
    kwargs can be endpoint, username, password, etc. (same as with File)
    """
    found = False

    app_logger = kwargs.get("app_Logger")
    db = Hdf5db(app_logger=app_logger)
    db.reader = HSDSReader(domain, **kwargs)
    try:
        db.open()
        found = True
    except IOError as ioe:
        if ioe.errno in (404, 410):
            # not found
            pass
        else:
            # other exception (403, etc.)
            raise
    return found


class H5Image(io.RawIOBase):
    """ file-like-object class that treats bytes of an HSDS dataset as an HDF5 file image
        Can be used as a subsitute for a file path in h5py.File(filepath).  E.g.:
        f = h5py.File(H5Image("hdf5:/myhsds_domain"))   """

    def __init__(self, domain_path, h5path="h5image", chunks_per_page=1, logger=None):
        """ verify dataset can be accessed and set logger if supplied """
        self._cursor = 0
        if domain_path and domain_path.startswith("hdf5::/"):
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
        from . import attrs

        return attrs.AttributeManager(self)

    @property
    def filename(self):
        """File name on disk"""
        filepath = None
        if self.id.db.reader:
            filepath = self.id.db.reader.filepath
        elif self.id.db.writer:
            filepath = self.id.db.writer.filepath
        else:
            pass  # no persistent storage enabled
        return filepath

    def _getStats(self):
        """ return info on storage usage """
        self._verifyOpen()
        if self.id.db.writer:
            stats = self.id.db.writer.getStats()
        elif self.id.db.reader:
            stats = self.id.db.reader.getStats()
        else:
            stats = {"created": 0, "lastModified": 0, "owner": 0}
        return stats

    def _verifyOpen(self):
        if not self.id:
            raise ValueError("file is closed")

    @property
    def driver(self):
        return "rest_driver"

    @property
    def mode(self):
        """Python mode used to open file"""

        self._verifyOpen()
        mode = 'r'
        if self.id.db.writer and self.id.db.writer.__class__.__name__ != "H5NullWriter":
            mode += '+'
        return mode

    @property
    def fid(self):
        """File ID (backwards compatibility)"""
        self._verifyOpen()
        return self.filename

    @property
    def libver(self):
        """File format version bounds (2-tuple: low, high)"""
        return ("0.0.1", "0.0.1")

    @property
    def serverver(self):
        stats = self._getStats()

        return stats.get("version")

    @property
    def userblock_size(self):
        """User block size (in bytes)"""
        return 0

    @property
    def created(self):
        """Creation time of the domain"""
        self._verifyOpen()
        stats = self._getStats()
        return stats["created"]

    @property
    def owner(self):
        """Username of the owner of the domain"""
        stats = self._getStats()
        return stats["owner"]

    @property
    def limits(self):
        stats = self._getStats()
        return stats.get("limits")

    @property
    def swmr_mode(self):
        """ Controls use of cached metadata """
        self._verifyOpen()
        return self._swmr_mode

    @swmr_mode.setter
    def swmr_mode(self, value):
        """ enforce the same rule as h5py - swmr_mode can't be changed after
          opening the file for read-only """
        self._verifyOpen()
        mode = self.mode
        if mode == "r":
            # read only mode
            msg = "SWMR mode can't be changed after file open"
            raise ValueError(msg)
        if self._swmr_mode and not value:
            msg = "SWMR mode can only be set to off by closing the file"
            raise ValueError(msg)
        self._swmr_mode = True

    def _init_db(self,
                 domain,
                 mode=None,
                 endpoint=None,
                 username=None,
                 password=None,
                 bucket=None,
                 api_key=None,
                 swmr=False,
                 retries=10,
                 timeout=180,
                 **kwds,
                 ):
        # initialize h5db using domain path

        cfg = config.get_config()  # pulls in state from a .hscfg file (if found).

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
            if domain and domain.startswith(protocol):
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
        if endpoint and endpoint.endswith('/'):
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

        db = Hdf5db(app_logger=self.log)  # initialize hdf5 db

        kwargs = {"app_logger": self.log}
        if swmr:
            kwargs["swmr"] = True  # disable metadata caching in swmr mode
        if username:
            kwargs["username"] = username
        if password:
            kwargs["password"] = password
        if endpoint:
            kwargs["endpoint"] = endpoint
        if bucket:
            kwargs["bucket"] = bucket
        if api_key:
            kwargs["api_key"] = api_key
        if retries:
            kwargs["retries"] = retries
        if timeout:
            kwargs["timeout"] = timeout

        root_id = None

        if mode != 'w':
            file_exists = is_hdf5(domain, **kwargs)
            if file_exists:
                if mode in ('w-', 'x'):
                    self.log.warning(f"Domain: {domain} already exists")
                    raise FileExistsError()
                db.reader = HSDSReader(domain, **kwargs)
                root_id = db.open()
            else:
                if mode in ('r', 'r+'):
                    self.log.warning(f"domain: {domain} not found")
                    raise FileNotFoundError()
        else:
            file_exists = False  # will overwrite in either case

        if root_id:
            # if mode is not read only, setup the writer
            if mode != 'r':
                db.close()
                db.writer = HSDSWriter(domain, append=True, **kwargs)
                db.open()
        else:
            # new domain, use writer to initialize domain
            db.writer = HSDSWriter(domain, **kwargs)
            root_id = db.open()
            # now set the reader
            db.reader = HSDSReader(domain, **kwargs)
            db.close()
            db.open()
        return db

    def __init__(
        self,
        domain,
        mode=None,
        endpoint=None,
        username=None,
        password=None,
        bucket=None,
        api_key=None,
        swmr=False,
        libver=None,
        logger=None,
        owner=None,
        track_order=None,
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
            in ascending creation order if this is True, if False in ascending alphanumeric order.
            If None use global default get_config().track_order.
        retries
            Number of retry attempts to be used if a server request fails
        timeout
            Timeout value in seconds
        """

        self.log = logging.getLogger()

        self.log.setLevel(logging.ERROR)

        # if we're passed a GroupId as domain, just initialize the file object
        # with that.  This will be faster and enable the File object to share the same http connection.
        no_endpoint_info = endpoint is None and username is None and password is None
        if (mode is None and no_endpoint_info and isinstance(domain, GroupID)):
            groupid = domain
            db = groupid.db
            if db.closed:
                db.open()

        else:
            if mode and mode not in ("r", "r+", "w", "w-", "x", "a"):
                raise ValueError("Invalid mode; must be one of r, r+, w, w-, x, a")

            if mode is None:
                mode = "r"

            kwargs = {"mode": mode}
            # any specific settings
            if api_key:
                kwargs["api_key"] = api_key
            if endpoint:
                kwargs["endpoint"] = endpoint
            if username:
                kwargs["username"] = username
            if password:
                kwargs["password"] = password
            if owner:
                kwargs["owner"] = owner
            if swmr:
                kwargs["swmr"] = swmr
            if bucket:
                kwargs["bucket"] = bucket

            db = self._init_db(domain, **kwargs)

        root_id = db.root_id
        root_json = db.getObjectById(root_id)

        if "limits" in root_json:
            self._limits = root_json["limits"]
        else:
            self._limits = None
        if "version" in root_json:
            self._version = root_json["version"]
        else:
            self._version = None

        self._id = GroupID(None, root_id, obj_json=root_json, db=db)

        self._db = db

        self._name = "/"
        self._verboseInfo = None  # additional state we'll get when requested
        self._verboseUpdated = None  # when the verbose data was fetched
        self._lastScan = None  # when summary stats where last updated by server
        self._swmr_mode = swmr

        Group.__init__(self, self._id, track_order=track_order)

    def _getVerboseInfo(self):
        self.verifyOpen()
        # now = time.time()
        return {}
        """
        if (self._verboseUpdated is None or now - self._verboseUpdated > VERBOSE_REFRESH_TIME):
            # resynch the verbose data
            req = "/?verbose=1"
            rsp_json = self.(req, use_cache=False, params={"CreateOrder": "1" if self._track_order else "0"})

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
        """

    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        stats = self._getStats()
        return stats["lastModified"]

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
        self._verifyOpen()
        if self.id:
            # compressors = self.id.http_conn.compressors
            compressors = COMPRESSION_FILTER_NAMES
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

    def close(self):
        """Clears reference to remote resource."""
        # this will flush any pending changes and close the http connection
        if self.id:
            self.id.close()

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
