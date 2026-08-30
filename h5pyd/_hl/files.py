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
from ..hsds_plugin import HsdsPlugin

from .. import config


VERBOSE_REFRESH_TIME = 1.0  # 1 second


def is_hdf5(domain, **kwargs):
    """Determine if domain is valid HSDS domain.
    kwargs can be endpoint, username, password, etc. (same as with File)
    """
    found = False

    app_logger = kwargs.get("app_logger")
    db = Hdf5db(app_logger=app_logger)
    db.plugin = HsdsPlugin(domain, read_only=True, **kwargs)
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
        if self.id.db.plugin:
            filepath = self.id.db.plugin.filepath
        return filepath

    def _getStats(self):
        """ return info on storage usage """
        self._verifyOpen()

        now = time.time()
        if self._verboseInfo is None or now - self._verboseUpdated > 1:
            # refresh info from server

            if self.id.db.plugin:
                stats = self.id.db.plugin.getStats(verbose=True)
            else:
                stats = {"created": 0, "lastModified": 0, "owner": 0}

            self._verboseUpdated = time.time()
            if "scan_info" in stats:
                scan_info = stats["scan_info"]
                if "scan_complete" in stats:
                    self.log.debug("updating _lastScan")
                    self._lastScan = scan_info["scan_complete"]
            self._verboseInfo = stats.copy()  # keep a copy
        else:
            stats = self._verboseInfo.copy()  # use cached copy

        return stats

    def _verifyOpen(self):
        if not self.id:
            raise ValueError("file is closed")

    def getACL(self, username):
        """ Return the ACL (access control list) entry for the given username """
        self._verifyOpen()
        return self.id.db.plugin.getACL(username)

    def getACLs(self):
        """ Return all the ACLs (access control list) for the domain """
        self._verifyOpen()
        return self.id.db.plugin.getACLs()

    def putACL(self, acl):
        """ Create or update an ACL (access control list) for the domain """
        self._verifyOpen()
        self.id.db.plugin.putACL(acl)

    @property
    def driver(self):
        return "rest_driver"

    @property
    def mode(self):
        """Python mode used to open file"""

        self._verifyOpen()
        mode = 'r'
        if not self.id.db.plugin.read_only:
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
        return stats.get("created")

    @property
    def owner(self):
        """Username of the owner of the domain"""
        stats = self._getStats()
        return stats.get("owner")

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
        if value and not self._swmr_mode:
            # entering SWMR mode is the writer's signal that the file's
            # structure is now stable and safe to read concurrently - flush
            # any pending metadata (e.g. a just-created dataset) so a reader
            # opening the domain fresh in another process can actually see
            # it, rather than racing the writer's next unrelated flush
            self.id.db.flush()
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
                 track_order=None,
                 getobjs=True,
                 retries=10,
                 timeout=180,
                 **kwds,
                 ):
        # initialize h5db using domain path

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

        # remove the trailing slash on endpoint if it exists
        if endpoint and endpoint.endswith('/'):
            endpoint = endpoint.strip('/')

        db = Hdf5db(app_logger=self.log)  # initialize hdf5 db

        if track_order is None:
            cfg = config.get_config()
            if cfg.track_order:
                track_order = True
            else:
                track_order = None

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
        if track_order:
            kwargs["track_order"] = track_order

        new_domain = False

        if mode in ('w-', 'x'):
            file_exists = is_hdf5(domain, **kwargs)
            if file_exists:
                raise FileExistsError()
            # domain doesn't exist - fall through and create it below
            db.plugin = HsdsPlugin(domain, getobjs=getobjs, **kwargs)
            new_domain = True
        elif mode in ('r', 'r+', 'a'):
            read_only = mode == 'r'
            db.plugin = HsdsPlugin(domain, append=True, read_only=read_only, getobjs=getobjs, **kwargs)
        else:
            # mode == 'w' - create/overwrite the domain
            db.plugin = HsdsPlugin(domain, getobjs=getobjs, **kwargs)
            new_domain = True

        db.open()

        if new_domain:
            # Flip the plugin out of its initial "bulk create" mode (_init) while
            # the domain is still empty, so real content added afterward by the
            # caller always goes through the normal per-object/per-selection
            # update path on flush, rather than a single merged full-array
            # rewrite the next time flush() happens to run - which loses the
            # original write selections and can conflict with chunking for a
            # resized/extended dataset.
            db.flush()

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
            if track_order is not None:
                kwargs["track_order"] = track_order
            kwargs["getobjs"] = True  # TBD: disable this optionally?

            db = self._init_db(domain, **kwargs)

        root_id = db.root_id
        root_json = db.getObjectById(root_id, refresh=True)

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
        self._verboseUpdated = 0  # when the verbose data was fetched
        self._lastScan = None  # when summary stats where last updated by server
        self._swmr_mode = swmr

        Group.__init__(self, self._id, track_order=track_order)

    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        stats = self._getStats()
        return stats["lastModified"]

    @property
    def num_objects(self):
        stats = self._getStats()
        num_objects = 0
        if "num_objects" in stats:
            num_objects = stats["num_objects"]
        return num_objects

    @property
    def num_datatypes(self):
        stats = self._getStats()
        num_datatypes = 0
        if "num_datatypes" in stats:
            num_datatypes = stats["num_datatypes"]
        return num_datatypes

    @property
    def num_groups(self):
        stats = self._getStats()
        num_groups = 0
        if "num_groups" in stats:
            num_groups = stats["num_groups"]
        return num_groups

    @property
    def num_chunks(self):
        stats = self._getStats()
        num_chunks = 0
        if "num_chunks" in stats:
            num_chunks = stats["num_chunks"]
        return num_chunks

    @property
    def num_linked_chunks(self):
        stats = self._getStats()
        num_linked_chunks = 0
        if "num_linked_chunks" in stats:
            num_linked_chunks = stats["num_linked_chunks"]
        return num_linked_chunks

    @property
    def num_datasets(self):
        stats = self._getStats()
        num_datasets = 0
        if "num_datasets" in stats:
            num_datasets = stats["num_datasets"]
        return num_datasets

    @property
    def allocated_bytes(self):
        stats = self._getStats()
        allocated_bytes = 0
        if "allocated_bytes" in stats:
            allocated_bytes = stats["allocated_bytes"]
        return allocated_bytes

    @property
    def metadata_bytes(self):
        stats = self._getStats()
        metadata_bytes = 0
        if "metadata_bytes" in stats:
            metadata_bytes = stats["metadata_bytes"]
        return metadata_bytes

    @property
    def linked_bytes(self):
        stats = self._getStats()
        linked_bytes = 0
        if "linked_bytes" in stats:
            linked_bytes = stats["linked_bytes"]
        return linked_bytes

    @property
    def total_size(self):
        stats = self._getStats()
        total_size = 0
        if "total_size" in stats:
            total_size = stats["total_size"]
        return total_size

    @property
    def md5_sum(self):
        stats = self._getStats()
        md5_sum = None
        if "md5_sum" in stats:
            md5_sum = stats["md5_sum"]
        return md5_sum

    @property
    def last_scan(self):
        self._getStats()  # will update _lastScan
        return self._lastScan

    @property
    def compressors(self):
        """return list of compressors supported by this server"""
        self._verifyOpen()
        stats = self._getStats()
        compressors = stats.get("compressors")
        if compressors is None:
            # server didn't report a list - fall back to every compressor
            # h5pyd knows how to represent client-side
            compressors = COMPRESSION_FILTER_NAMES
        return compressors

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
