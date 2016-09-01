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

import os
import six

import requests
import json

from .objectid import GroupID
from .base import phil, parse_lastmodified, getHeaders
from .group import Group
from .. import version


hdf5_version = version.hdf5_version_tuple[0:3]


class File(Group):

    """
        Represents an HDF5 file.
    """

    @property
    def attrs(self):
        """ Attributes attached to this object """
        # hdf5 complains that a file identifier is an invalid location for an
        # attribute. Instead of self, pass the root group to AttributeManager:
        from . import attrs
        return attrs.AttributeManager(self['/'])

    @property
    def filename(self):
        """File name on disk"""
        return self.id.domain

    @property
    def driver(self):
        return "rest_driver"

    @property
    def mode(self):
        """ Python mode used to open file """
        return self.id._mode

    @property
    def fid(self):
        """File ID (backwards compatibility) """
        return self.id.domain

    @property
    def libver(self):
        """File format version bounds (2-tuple: low, high)"""
        # bounds = self.id.get_access_plist().get_libver_bounds()
        # return tuple(libver_dict_r[x] for x in bounds)
        return ("0.0.1",)

    @property
    def userblock_size(self):
        """ User block size (in bytes) """
        return 0

    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        return self._modified

    def __init__(self, domain_name, mode=None, endpoint=None, 
        username=None, password=None, **kwds):
        """Create a new file object.

        See the h5py user guide for a detailed explanation of the options.

        domain_name
            URI of the domain name to access. E.g.: tall.data.hdfgroup.org.
        mode
            Access mode: 'r', 'r+', 'w', or 'a'
        endpoint
            Server endpoint.   Defaults to "http://localhost:5000"
        """

        with phil:
            """
            if isinstance(name, _objects.ObjectID):
                fid = h5i.get_file_id(name)
            else:
                try:
                    # If the byte string doesn't match the default
                    # encoding, just pass it on as-is.  Note Unicode
                    # objects can always be encoded.
                    name = name.encode(sys.getfilesystemencoding())
                except (UnicodeError, LookupError):
                    pass

                fapl = make_fapl(driver, libver, **kwds)
            """
            if mode and mode not in ('r', 'r+', 'w', 'w-', 'x', 'a'):
                raise ValueError(
                    "Invalid mode; must be one of r, r+, w, w-, x, a")

            if mode is None:
                mode = 'a'

            if endpoint is None:
                if "H5SERV_ENDPOINT" in os.environ:
                    endpoint = os.environ["H5SERV_ENDPOINT"]
                else:
                    endpoint = "http://127.0.0.1:5000"

            root_json = None

            # try to do a GET from the domain
            req = endpoint + "/"
             
            headers = getHeaders(domain=domain_name, username=username, password=password)
            
            rsp = requests.get(req, headers=headers, verify=self.verifyCert())

            if rsp.status_code == 200:
                root_json = json.loads(rsp.text)

            if rsp.status_code != 200 and mode in ('r', 'r+'):
                # file must exist
                raise IOError(rsp.reason)
            if rsp.status_code == 200 and mode in ('w-', 'x'):
                # Fail if exists
                raise IOError("domain already exists")
            if rsp.status_code == 200 and mode == 'w':
                # delete existing domain
                rsp = requests.delete(req, headers=headers,
                                      verify=self.verifyCert())
                if rsp.status_code != 200:
                    # failed to delete
                    raise IOError(rsp.reason)
                root_json = None
            if root_json is None:
                # create the domain
                if mode not in ('w', 'a'):
                    raise IOError("File not found")
                rsp = requests.put(req, headers=headers,
                                   verify=self.verifyCert())
                if rsp.status_code != 201:
                    raise IOError(rsp.reason)
                root_json = json.loads(rsp.text)

            if 'root' not in root_json:
                raise IOError("Unexpected error")
            if 'created' not in root_json:
                raise IOError("Unexpected error")
            if 'lastModified' not in root_json:
                raise IOError("Unexpected error")

            if mode in ('w', 'w-', 'x', 'a'):
                mode = 'r+'

            # print "root_json:", root_json
            root_uuid = root_json['root']

            # get the group json for the root group
            req = endpoint + "/groups/" + root_uuid

            rsp = requests.get(req, headers=headers, verify=self.verifyCert())

            # print "req:", req

            if rsp.status_code != 200:
                raise IOError("Unexpected Error")
            group_json = json.loads(rsp.text)

            self._id = GroupID(None, group_json, domain=domain_name,
                               endpoint=endpoint, username=username,
                               password=password, mode=mode)

            self._name = '/'
            self._created = root_json['created']
            self._modified = parse_lastmodified(root_json['lastModified'])

            Group.__init__(self, self._id)

    def close(self):
        """ Clears reference to remote resource.
        """
        self._id.close()

    def remove(self):
        """ Deletes the domain on the server"""
        if self.id.mode == 'r':
            raise ValueError("Unable to remove file (No write intent on file)")
        self.DELETE('/')
        self._id.close()

    def flush(self):
        """ For h5py compatibility, doesn't currently do anything in h5pyd.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.id:
            self.close()

    def __repr__(self):
        if not self.id:
            r = six.u('<Closed HDF5 file>')
        else:
            # Filename has to be forced to Unicode if it comes back bytes
            # Mode is always a "native" string
            filename = self.filename
            if isinstance(filename, bytes):  # Can't decode fname
                filename = filename.decode('utf8', 'replace')
            r = (six.u('<HDF5 file "%s" (mode %s)>') %
                 (os.path.basename(filename), self.mode))

        if six.PY3:
            return r
        return r.encode('utf8')
