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

import config
import os

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase
from datetime import datetime
from copy import copy
import six
import time
import logging

class TestFile(TestCase):

    def test_version(self):
        version = h5py.version.version
        # should be of form "n.n.n"
        n = version.find(".")
        self.assertTrue(n>=1)
        m = version[(n+1):].find('.')
        self.assertTrue(m>=1)

    def test_serverinfo(self):
        if h5py.__name__ == "h5pyd":
            info = h5py.getServerInfo()
            self.assertTrue("greeting" in info)
            self.assertTrue("name" in info)
            self.assertTrue("about" in info)
            self.assertTrue("endpoint" in info)
            self.assertTrue("username" in info)
            self.assertTrue("password" in info)
            if "hsds_version" in info:
                # server is HSDS
                self.assertTrue("node_count" in info)
                node_count = info["node_count"]
                self.assertTrue(node_count >= 1)
                self.assertTrue("isadmin" in info)


    def test_create(self):
        filename = self.getFileName("new_file")
        print("filename:", filename)
        now = time.time()
        f = h5py.File(filename, 'w')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 0)
        self.assertEqual(f.mode, 'r+')
        self.assertTrue(h5py.is_hdf5(filename))
        is_hsds = False
        if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
            is_hsds = True  # HSDS has different permission defaults
        if h5py.__name__ == "h5pyd":
            self.assertTrue(f.id.http_conn.endpoint.startswith("http"))
        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)
        # should not see id as a file
        self.assertFalse(h5py.is_hdf5(f.id.id))
        # Check domain's timestamps
        if h5py.__name__ == "h5pyd" and is_hsds:
            # TBD: remove is_hsds when h5serv timestamp changed to float
            #print("modified:", datetime.fromtimestamp(f.modified), f.modified)
            #print("created: ", datetime.fromtimestamp(f.created), f.created)
            #print("now:     ", datetime.fromtimestamp(now), now)
            # verify the timestamps make sense
            # we add a 30-sec margin to account for possible time skew
            # between client and server
            self.assertTrue(f.created - 30.0 < now)
            self.assertTrue(f.created + 30.0 > now)
            self.assertTrue(f.modified - 30.0 < now)
            self.assertTrue(f.modified + 30.0 > now)
            self.assertTrue(f.modified >= f.created)
        if is_hsds:
            # owner prop is just for HSDS
            self.assertTrue(len(f.owner) > 0)
            version = f.serverver
             # server version should be of form "n.n.n"
            n = version.find(".")
            self.assertTrue(n>=1)
            limits = f.limits
            for k in ('min_chunk_size', 'max_chunk_size', 'max_request_size'):
                self.assertTrue(k in limits)

        r = f['/']
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertEqual(len(f.attrs.keys()), 0)

        # flush any pending changes - this would be called by f.close() internally,
        # but try here to confirm it can be called explicitly
        f.flush()

        f.close()
        self.assertEqual(f.id.id, 0)

        # re-open as read-write
        f = h5py.File(filename, 'w')
        self.assertTrue(f.id.id is not None)
        self.assertEqual(f.mode, 'r+')
        self.assertEqual(len(f.keys()), 0)
        root_grp = f['/']
        #f.create_group("subgrp")
        root_grp.create_group("subgrp")
        self.assertEqual(len(f.keys()), 1)
        f.close()
        self.assertEqual(f.id.id, 0)
         
        # re-open as read-only
        if is_hsds:
            wait_time = 90
            print("waiting {} seconds for root scan sync".format(wait_time))
            time.sleep(wait_time)  # let async process update obj number
        f = h5py.File(filename, 'r')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 1)
        self.assertEqual(f.mode, 'r')
        self.assertTrue('/' in f)
        r = f['/']
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertEqual(len(f.attrs.keys()), 0)

        # verify that trying to modify the file fails
        try:
            f.create_group("another_subgrp")
            self.assertTrue(False)  # expect exception
        except (IOError, OSError, ValueError):
            # h5py throws ValueError
            # h5pyd is throwing IOError
            pass

        try:
            f.attrs["foo"] = "bar"
            self.assertTrue(False)  # expect exception
        except (IOError, OSError):
            pass

        self.assertEqual(len(f.keys()), 1)

        if  h5py.__name__ == "h5pyd":
            # check properties that are only available for h5pyd
            # Note: num_groups won't reflect current state since the
            # data is being updated asynchronously
            if is_hsds:
                self.assertEqual(f.num_objects, 2)
                self.assertEqual(f.num_groups, 2)
            else:
                # reported as 0 for h5serv
                self.assertEqual(f.num_objects, 0)
                self.assertEqual(f.num_groups, 0)

            self.assertEqual(f.num_datasets, 0)
            self.assertEqual(f.num_datatypes, 0)
            self.assertTrue(f.allocated_bytes == 0)

        f.close()
        self.assertEqual(f.id.id, 0)

        # re-open using hdf5:// prefix
        if h5py.__name__ == "h5pyd" and is_hsds:
            if filename[0] == '/':
                filepath = "hdf5:/" + filename
            else:
                filepath = "hdf5://" + filename
            f = h5py.File(filepath, 'r')
            self.assertEqual(f.filename, filename)
            self.assertEqual(f.name, "/")
            self.assertTrue(f.id.id is not None)
            self.assertEqual(len(f.keys()), 1)
            self.assertEqual(f.mode, 'r')
            self.assertTrue('/' in f)
            f.close()


        # open in truncate mode
        f = h5py.File(filename, 'w')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 0)
        self.assertEqual(f.mode, 'r+')
        self.assertTrue('/' in f)
        r = f['/']
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertEqual(len(f.attrs.keys()), 0)

        f.close()
        self.assertEqual(f.id.id, 0)


    def test_open_notfound(self):
        # verify open of non-existent file throws exception

        try:
            filename = self.getFileName("no_file_here")
            print("filename:", filename)
            f = h5py.File(filename, 'r')
            self.assertTrue(False) #expect exception
        except IOError:
            pass


    def test_auth(self):
        if h5py.__name__ == "h5py":
            return  # ACLs are just for h5pyd

        filename = self.getFileName("file_auth")
        print("filename:", filename)

        f = h5py.File(filename, 'w')

        for name in ("g1", "g2", "g1/g1.1"):
            f.create_group(name)

        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        root_id = f.id.id
        self.assertEqual(len(f.keys()), 2)

        is_hsds = False
        if root_id.startswith("g-"):
            is_hsds = True  # HSDS has different permission defaults

        # no explicit ACLs yet
        file_acls = f.getACLs()
        if is_hsds:
            self.assertTrue(len(file_acls) >= 1)  # Should have at least the test_user1 acl
        else:
            self.assertEqual(len(file_acls), 0)

        if is_hsds:
            username = f.owner
        else:
            username = "test_user1"

        file_acl = f.getACL(username)
        # default owner ACL should grant full permissions
        acl_keys = ("create", "read", "update", "delete", "readACL", "updateACL")
        #self.assertEqual(file_acl["userName"], "default")
        for k in acl_keys:
            self.assertEqual(file_acl[k], True)

        # for h5serv a default acl should be available
        # hsds does not create one initially

        try:
            default_acl = f.getACL("default")
        except IOError as ioe:
            if ioe.errno == 404:
                if is_hsds:
                    pass # expected
                else:
                    self.assertTrue(False)

        if is_hsds:
            # create  public-read ACL
            default_acl = {}
            for key in acl_keys:
                if key == "read":
                    default_acl[key] = True
                else:
                    default_acl[key] = False
            default_acl["userName"] = "default"
            f.putACL(default_acl)
        f.close()

        # ooen with test_user2 should succeed for read mode
        try:
            f = h5py.File(filename, 'r', username=self.test_user2["name"], password=self.test_user2["password"])
            f.close()
        except IOError as ioe:
            self.assertTrue(False)

        # test_user2 has read access, but opening in write mode should fail
        try:
            f = h5py.File(filename, 'w', username=self.test_user2["name"], password=self.test_user2["password"])
            self.assertFalse(is_hsds)  # expect exception for hsds
        except IOError as ioe:
            self.assertTrue(is_hsds)
            self.assertEqual(ioe.errno, 403)  # user is not authorized

        # append mode w/ test_user2
        try:
            f = h5py.File(filename, 'a', username=self.test_user2["name"], password=self.test_user2["password"])
            self.assertFalse(is_hsds)  # expected exception
        except IOError as ioe:
            self.assertTrue(is_hsds)
            self.assertEqual(ioe.errno, 403)  # Forbidden

        f = h5py.File(filename, 'a')  # open for append with original username
        # add an acl for test_user2 that has only read/update access
        user2_acl = copy(default_acl)
        user2_acl["userName"] = self.test_user2["name"]
        user2_acl["read"] = True  # allow read access
        user2_acl["update"] = True
        user2_acl["readACL"] = True
        f.putACL(user2_acl)

        f.close()

        # ooen with test_user2 should succeed for read mode
        try:
            f = h5py.File(filename, 'r', username=self.test_user2["name"], password=self.test_user2["password"])
        except IOError as ioe:
            self.assertTrue(False)

        # test_user2  opening in write mode should still fail
        try:
            f = h5py.File(filename, 'w', username=self.test_user2["name"], password=self.test_user2["password"])
            self.assertFalse(is_hsds)  # expected exception
        except IOError as ioe:
            self.assertTrue(is_hsds)
            self.assertEqual(ioe.errno, 403)  # user is not authorized

        # append mode w/ test_user2
        try:
            f = h5py.File(filename, 'a', username=self.test_user2["name"], password=self.test_user2["password"])
        except IOError as ioe:
            self.assertTrue(False)  # shouldn't get here

        grp = f['/']
        grp.file.close()  # try closing the file via a group reference

    def test_http_url(self):
        if h5py.__name__ == "h5pyd":
            if "HS_ENDPOINT" in os.environ:
                endpoint = os.environ["HS_ENDPOINT"]
                filename = self.getFileName("test_http_url_file")
                is_hsds = False
                f = h5py.File(filename, 'w')
                if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
                    is_hsds = True  # HSDS has different permission defaults
                f.close()
                if is_hsds:
                    url = endpoint + filename
                    f = h5py.File(url, 'w')
                    self.assertEqual(f.filename, filename)
                    self.assertEqual(f.name, "/")
                    self.assertTrue(f.id.id is not None)
                    self.assertEqual(len(f.keys()), 0)
                    self.assertEqual(f.mode, 'r+')
                    f.close()
            else:
                print("set HS_ENDPOINT environment variable to enable test_http_url test")

    def test_close(self):
        filename = self.getFileName("close_file")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        self.assertTrue(f)
        f.close()
        self.assertFalse(f)



if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
