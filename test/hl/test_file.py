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
from copy import copy
import time
import logging


class TestFile(TestCase):

    def test_version(self):
        version = h5py.version.version
        # should be of form "n.n.n"
        n = version.find(".")
        self.assertTrue(n >= 1)
        m = version[(n + 1):].find('.')
        self.assertTrue(m >= 1)

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

        if h5py.__name__ == "h5pyd":
            self.assertTrue(f.id.http_conn.endpoint.startswith("http"))
        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)
        # should not see id as a file
        # skip for h5py, since its is_hdf5 implementation expects a path
        if h5py.__name__ == "h5pyd":
            self.assertFalse(h5py.is_hdf5(f.id.id))
        # Check domain's timestamps
        if h5py.__name__ == "h5pyd":
            # print("modified:", datetime.fromtimestamp(f.modified), f.modified)
            # print("created: ", datetime.fromtimestamp(f.created), f.created)
            # print("now:     ", datetime.fromtimestamp(now), now)
            # verify the timestamps make sense
            # we add a 30-sec margin to account for possible time skew
            # between client and server
            self.assertTrue(f.created - 30.0 < now)
            self.assertTrue(f.created + 30.0 > now)
            self.assertTrue(f.modified - 30.0 < now)
            self.assertTrue(f.modified + 30.0 > now)
            self.assertTrue(f.modified >= f.created)

            self.assertTrue(len(f.owner) > 0)
            version = f.serverver
            # server version should be of form "n.n.n"
            n = version.find(".")
            self.assertTrue(n >= 1)
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

        for mode in ("w-", "x"):
            try:
                # re-open is exclusive mode (should fail)
                h5py.File(filename, mode)
                self.assertTrue(False)
            except IOError:
                pass

        # re-open as read-write
        f = h5py.File(filename, 'w')
        self.assertTrue(f.id.id is not None)
        self.assertEqual(f.mode, 'r+')
        self.assertEqual(len(f.keys()), 0)
        root_grp = f['/']
        # f.create_group("subgrp")
        root_grp.create_group("subgrp")
        self.assertEqual(len(f.keys()), 1)
        f.close()
        self.assertEqual(f.id.id, 0)

        # rre-open in append mode
        f = h5py.File(filename, "a")
        f.create_group("foo")
        del f["foo"]
        f.close()

        # re-open as read-only
        if h5py.__name__ == "h5pyd":
            wait_time = 90  # change to >90 to test async updates
            print("waiting {wait_time:d} seconds for root scan sync".format(wait_time=wait_time))
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

        if h5py.__name__ == "h5pyd":
            # check properties that are only available for h5pyd
            # Note: num_groups won't reflect current state since the
            # data is being updated asynchronously
            self.assertEqual(f.num_objects, 3)
            self.assertEqual(f.num_groups, 3)

            self.assertEqual(f.num_datasets, 0)
            self.assertEqual(f.num_datatypes, 0)
            self.assertTrue(f.allocated_bytes == 0)

        f.close()
        self.assertEqual(f.id.id, 0)

        # re-open using hdf5:// prefix
        if h5py.__name__ == "h5pyd":
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
            with h5py.File(filename, 'r') as f:
                self.assertNotEqual(f, None)
                self.assertTrue(False)  # expect exception
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
        self.assertEqual(len(f.keys()), 2)

        # no explicit ACLs yet
        file_acls = f.getACLs()
        self.assertTrue(len(file_acls) >= 1)  # Should have at least the test_user1 acl

        username = f.owner

        file_acl = f.getACL(username)
        # default owner ACL should grant full permissions
        acl_keys = ("create", "read", "update", "delete", "readACL", "updateACL")
        # self.assertEqual(file_acl["userName"], "default")
        for k in acl_keys:
            self.assertEqual(file_acl[k], True)

        try:
            default_acl = f.getACL("default")
        except IOError as ioe:
            if ioe.errno == 404:
                pass  # expected

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
        except IOError:
            self.assertTrue(False)

        # test_user2 has read access, but opening in write mode should fail
        try:
            f = h5py.File(filename, 'w', username=self.test_user2["name"], password=self.test_user2["password"])
            self.assertFalse(True)  # expect exception for hsds
        except IOError as ioe:
            self.assertEqual(ioe.errno, 403)  # user is not authorized

        # append mode w/ test_user2
        try:
            f = h5py.File(filename, 'a', username=self.test_user2["name"], password=self.test_user2["password"])
            self.assertFalse(True)  # expected exception
        except IOError as ioe:
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
        except IOError:
            self.assertTrue(False)

        # test_user2  opening in write mode should still fail
        try:
            f = h5py.File(filename, 'w', username=self.test_user2["name"], password=self.test_user2["password"])
            self.assertFalse(True)  # expected exception
        except IOError as ioe:
            self.assertEqual(ioe.errno, 403)  # user is not authorized

        # append mode w/ test_user2
        try:
            f = h5py.File(filename, 'a', username=self.test_user2["name"], password=self.test_user2["password"])
        except IOError:
            self.assertTrue(False)  # shouldn't get here

        grp = f['/']
        grp.file.close()  # try closing the file via a group reference

    def test_http_url(self):
        if h5py.__name__ == "h5pyd":
            if "HS_ENDPOINT" in os.environ:
                endpoint = os.environ["HS_ENDPOINT"]
                filename = self.getFileName("test_http_url_file")
                f = h5py.File(filename, 'w')

                f.close()

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


class TestTrackOrder(TestCase):
    titles = ("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")

    def populate(self, f):
        count = len(self.titles)
        # create count datasets/groups
        for i in range(count):
            title = self.titles[i]
            # Mix group and dataset creation.
            if i % 2 == 0:
                f.create_group(title)
            else:
                f[title] = [i]
        # create count attributes
        for i in range(count):
            title = self.titles[i]
            f.attrs[title] = i

    def test_track_order(self):
        filename = self.getFileName("test_track_order_file")
        print(f"filename: {filename}")
        # write file using creation order
        with h5py.File(filename, 'w', track_order=True) as f:
            self.populate(f)
            self.assertEqual(list(f), list(self.titles))
            self.assertEqual(list(f.attrs), list(self.titles))

        with h5py.File(filename) as f:
            # domain/file should have been saved with track_order state
            self.assertEqual(list(f), list(self.titles))
            self.assertEqual(list(f.attrs), list(self.titles))

    def test_cfg_track_order(self):
        filename = self.getFileName("test_cfg_track_order_file")
        print(f"filename: {filename}")
        # write file using creation order
        cfg = h5py.get_config()
        cfg.track_order = True
        with h5py.File(filename, 'w') as f:
            self.populate(f)
            self.assertEqual(list(f), list(self.titles))
            self.assertEqual(list(f.attrs), list(self.titles))
        cfg.track_order = False  # reset

        with h5py.File(filename) as f:
            # domain/file should have been saved with track_order state
            self.assertEqual(list(f), list(self.titles))
            self.assertEqual(list(f.attrs), list(self.titles))

    def test_no_track_order(self):
        filename = self.getFileName("test_no_track_order_file")
        print(f"filename: {filename}")

        # create file using alphanumeric order
        with h5py.File(filename, 'w', track_order=False) as f:
            self.populate(f)
            self.assertEqual(list(f), sorted(self.titles))

        with h5py.File(filename) as f:  # name alphanumeric
            self.assertEqual(list(f), sorted(self.titles))


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
