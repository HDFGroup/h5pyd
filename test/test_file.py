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

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase
from datetime import datetime
import six


class TestFile(TestCase):

    def test_version(self):
        version = h5py.version.version
        # should be of form "n.n.n"
        n = version.find(".")
        self.assertTrue(n>=1)
        m = version[(n+1):].find('.')
        self.assertTrue(n>=1)

    def test_create(self):
        filename = self.getFileName("new_file")
        print("filename:", filename)
        
        f = h5py.File(filename, 'w')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 0)
        self.assertEqual(f.mode, 'r+')
        self.assertTrue(f.id.endpoint.startswith("http"))
        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)
        r = f['/']
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertEqual(len(f.attrs.keys()), 0)
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

        # Check domain's last modified time
        self.assertTrue(isinstance(f.modified, datetime))
        self.assertEqual(f.modified.tzname(), six.u('UTC'))

        try:
            f.create_group("another_subgrp")
            self.assertTrue(False)  # expect exception
        except ValueError:
            pass
        self.assertEqual(len(f.keys()), 1)

        f.close()
        self.assertEqual(f.id.id, 0)

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

        # verify open of non-existent file throws exception
        try:
            filename = self.getFileName("no_file_here")
            print("filename:", filename)
            f = h5py.File(filename, 'r')
            self.assertTrue(False) #expect exception
        except IOError:
            pass
        
    def test_auth(self):
        filename = "tallp.hdfgroup.org"
        print("filename:", filename)

        docker_endpoint = "http://192.168.1.100:5000"
        
        try:
            f = h5py.File(filename, 'r', endpoint=docker_endpoint) 
            self.assertTrue(False)    # should be blocked from unauthenticated access
        except IOError as ioe:
            self.assertEqual(str(ioe), "Unauthorized")

        f = h5py.File(filename, 'a', endpoint=docker_endpoint, username="test_user1", password="test")
        root_id = f.id.id

        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(f.id.username, "test_user1")
        self.assertEqual(f.id.password, "test")
        self.assertEqual(len(f.keys()), 2)

        file_acl = f.getACL("default")
        self.assertEqual(file_acl["userName"], "default")        
        self.assertEqual(file_acl["create"], False)
        self.assertEqual(file_acl["read"], False)
        self.assertEqual(file_acl["update"], False)
        self.assertEqual(file_acl["delete"], False)
        self.assertEqual(file_acl["readACL"], False)
        self.assertEqual(file_acl["updateACL"], False)

        file_acl = f.getACL("test_user1")
        self.assertEqual(file_acl["userName"], "test_user1")        
        self.assertEqual(file_acl["create"], True)
        self.assertEqual(file_acl["read"], True)
        self.assertEqual(file_acl["update"], True)
        self.assertEqual(file_acl["delete"], True)
        self.assertEqual(file_acl["readACL"], True)
        self.assertEqual(file_acl["updateACL"], True)

        file_acl = f.getACL("test_user2")
        self.assertEqual(file_acl["userName"], "test_user2")        
        self.assertEqual(file_acl["create"], False)
        self.assertEqual(file_acl["read"], True)
        self.assertEqual(file_acl["update"], False)
        self.assertEqual(file_acl["delete"], False)
        self.assertEqual(file_acl["readACL"], False)
        self.assertEqual(file_acl["updateACL"], False)

        try:
            file_acl = f.getACL("not_a_user")
            self.assertTrue(False)
        except IOError as ioe:
            self.assertEqual(str(ioe), "username does not exist")

        # TBD: get ACLS
        #file_acls = f.getACLs()
        #print("acls:", file_acls)

        f.close()

        # test_user2 has read, but not write access
        try:
            f = h5py.File(filename, 'w', endpoint=docker_endpoint, username="test_user2", password="test") 
            self.assertTrue(False)    # should be blocked from unauthenticated access
        except IOError as ioe:
            self.assertEqual(str(ioe), "Forbidden")        


if __name__ == '__main__':
    ut.main()
