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
    import os
else:
    import h5pyd as h5py

from common import ut, TestCase
from datetime import datetime
from copy import copy
import six
import time


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
        is_hsds = False
        if isinstance(f.id.id, str) and f.id.id.startswith("g-"):
            is_hsds = True  # HSDS has different permission defaults
        if h5py.__name__ == "h5pyd":
            self.assertTrue(f.id.http_conn.endpoint.startswith("http"))
        self.assertTrue(f.id.id is not None)
        self.assertTrue('/' in f)
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
            self.assertEqual(f.modified, f.created)
        if is_hsds:
            # owner prop is just for HSDS
            self.assertEqual(f.owner, self.test_user1["name"]) 
         
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

        try:
            f.create_group("another_subgrp")
            self.assertTrue(False)  # expect exception
        except ValueError:
            pass
        self.assertEqual(len(f.keys()), 1)

        if h5py.__name__ == "h5pyd":
            # check properties that are only available for h5pyd
            # Note: num_groups won't reflect current state since the
            # data is being updated asynchronously
            self.assertEqual(f.num_chunks, 0)
            self.assertTrue(f.num_groups >= 0)
            self.assertEqual(f.num_datasets, 0)
            self.assertEqual(f.num_datatypes, 0)
            self.assertTrue(f.allocated_bytes >= 0)

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
            self.assertEqual(len(file_acls), 2)  # HSDS setup creates two initial acls - "default" and test_user1
        else:
            self.assertEqual(len(file_acls), 0)

        file_acl = f.getACL(self.test_user1["name"])
        # there's no ACL for test_User1 yet, so this should return the default ACL
        acl_keys = ("create", "read", "update", "delete", "readACL", "updateACL")
        #self.assertEqual(file_acl["userName"], "default")   
        for k in acl_keys:
            self.assertEqual(file_acl[k], True)

        # Should always be able to get default acl
        default_acl = f.getACL("default")
        for k in acl_keys:
            if k == "read" or not is_hsds:
                self.assertEqual(default_acl[k], True)
            else:
                self.assertEqual(default_acl[k], False)
       
        user1_acl = copy(default_acl)
        user1_acl["userName"] = self.test_user1["name"]
        f.close()


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
            self.assertEqual(ioe.errno, 403)  # user is not authorized
        
        f = h5py.File(filename, 'a')  # open for append with original username
        # add an acl for test_user2 that has only read/update access
        user2_acl = copy(default_acl)
        user2_acl["userName"] = self.test_user2["name"]
        user2_acl["read"] = True  # allow read access
        user2_acl["update"] = True
        f.putACL(user2_acl)
 
        f.close()

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
        
        
         

    def test_close(self):
        filename = self.getFileName("close_file")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        self.assertTrue(f)
        f.close()
        self.assertFalse(f)
           
              
if __name__ == '__main__':
    ut.main()
