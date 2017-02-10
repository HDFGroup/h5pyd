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


class TestFile(TestCase):

    def test_version(self):
        version = h5py.version.version
        # should be of form "n.n.n"
        n = version.find(".")
        self.assertTrue(n>=1)
        m = version[(n+1):].find('.')
        self.assertTrue(m>=1)

    def test_create(self):
        filename = self.getFileName("new_file")
        print("filename:", filename)
        
        f = h5py.File(filename, 'w')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 0)
        self.assertEqual(f.mode, 'r+')
        if h5py.__name__ == "h5pyd":
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
        if h5py.__name__ == "h5pyd":
            self.assertTrue(isinstance(f.modified, datetime))
            #self.assertEqual(f.modified.tzname(), six.u('UTC'))

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

    def test_delete(self):
        filename = self.getFileName("delete_me")        
        print("filename:", filename)

        f = h5py.File(filename, 'w')

        for name in ("g1", "g2", "g1/g1.1"):
            f.create_group(name)
        f.close()

        f = h5py.File(filename, 'r') 
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 2)

        # removing file in read-mode should fail
        if h5py.__name__ == "h5pyd":
            try:
                f.remove()
                self.assertTrue(False)  # expected exception
            except ValueError as ve:
                self.assertEqual(str(ve), "Unable to remove file (No write intent on file)")

        f.close()
        
        f = h5py.File(filename, 'r+')
        self.assertEqual(f.filename, filename)
        self.assertEqual(f.name, "/")
        self.assertTrue(f.id.id is not None)
        self.assertEqual(len(f.keys()), 2)

        # delete the file
        if h5py.__name__ == "h5py":
            os.remove(filename)
        else:
            f.remove()
        if h5py.__name__ == "h5pyd":
            self.assertEqual(f.id.id, 0)

        # opening in read-mode should fail
        try:
            f = h5py.File(filename, 'r') 
            self.assertTrue(False)  # expected exception
        except IOError as ioe:
            if h5py.__name__ == "h5pyd":
                self.assertTrue(str(ioe) in ("Gone", "Not Found"))

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

        file_acl = f.getACL("test_user1")
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
        user1_acl["userName"] = "test_user1"
        f.close()


        # test_user2 has read access, but opening in write mode should fail
        try:
            f = h5py.File(filename, 'w', username="test_user2", password="test")
            self.assertFalse(is_hsds)  # expect exception for hsds
        except IOError as ioe:
            self.assertTrue(is_hsds)
            self.assertEqual(str(ioe), "Forbidden")  # user is not authorized

        # append mode w/ test_user2
        try:
            f = h5py.File(filename, 'a', username="test_user2", password="test")
            self.assertFalse(is_hsds)  # expected exception
        except IOError as ioe:
            self.assertTrue(is_hsds)
            self.assertEqual(str(ioe), "Forbidden")  # user is not authorized
        
        f = h5py.File(filename, 'a')  # open for append with original username
        # add an acl for test_user2 that has only read/update access
        user2_acl = copy(default_acl)
        user2_acl["userName"] = "test_user2"
        user2_acl["read"] = True  # allow read access
        user2_acl["update"] = True
        f.putACL(user2_acl)
 
        f.close()

        # test_user2  opening in write mode should still fail
        try:
            f = h5py.File(filename, 'w', username="test_user2", password="test")
            self.assertFalse(is_hsds)  # expected exception
        except IOError as ioe:
            self.assertTrue(is_hsds)
            self.assertEqual(str(ioe), "Forbidden")  # user is not authorized

        # append mode w/ test_user2
        try:
            f = h5py.File(filename, 'a', username="test_user2", password="test")
        except IOError as ioe:
            self.assertTrue(False)  # shouldn't get here

        grp = f['/']
        grp.file.close()  # try closing the file via a group reference
        
        f = h5py.File(filename, 'r+') 
 
        # delete the file
        f.remove()
        self.assertEqual(f.id.id, 0)

        try:
            f = h5py.File(filename, 'r+') 
        except IOError as ioe:
            self.assertTrue(str(ioe) in ("Gone", "Not Found"))
           
              
if __name__ == '__main__':
    ut.main()
