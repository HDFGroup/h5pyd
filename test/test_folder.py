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

import os.path as op
from common import ut, TestCase
from datetime import datetime
from copy import copy
import six


class TestFolders(TestCase):

     
    def test_list(self):
        test_domain = self.getFileName("folder_test")  
        print("test_domain:", test_domain)
        f = h5py.File(test_domain, 'w')  # create a new domain
        if not f.id.id.startswith("g-"):
            # Folders aren't implemented yet for h5serv, so skip
            return
        filepath = self.getPathFromDomain(test_domain)

        folder_name = op.dirname(filepath)  + '/' 
         
        dir = h5py.Folder(folder_name)  # get folder object
        self.assertEqual(dir.domain, folder_name)
        self.assertTrue(dir.modified)
        self.assertTrue(dir.created) 
        self.assertEqual(str(dir), folder_name)
        self.assertEqual(dir.owner, self.test_user1["name"])
        dir_parent = dir.parent
        self.assertEqual(dir.parent[:-1], op.dirname(folder_name[:-1]))

        # get ACL for dir
        dir_acl = dir.getACL(self.test_user1["name"])
        self.assertEqual(len(dir_acl.keys()), 6)
        for k in dir_acl.keys():
            self.assertTrue(dir_acl[k])

        dir_acls = dir.getACLs()
        self.assertTrue(isinstance(dir_acls, list))

        count = len(dir)
        self.assertTrue(count > 1)
           
        test_domain_found = False
        
        i = 0
        for name in dir:
            if name == "folder_test":
                self.assertFalse(test_domain_found)
                test_domain_found = True
            item = dir[name]
            self.assertTrue("lastModified" in item)
            self.assertTrue("created" in item)
            self.assertTrue("owner" in item)
            self.assertEqual(item["owner"], self.test_user1["name"])
            i += 1
        self.assertTrue(test_domain_found)
        self.assertEqual(i, count)

            
            
         

     
              
if __name__ == '__main__':
    ut.main()
