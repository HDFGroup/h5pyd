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

import os.path as op
from common import ut, TestCase


class TestFolders(TestCase):

     
    def test_list(self):
        #loglevel = logging.DEBUG
        #logging.basicConfig( format='%(asctime)s %(message)s', level=loglevel)
        test_domain = self.getFileName("folder_test")  

        
        filepath = self.getPathFromDomain(test_domain)
        print("test_domain:", filepath)
        # create test file if not present.
        # on first run, this may take a minute before it is visible as a folder item
        f = h5py.File(filepath, mode='a')  
        self.assertTrue(f.id.id is not None)
        f.close()

        folder_name = op.dirname(filepath)  + '/'  
        print("folder_name:", folder_name)
         
        dir = h5py.Folder(folder_name)  # get folder object
         
        self.assertEqual(dir.domain, folder_name)
        self.assertTrue(dir.modified)
        self.assertTrue(dir.created) 
        self.assertEqual(str(dir), folder_name)
        self.assertEqual(dir.owner, self.test_user1["name"])
        
        dir_parent = dir.parent
        self.assertEqual(dir_parent[:-1], op.dirname(folder_name[:-1]))

        # get ACL for dir
        dir_acl = dir.getACL(self.test_user1["name"])
        self.assertEqual(len(dir_acl.keys()), 7)
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
            #'owner': 'test_user1', 
            #'created': 1496729517.2346532, 
            #'class': 'domain', 
            #'name': '/org/hdfgroup/h5pyd_test/bool_dset', 
            #'lastModified': 1496729517.2346532
            self.assertTrue("lastModified" in item)
            self.assertTrue("created" in item)
            self.assertTrue("owner" in item)
            self.assertTrue("class" in item)

            i += 1
        self.assertTrue(test_domain_found)
        self.assertEqual(i, count)
        dir.close()

        # try opening a domain object as a folder
        f = h5py.Folder(filepath + '/')
        count = len(f)
        self.assertEqual(count, 0)
        for name in f:
            self.assertTrue(False)  # unexpected
        f.close()
         

    def test_create_folder(self):
        folder_test = self.getFileName("create_folder_test")  
        folder_path = self.getPathFromDomain(folder_test) + '/'

        print("folder_path", folder_path)
        dir = h5py.Folder(folder_path, mode='w')  # create a new folder
        dir.close()
        # re-open
        dir = h5py.Folder(folder_path)
        self.assertTrue(dir.is_folder)
        dir.close()
        

    def test_root_folder(self):
        test_domain = self.getFileName("folder_test")  

        
        filepath = self.getPathFromDomain(test_domain)
        path_components = filepath.split('/')
        top_level_domain = path_components[1]

        dir = h5py.Folder('/')  # get folder object for root
        found = False
        self.assertTrue(len(dir) > 0)
        self.assertTrue(dir.is_folder)
        for name in dir:
            # we should come across the given domain
            if top_level_domain == name:
                found = True
        dir.close()
        self.assertTrue(found)


     
              
if __name__ == '__main__':
    ut.main()
