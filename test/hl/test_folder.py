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

import logging
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
        print(filepath)
        # create test file if not present.
        # on first run, this may take a minute before it is visible as a folder item
        f = h5py.File(filepath, mode='a')
        if config.get("use_h5py"):
            # Folders not supported for h5py
            f.close()
            return

        self.assertTrue(f.id.id is not None)
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # HSDS currently supports folders, but h5serv does not
            f.close()
            return
        f.close()

        folder_name = op.dirname(filepath)  + '/'

        d = h5py.Folder(folder_name)  # get folder object    

        self.assertEqual(d.domain, folder_name)
        self.assertTrue(d.modified)
        self.assertTrue(d.created)
        self.assertEqual(str(d), folder_name)
        self.assertEqual(d.owner, self.test_user1["name"])

        dir_parent = d.parent
        self.assertEqual(dir_parent[:-1], op.dirname(folder_name[:-1]))

        # get ACL for dir
        dir_acl = d.getACL(self.test_user1["name"])
        self.assertEqual(len(dir_acl.keys()), 7)
        for k in dir_acl.keys():
            self.assertTrue(dir_acl[k])

        dir_acls = d.getACLs()
        self.assertTrue(isinstance(dir_acls, list))

        count = len(d)
        self.assertTrue(count > 1)

        test_domain_found = False

        i = 0
        for name in d:
            if name == op.basename(test_domain):
                self.assertFalse(test_domain_found)
                test_domain_found = True
            item = d[name]
            #'owner': 'test_user1',
            #'created': 1496729517.2346532,
            #'class': 'domain',
            #'name': '/org/hdfgroup/h5pyd_test/bool_dset',
            #'lastModified': 1496729517.2346532
            #self.assertTrue("created" in item)
            self.assertTrue("owner" in item)
            self.assertTrue("class" in item)
            self.assertTrue("name" in item)
            if "root" in item:
                # non-folder objects will have last modified time
                self.assertTrue("lastModified" in item)
                self.assertTrue("created" in item)
                # shouldn't have total_size, other verbose only,items
                self.assertFalse("total_size" in item)

            i += 1
        self.assertTrue(test_domain_found)
        self.assertEqual(i, count)
        d.close()

        # open in verbose mode
        d = h5py.Folder(folder_name, verbose=True)  # get folder object     

        self.assertEqual(d.domain, folder_name)
        self.assertTrue(d.modified)
        self.assertTrue(d.created)
        self.assertEqual(str(d), folder_name)
        self.assertEqual(d.owner, self.test_user1["name"])

        dir_parent = d.parent
        self.assertEqual(dir_parent[:-1], op.dirname(folder_name[:-1]))

        # get ACL for dir
        dir_acl = d.getACL(self.test_user1["name"])
        self.assertEqual(len(dir_acl.keys()), 7)
        for k in dir_acl.keys():
            self.assertTrue(dir_acl[k])

        dir_acls = d.getACLs()
        self.assertTrue(isinstance(dir_acls, list))

        count = len(d)
        self.assertTrue(count > 1)

        test_domain_found = False

        i = 0
        for name in d:
            if name == op.basename(test_domain):
                self.assertFalse(test_domain_found)
                test_domain_found = True
            item = d[name]
            #'owner': 'test_user1',
            #'created': 1496729517.2346532,
            #'class': 'domain',
            #'name': '/org/hdfgroup/h5pyd_test/bool_dset',
            #'lastModified': 1496729517.2346532
            #self.assertTrue("created" in item)
            self.assertTrue("owner" in item)
            self.assertTrue("class" in item)
            self.assertTrue("name" in item)
            if "root" in item:
                # non-folder objects will have last modified time
                self.assertTrue("lastModified" in item)
                self.assertTrue("created" in item)
                # these should show up only in verbose mode
                self.assertTrue("md5_sum" in item)
                self.assertTrue("num_groups" in item)
                self.assertTrue("num_datasets" in item)
                self.assertTrue("num_datatypes" in item)
                self.assertTrue("num_objects" in item)
                self.assertTrue("num_chunks" in item)
                self.assertTrue("num_linked_chunks" in item)
                self.assertTrue("total_size" in item)
                self.assertTrue("allocated_bytes" in item)
                self.assertTrue("metadata_bytes" in item)
                self.assertTrue("metadata_bytes" in item)
                self.assertTrue("linked_bytes" in item)
            i += 1
        self.assertTrue(test_domain_found)
        self.assertEqual(i, count)
        d.close()


        # try opening a domain object as a folder
        f = h5py.Folder(filepath + '/')
        count = len(f)
        self.assertEqual(count, 0)
        for name in f:
            self.assertTrue(False)  # unexpected
        f.close()


    def test_create_folder(self):
        empty = self.getFileName("empty")
        empty_path = self.getPathFromDomain(empty)

        print("empty_path", empty_path)

        f = h5py.File(empty_path, mode='a')
        self.assertTrue(f.id.id is not None)
        if config.get("use_h5py"):
            # Folders not supported for h5py
            f.close()
            return

        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # HSDS currently supports folders, but h5serv does not
            f.close()
            return
        f.close()

        folder_test = self.getFileName("create_folder_test")
        folder_path = self.getPathFromDomain(folder_test) + '/'

        d = h5py.Folder(folder_path, mode='w')  # create a new folder
        d.close()
        # re-open
        d = h5py.Folder(folder_path)
        self.assertTrue(d.is_folder)
        d.close()


    def test_root_folder(self):
        test_domain = self.getFileName("folder_test")


        filepath = self.getPathFromDomain(test_domain)
        f = h5py.File(filepath, mode='a')
        self.assertTrue(f.id.id is not None)
        if config.get("use_h5py"):
            # Folders not supported for h5py
            f.close()
            return
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # HSDS currently supports folders, but h5serv does not
            f.close()
            return
        f.close()

        path_components = filepath.split('/')
        top_level_domain = path_components[1]

        d = h5py.Folder('/')  # get folder object for root
        found = False
        self.assertTrue(len(d) > 0)
        self.assertTrue(d.is_folder)
        self.assertTrue(d.domain == '/')
        self.assertTrue(d.__repr__() == '/')
        self.assertIsNone(d.parent)
        for name in d:
            # we should come across the given domain
            if top_level_domain == name:
                found = True
        d.close()
        self.assertTrue(found)

if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
