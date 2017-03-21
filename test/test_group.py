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


class TestGroup(TestCase):


    def test_cache(self):
        # create main test file
        filename = self.getFileName("create_group_cache")
        print("filename:", filename)
        f = h5py.File(filename, 'w', use_cache=True)
        self.assertTrue('/' in f)
        r = f['/'] 
        self.assertEqual(len(r), 0)
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertTrue(r.name, '/')
        self.assertEqual(len(r.attrs.keys()), 0)
        self.assertFalse('g1' in r)
         
        g1 = r.create_group('g1')
        self.assertEqual(len(r), 1)
        file = g1.file
        f.close()

    def test_create(self):
        # create main test file
        filename = self.getFileName("create_group")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        self.assertTrue('/' in f)
        r = f['/'] 
        self.assertEqual(len(r), 0)
        self.assertTrue(isinstance(r, h5py.Group))
        self.assertTrue(r.name, '/')
        self.assertEqual(len(r.attrs.keys()), 0)
        self.assertFalse('g1' in r)

        r.create_group("g1")
        self.assertEqual(len(r), 1)
        self.assertTrue('g1' in r)

        g1 = r['g1']
        self.assertTrue(g1.id.id != r.id.id)
        self.assertEqual(g1.name, "/g1")

        r.create_group("g1/g1.1")
        g1_1 = r["g1/g1.1"]
        self.assertEqual(g1_1.name, "/g1/g1.1")
        self.assertEqual(len(r), 1)
        self.assertEqual(len(g1), 1)
           
        r.create_group('g2')
        self.assertEqual(len(r), 2)
        keys = []
        # iterate through keys
        for k in r:
            keys.append(k)

        self.assertEqual(len(keys), 2)
        self.assertTrue('g1' in keys)
        self.assertTrue('g2' in keys)

        self.assertTrue('g1' in r)
        self.assertTrue('/g1' in r)
        g1 = r.get('/g1')
        self.assertTrue(g1.id.id)
        self.assertEqual(g1.name, '/g1')

        g1_class = r.get('g1', getclass=True)
        self.assertEqual(g1_class, h5py.Group)

        # try creating a group that already exists
        try:
            r.create_group('g1')
            self.assertTrue(False)
        except ValueError:
            pass # expected

        r.create_group('g3')
        self.assertEqual(len(r), 3)
        del r['g3']
        self.assertEqual(len(r), 2)

        r.require_group('g4')
        self.assertEqual(len(r), 3)
        r.require_group('g2')
        self.assertEqual(len(r), 3)

        # create a hardlink
        tmp_grp = r.create_group("tmp")
        r['g1.1'] = tmp_grp

        # try to replace the link
        try:
            r['g1.1'] = g1_1
            self.assertTrue(False)  # shouldn't get here'
        except RuntimeError:
            pass # expected
        
        del r['tmp']
        self.assertEqual(len(r), 4)

        # create a softlink
        r['mysoftlink'] = h5py.SoftLink('/g1/g1.1')
        self.assertEqual(len(r), 5)
        self.assertEqual(len(g1), 1)
        self.assertEqual(len(g1_1), 0)

        slink = r['mysoftlink']
        self.assertEqual(slink.id, g1_1.id)

        # create a file that we'll link to
        link_target_filename = self.getFileName("link_target")
        g = h5py.File(link_target_filename, 'w')
        g.create_group("somepath")
        g.close()

        # create a external hardlink
        r['myexternallink'] = h5py.ExternalLink(link_target_filename, "somepath")
        # test getclass
        g1_class = r.get('g1', getclass=True)
        self.assertEqual(g1_class, h5py.Group)
        linkee_class = r.get('mysoftlink', getclass=True)
        self.assertEqual(linkee_class, h5py.Group)
        link_class = r.get('mysoftlink', getclass=True, getlink=True)
        self.assertEqual(link_class, h5py.SoftLink)
        softlink = r.get('mysoftlink', getlink=True)
        self.assertEqual(softlink.path, '/g1/g1.1')

        linkee_class = r.get('myexternallink', getclass=True)
        link_class = r.get('myexternallink', getclass=True, getlink=True)
        self.assertEqual(link_class, h5py.ExternalLink)
        external_link = r.get('myexternallink', getlink=True)
        self.assertEqual(external_link.path, 'somepath')
        external_link_filename = external_link.filename
        if config.get('use_h5py'):
            # h5py external link should be a posix path
            self.assertTrue(external_link_filename.find('/') > 0)
        else:
            # HDF Server should be a DNS style name
            self.assertEqual(external_link_filename.find('/'), -1)

        links = r.items()
        got_external_link = False
        for link in links:
            title = link[0]
            obj = link[1]
            if title == 'myexternallink':
                self.assertTrue(obj is not None)            
                self.assertEqual(len(obj), 0)
                self.assertTrue(obj.file.filename != filename)
                got_external_link = True

        self.assertTrue(got_external_link)

        del r['mysoftlink']
        self.assertEqual(len(r), 5)

        del r['myexternallink']
        self.assertEqual(len(r), 4)

        # create group using nested path
        g2 = r['g2']
        r['g1/g1.3'] = g2
        self.assertEqual(len(r), 5)

        # try creating a link with a space in the name
        r["a space"] = g2
        self.assertEqual(len(r), 6)
         
        # Check group's last modified time
        if h5py.__name__ == "h5pyd":
            self.assertTrue(isinstance(g1.modified, datetime))
            #self.assertEqual(g1.modified.tzname(), six.u('UTC'))
         
        f.close()
        

if __name__ == '__main__':
    ut.main()




