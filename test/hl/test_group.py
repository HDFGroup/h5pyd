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
import logging
if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase
from datetime import datetime
import os.path


class TestGroup(TestCase):

    def test_del(self):
        filename = self.getFileName("test_del")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        self.assertTrue('/' in f)
        f.create_group("deadcat")
        del f["deadcat"]
        f.close()
        f = h5py.File(filename)

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
            pass  # expected

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
            pass  # expected
        except OSError:
            pass  # also acceptable

        del r['tmp']
        self.assertEqual(len(r), 4)

        # create a softlink
        r['mysoftlink'] = h5py.SoftLink('/g1/g1.1')
        self.assertTrue("mysoftlink" in r)
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
        print(f"test {r}.get, getclass and getlink are true")
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
        self.assertTrue(external_link_filename.find('link_target') > -1)

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

        self.assertEqual(len(r), 4)

        # try creating a link with a space in the name
        r["a space"] = g2
        self.assertEqual(len(r), 5)

        # re-create softlink
        r['mysoftlink'] = h5py.SoftLink('/g1/g1.1')

        # Check group's last modified time
        if h5py.__name__ == "h5pyd":
            self.assertTrue(isinstance(g1.modified, datetime))

        # try creating an anon group
        anon_group = g1.create_group(None)
        anon_group_id = anon_group.id.id

        f.close()

        # re-open file in read-only mode
        f = h5py.File(filename, 'r')
        self.assertEqual(len(f), 6)
        for name in ("g1", "g2", "g4", "g1.1", "a space", "mysoftlink"):
            self.assertTrue(name in f)
        self.assertTrue("/g1/g1.1" in f)
        g1_1 = f["/g1/g1.1"]
        r = f["/"]

        linkee_class = r.get('mysoftlink', getclass=True)
        self.assertEqual(linkee_class, h5py.Group)
        link_class = r.get('mysoftlink', getclass=True, getlink=True)
        self.assertEqual(link_class, h5py.SoftLink)
        softlink = r.get('mysoftlink', getlink=True)
        self.assertEqual(softlink.path, '/g1/g1.1')
        linked_obj = f["mysoftlink"]
        self.assertEqual(linked_obj.id, g1_1.id)

        if h5py.__name__ == "h5pyd":
            # for h5pyd we should be able to retrieve the anon group
            anon_group = f[anon_group_id]
            self.assertEqual(anon_group_id, anon_group.id.id)
            # can also get anon group using groups/<uuid> key
            anon_group = f[f"groups/{anon_group_id}"]
            self.assertEqual(anon_group_id, anon_group.id.id)

        f.close()

    def test_nested_create(self):
        filename = self.getFileName("create_nested_group")
        print("filename:", filename)
        f = h5py.File(filename, 'w')
        self.assertTrue('/' in f)
        r = f['/']
        self.assertEqual(len(r), 0)

        # create multiple groups in a path
        r.create_group("/g1/g1.1")
        self.assertEqual(len(r), 1)
        self.assertTrue("g1" in r)
        g1 = r["g1"]
        self.assertTrue("g1.1" in g1)

        # same think, but with relative path
        g1.create_group("g1.2/g1.2.1/g1.2.1.1")
        self.assertEqual(len(g1), 2)
        self.assertTrue("g1.2" in g1)

        f.close()

    def test_external_links(self):
        # create a file for use a link target
        linked_filename = self.getFileName("linked_file")

        f = h5py.File(linked_filename, 'w')
        abs_filepath = os.path.abspath(linked_filename)
        if h5py.__name__ == "h5pyd":
            rel_filepath = "linked_file.h5"
        else:
            rel_filepath = os.path.relpath(linked_filename)

        g1 = f.create_group("g1")
        dset = g1.create_dataset('ds', (5, 7), dtype='f4')
        dset_id = dset.id.id
        f.close()

        filename = self.getFileName("external_links")
        print("filename:", filename)

        f = h5py.File(filename, 'w')
        f["missing_link"] = h5py.ExternalLink(abs_filepath, "somepath")
        f["abspath_link"] = h5py.ExternalLink(abs_filepath, "/g1/ds")
        f["relpath_link"] = h5py.ExternalLink(rel_filepath, "/g1/ds")
        try:
            linked_obj = f["missing_link"]
            self.assertTrue(False)
        except KeyError:
            pass  # expected

        linked_obj = f["abspath_link"]
        self.assertTrue(linked_obj.name, "/g1/ds")
        self.assertEqual(linked_obj.shape, (5, 7))
        # The following no longer works for h5py 2.8
        # self.assertEqual(linked_obj.id.id, dset_id)

        linked_obj = f["relpath_link"]
        self.assertTrue(linked_obj.name, "/g1/ds")
        self.assertEqual(linked_obj.shape, (5, 7))
        if h5py.__name__ == "h5pyd":
            self.assertEqual(linked_obj.id.id, dset_id)

        f.close()

    def test_link_removal(self):

        def get_count(grp):
            count = 0
            for item in grp:
                count += 1
            return count
        # create a file for use as a link target
        filename = self.getFileName("test_link_removal")
        print(f"filename: {filename}")

        f = h5py.File(filename, 'w')
        g1 = f.create_group("g1")
        dset = g1.create_dataset('ds', (5, 7), dtype='f4')

        self.assertNotEqual(dset, None)
        self.assertEqual(len(g1), 1)
        self.assertEqual(get_count(g1), 1)

        g1_clone = f["g1"]
        self.assertEqual(len(g1_clone), 1)
        self.assertEqual(get_count(g1_clone), 1)

        del g1["ds"]
        self.assertEqual(len(g1), 0)
        self.assertEqual(get_count(g1), 0)

        self.assertEqual(len(g1_clone), 0)
        self.assertEqual(get_count(g1_clone), 0)

        f.close()


class TestTrackOrder(TestCase):
    titles = ("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")

    def populate(self, g):
        count = len(self.titles)
        for i in range(count):
            # Mix group and dataset creation.
            if i % 2 == 0:
                g.create_group(self.titles[i])
            else:
                g[self.titles[i]] = [i]

    def populate_attrs(self, d):
        count = len(self.titles)
        for i in range(count):
            d.attrs[self.titles[i]] = i

    def test_track_order(self):
        filename = self.getFileName("test_track_order_group")
        print(f"filename: {filename}")
        with h5py.File(filename, 'w') as f:
            g = f.create_group('order', track_order=True)  # creation order
            self.populate(g)

            ref = self.titles
            self.assertEqual(tuple(g), ref)
            i = 0
            for title in g:
                self.assertEqual(title, self.titles[i])
                i += 1

            if h5py.__name__ == "h5pyd":
                # test with get and track_order=False
                g = f.get('order', track_order=False)
                ref = sorted(self.titles)
                self.assertEqual(list(g.keys()), ref)
        # re-opening the file should retain the track_order setting
        with h5py.File(filename) as f:
            g = f['order']
            self.assertEqual(len(g), len(self.titles))
            self.assertEqual(tuple(g), self.titles)
            self.assertEqual(tuple(reversed(g)), tuple(reversed(self.titles)))
            i = 0
            for title in g:
                self.assertEqual(title, self.titles[i])
                i += 1

    def test_track_order_cfg(self):
        filename = self.getFileName("test_track_order_cfg_group")
        print(f"filename: {filename}")
        cfg = h5py.get_config()
        with h5py.File(filename, 'w') as f:
            cfg.track_order = True  # creation order
            g = f.create_group('order')
            cfg.track_order = False  # reset
            self.populate(g)
            self.assertEqual(tuple(g), self.titles)
            i = 0
            for title in g:
                self.assertEqual(title, self.titles[i])
                i += 1

        # re-opening the file should retain the track_order setting
        with h5py.File(filename) as f:
            g = f['order']
            self.assertEqual(len(g), len(self.titles))
            self.assertEqual(tuple(g), self.titles)
            i = 0
            for title in g:
                self.assertEqual(title, self.titles[i])
                i += 1

    def test_no_track_order(self):
        filename = self.getFileName("test_no_track_order_group")
        print(f"filename: {filename}")
        with h5py.File(filename, 'w') as f:
            g = f.create_group('order', track_order=False)  # name alphanumeric
            self.populate(g)
            ref = sorted(self.titles)
            self.assertEqual(list(g), ref)
            self.assertEqual(list(reversed(g)), list(reversed(ref)))

        with h5py.File(filename) as f:
            g = f['order']  # name alphanumeric
            ref = sorted(self.titles)
            self.assertEqual(list(g), ref)
            self.assertEqual(list(reversed(g)), list(reversed(ref)))

    def test_get_dataset_track_order(self):

        filename = self.getFileName("test_get_dataset_track_order")
        print(f"filename: {filename}")
        if h5py.__name__ == "h5py":
            return  # h5py does not support track_order on group.get()

        with h5py.File(filename, 'w') as f:
            g = f.create_group('order')
            dset = g.create_dataset('dset', (10,), dtype='i4')
            dset2 = g.create_dataset('dset2', (10,), dtype='i4')
            self.populate_attrs(dset)
            self.populate_attrs(dset2)

        with h5py.File(filename) as f:
            g = f['order']
            d = g.get('dset', track_order=True)
            self.assertEqual(list(d.attrs), list(self.titles))
            d2 = g.get('dset2', track_order=False)
            ref = sorted(self.titles)
            self.assertEqual(list(d2.attrs), ref)

    def test_get_group_track_order(self):
        # h5py does not support track_order on group.get()
        filename = self.getFileName("test_get_group_track_order")
        print(f"filename: {filename}")
        if h5py.__name__ == "h5py":
            return  # h5py does not support track_order on group.get()

        with h5py.File(filename, 'w') as f:
            g = f.create_group('order')
            g._track_order = True
            # create subgroup and populate it with links
            g.create_group('subgroup')
            self.populate(g['subgroup'])

        with h5py.File(filename) as f:
            g = f['order']
            subg = g.get('subgroup', track_order=True)
            self.assertEqual(tuple(subg), self.titles)

        with h5py.File(filename) as f:
            g = f['order']
            subg2 = g.get('subgroup', track_order=False)
            self.assertEqual(list(subg2), sorted(self.titles))


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
