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

import numpy as np
import logging
import config

if config.get("use_h5py"):
    import h5py
else:
    import h5pyd as h5py

from common import ut, TestCase

# Note: this test currently works with the HSDS server but not h5serv

class TestVlenTypes(TestCase):


    def test_create_vlen_attr(self):
        filename = self.getFileName("create_vlen_attribute")
        print("filename:", filename)
        if config.get("use_h5py"):
            # TBD - skipping as this core dumps in travis for some reason
            return
        f = h5py.File(filename, 'w')
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # vlen ref types not working for h5serv, so abort here
            f.close()
            return

        g1 = f.create_group('g1')
        g1_1 = g1.create_group('g1_1')
        g1_1.attrs["name"] = 'g1_1'
        g1_2 = g1.create_group('g1_2')
        g1_2.attrs["name"] = 'g1_2'
        g1_3 = g1.create_group('g1_3')
        g1_3.attrs["name"] = 'g1_3'

        # create an attribute that is a VLEN int32
        dtvlen = h5py.special_dtype(vlen=np.dtype('int32'))
        e0 = np.array([0,1,2])
        e1 = np.array([0,1,2,3])
        data = np.array([e0, e1], dtype=object)

        g1.attrs.create("a1", data, shape=(2,), dtype=dtvlen)

        ret_val = g1.attrs["a1"]
        self.assertTrue(isinstance(ret_val, np.ndarray))
        self.assertEqual(len(ret_val), 2)
        self.assertTrue(isinstance(ret_val[0], np.ndarray))
        # py36  attribute[a1]: [array([0, 1, 2], dtype=int32) array([0, 1, 2, 3], dtype=int32)]
        # py27  [(0, 1, 2) (0, 1, 2, 3)]
        self.assertEqual(list(ret_val[0]), [0,1,2])
        self.assertEqual(ret_val[0].dtype, np.dtype('int32'))
        self.assertTrue(isinstance(ret_val[1], np.ndarray))
        self.assertEqual(ret_val[1].dtype, np.dtype('int32'))

        self.assertEqual(list(ret_val[1]), [0,1,2,3])

        # create an attribute that is VLEN ObjRef
        dtref = h5py.special_dtype(ref=h5py.Reference)
        dtvlen = h5py.special_dtype(vlen=dtref)
        e0 = np.array((g1_1.ref,), dtype=dtref)
        e1 = np.array((g1_1.ref,g1_2.ref), dtype=dtref)
        e2 = np.array((g1_1.ref,g1_2.ref,g1_3.ref), dtype=dtref)
        data = [e0,e1,e2]

        g1.attrs.create("b1", data, shape=(3,),dtype=dtvlen)

        vlen_val = g1.attrs["b1"]  # read back attribute
        self.assertTrue(isinstance(vlen_val, np.ndarray))
        self.assertEqual(len(vlen_val), 3)
        for i in range(3):
            e = vlen_val[i]
            self.assertTrue(isinstance(e, np.ndarray))
            ref_type = h5py.check_dtype(ref=e.dtype)
            self.assertEqual(ref_type, h5py.Reference)
            # TBD - h5pyd is returning shape of () rather than (1,) for singletons
            if i>0:
                self.assertEqual(e.shape, ((i+1),))
                # first element is always a ref to g1
                refd_group = f[e[0]]
                self.assertEqual(refd_group.attrs['name'], 'g1_1')

        # create an attribute with compound type of vlen objref and int32
        dtcompound = np.dtype([('refs', dtvlen), ('number', 'int32')])
        # create np array with data for the attribute
        # note: two step process is needed, see: https://github.com/h5py/h5py/issues/573
        data = np.zeros((2,), dtype=dtcompound)
        data[0] = (e1, 1)
        data[1] = (e2, 2)

        g1.attrs.create("c1", data, shape=(2,), dtype=dtcompound)
        compound_val = g1.attrs["c1"]
        self.assertTrue(isinstance(compound_val, np.ndarray))
        self.assertEqual(len(compound_val), 2)
        self.assertEqual(len(compound_val.dtype), 2)
        for i in range(2):
            item = compound_val[i]
            self.assertTrue(isinstance(item, np.void))
            self.assertEqual(len(item), 2)
            e = item[0]
            self.assertEqual(len(e), i+2)
            refd_group = f[e[0]]
            self.assertEqual(refd_group.attrs['name'], 'g1_1')
            self.assertEqual(item[1], i+1)

        # close file
        f.close()


    def test_create_vlen_dset(self):
        filename = self.getFileName("create_vlen_dset")
        print("filename:", filename)
        if config.get("use_h5py"):
            # TBD - skipping as this core dumps in travis for some reason
            return
        f = h5py.File(filename, 'w')

        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # vlen ref types not working for h5serv, so abort here
            f.close()
            return

        g1 = f.create_group('g1')
        g1_1 = g1.create_group('g1_1')
        g1_1.attrs["name"] = 'g1_1'
        g1_2 = g1.create_group('g1_2')
        g1_2.attrs["name"] = 'g1_2'
        g1_3 = g1.create_group('g1_3')
        g1_3.attrs["name"] = 'g1_3'

        # create a dataset that is a VLEN int32
        dtvlen = h5py.special_dtype(vlen=np.dtype('uint16'))

        dset1 = f.create_dataset("dset1", shape=(2,), dtype=dtvlen)

        # create numpy object array
        e0 = np.array([1,2,3],dtype='uint16')
        e1 = np.array([1,2,3,4],dtype='uint16')
        data = np.array([e0, e1], dtype=dtvlen)

        # write data
        dset1[...] = data

        # read back data
        e = dset1[0]
        ret_val = dset1[...]
        self.assertTrue(isinstance(ret_val, np.ndarray))
        self.assertEqual(len(ret_val), 2)
        self.assertTrue(isinstance(ret_val[0], np.ndarray))
        # py36  attribute[a1]: [array([0, 1, 2], dtype=int32) array([0, 1, 2, 3], dtype=int32)]
        # py27  [(0, 1, 2) (0, 1, 2, 3)]
        self.assertEqual(list(ret_val[0]), [1,2,3])
        self.assertEqual(ret_val[0].dtype, np.dtype('uint16'))
        self.assertTrue(isinstance(ret_val[1], np.ndarray))
        self.assertEqual(ret_val[1].dtype, np.dtype('uint16'))

        self.assertEqual(list(ret_val[1]), [1,2,3,4])

        # Read back just one element
        e0 = dset1[0]
        self.assertEqual(len(e0), 3)
        self.assertEqual(list(e0), [1,2,3])
        
        # try writing int arrays into dataset
        data = [42,]  
        dset1[0] = data
        ret_val = dset1[...]
        self.assertEqual(list(ret_val[0]), [42])


        # TBD: Test for VLEN objref and comount as with attribute test above

        # close file
        f.close()

    def test_create_vlen_compound_dset(self):
        filename = self.getFileName("create_vlen_compound_dset")
        print("filename:", filename)
        f = h5py.File(filename, "w")

        count = 10
         # create a dataset that is a VLEN int32
        dtvlen = h5py.special_dtype(vlen=np.dtype('int32'))
        dt = np.dtype([('x', np.int32), ('vals', dtvlen)])
        dset = f.create_dataset('compound_vlen', (count,), dtype=dt)

        elem = dset[0]
        for i in range(count):
            elem['x'] = i
            elem['vals'] = np.array(list(range(i+1)), dtype=np.int32)
            dset[i] = elem

        e = dset[5]
        self.assertEqual(len(e), 2)
        self.assertEqual(e[0], 5)
        e1 = list(e[1])
        self.assertEqual(e1, list(range(6)))
        
        f.close()

    def test_create_vlen_2d_dset(self):
        filename = self.getFileName("create_vlen_2d_dset")
        print("filename:", filename)
        if config.get("use_h5py"):
            # TBD - skipping as this core dumps in travis for some reason
            return
        f = h5py.File(filename, 'w')

        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # vlen ref types not working for h5serv, so abort here
            f.close()
            return

        # create a dataset that is a VLEN int32
        dtvlen = h5py.special_dtype(vlen=np.dtype('int32'))

        nrows = 2
        ncols = 3
        dset1 = f.create_dataset("dset1", shape=(nrows,ncols), dtype=dtvlen)

        # create numpy object array
        data = np.zeros((nrows,ncols), dtype=dtvlen)
        for i in range(nrows):
            for j in range(ncols):
                alist = []
                for k in range((i+1)*(j+1)):
                    alist.append(k)
                data[i,j] = np.array(alist, dtype="int32")

        # write data
        dset1[...] = data

        # read back data
        ret_val = dset1[...]
        self.assertTrue(isinstance(ret_val, np.ndarray))
        self.assertEqual(ret_val.shape, (nrows, ncols))
        e12 = ret_val[1,2]
        self.assertTrue(isinstance(e12, np.ndarray))
        # py36  attribute[a1]: [array([0, 1, 2], dtype=int32) array([0, 1, 2, 3], dtype=int32)]
        # py27  [(0, 1, 2) (0, 1, 2, 3)]
        self.assertEqual(list(e12), [0,1,2,3,4,5])
        self.assertEqual(e12.dtype, np.dtype('int32'))

        # Read back just one element
        e12 = dset1[1,2]
        self.assertTrue(isinstance(e12, np.ndarray))
        self.assertEqual(e12.shape, (6,))
        # py36  attribute[a1]: [array([0, 1, 2], dtype=int32) array([0, 1, 2, 3], dtype=int32)]
        # py27  [(0, 1, 2) (0, 1, 2, 3)]
        self.assertEqual(list(e12), [0,1,2,3,4,5])
        self.assertEqual(e12.dtype, np.dtype('int32'))

        # close file
        f.close()


    def test_variable_len_str_attr(self):
        filename = self.getFileName("variable_len_str_dset")
        print("filename:", filename)
        if config.get("use_h5py"):
            # TBD - skipping as this core dumps in travis for some reason
            return
        f = h5py.File(filename, "w")
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # vlen ref types not working for h5serv, so abort here
            f.close()
            return

        words = (b"one", b"two", b"three", b"four", b"five", b"six", b"seven", b"eight", b"nine", b"ten")

        dims = (10,)
        dt = h5py.special_dtype(vlen=bytes)
        f.attrs.create('a1', words, shape=dims, dtype=dt)


        vals = f.attrs["a1"]  # read back

        self.assertTrue("vlen" in vals.dtype.metadata)

        for i in range(10):
            self.assertEqual(vals[i], words[i])

        f.close()


    def test_variable_len_str_dset(self):
        filename = self.getFileName("variable_len_str_dset")
        print("filename:", filename)
        if config.get("use_h5py"):
            # TBD - skipping as this core dumps in travis for some reason
            return
        f = h5py.File(filename, "w")
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            # vlen  types not working for h5serv, so abort here
            f.close()
            return

        dims = (10,)
        dt = h5py.special_dtype(vlen=bytes)
        dset = f.create_dataset('variable_len_str_dset', dims, dtype=dt)

        self.assertEqual(dset.name, "/variable_len_str_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 10)
        self.assertEqual(str(dset.dtype), 'object')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 10)
        if config.get('use_h5py'):
            self.assertEqual(dset.fillvalue, None)
        else:
            self.assertEqual(dset.fillvalue, 0)

        self.assertEqual(dset[0], b'')

        words = (b"one", b"two", b"three", b"four", b"five", b"six", b"seven", b"eight", b"nine", b"ten")
        dset[:] = words
        vals = dset[:]  # read back

        self.assertTrue("vlen" in vals.dtype.metadata)

        for i in range(10):
            self.assertEqual(vals[i], words[i])

        f.close()

    def test_variable_len_unicode_dset(self):
        filename = self.getFileName("variable_len_unicode_dset")
        print("filename:", filename)
        """
        if config.get("use_h5py"):
            # TBD - skipping as this core dumps in travis for some reason
            return
        """
        f = h5py.File(filename, "w")
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            f.close()
            return  # variable len types not working with h5serv

        dims = (10,)
        dt = h5py.special_dtype(vlen=str)

        dset = f.create_dataset('variable_len_unicode_dset', dims, dtype=dt)

        self.assertEqual(dset.name, "/variable_len_unicode_dset")
        self.assertTrue(isinstance(dset.shape, tuple))
        self.assertEqual(len(dset.shape), 1)
        self.assertEqual(dset.shape[0], 10)
        self.assertEqual(str(dset.dtype), 'object')
        self.assertTrue(isinstance(dset.maxshape, tuple))
        self.assertEqual(len(dset.maxshape), 1)
        self.assertEqual(dset.maxshape[0], 10)
        if config.get('use_h5py'):
            self.assertEqual(dset.fillvalue, None)
        else:
            self.assertEqual(dset.fillvalue, 0)

        self.assertEqual(dset[0], b'')

        words = (u"one: \u4e00", u"two: \u4e8c", u"three: \u4e09", u"four: \u56db", u"five: \u4e94", u"six: \u516d", u"seven: \u4e03", u"eight: \u516b", u"nine: \u4e5d", u"ten: \u5341")
        dset[:] = words
        vals = dset[:]  # read back

        self.assertTrue("vlen" in vals.dtype.metadata)

        for i in range(10):
            word = words[i].encode("utf-8")
            self.assertEqual(vals[i], word)

        f.close()

    def test_variable_len_unicode_attr(self):
        filename = self.getFileName("variable_len_unicode_attr")
        print("filename:", filename)
        f = h5py.File(filename, "w")
        if isinstance(f.id.id, str) and not f.id.id.startswith("g-"):
            f.close()
            return  # variable len types not working with h5serv

        dims = (10,)
        dt = h5py.special_dtype(vlen=str)

        words = (u"one: \u4e00", u"two: \u4e8c", u"three: \u4e09", u"four: \u56db", u"five: \u4e94", u"six: \u516d", u"seven: \u4e03", u"eight: \u516b", u"nine: \u4e5d", u"ten: \u5341")

        f.attrs.create('a1', words, shape=dims, dtype=dt)

        vals = f.attrs["a1"]  # read back
        #print("type:", type(vals))
        self.assertTrue("vlen" in vals.dtype.metadata)

        for i in range(10):
            self.assertEqual(vals[i], words[i])
            self.assertEqual(type(vals[i]), str)


        f.close()


if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()
