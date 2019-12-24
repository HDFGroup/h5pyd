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
import unittest
import logging
import numpy as np
from h5type import special_dtype
from h5type import check_dtype
from base import Reference
import h5type


class H5TypeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(H5TypeTest, self).__init__(*args, **kwargs)
        # main
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def testBaseIntegerTypeItem(self):
        dt = np.dtype('<i1')
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_INTEGER')
        self.assertEqual(typeItem['base'], 'H5T_STD_I8LE')
        typeItem = h5type.getTypeResponse(typeItem) # non-verbose format
        self.assertEqual(typeItem['class'], 'H5T_INTEGER')
        self.assertEqual(typeItem['base'], 'H5T_STD_I8LE')


    def testBaseFloatTypeItem(self):
        dt = np.dtype('<f8')
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_FLOAT')
        self.assertEqual(typeItem['base'], 'H5T_IEEE_F64LE')
        typeItem = h5type.getTypeResponse(typeItem) # non-verbose format
        self.assertEqual(typeItem['class'], 'H5T_FLOAT')
        self.assertEqual(typeItem['base'], 'H5T_IEEE_F64LE')

    def testBaseStringTypeItem(self):
        dt = np.dtype('S3')
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_STRING')
        self.assertEqual(typeItem['length'], 3)
        self.assertEqual(typeItem['strPad'], 'H5T_STR_NULLPAD')
        self.assertEqual(typeItem['charSet'], 'H5T_CSET_ASCII')

    def testBaseStringUTFTypeItem(self):
        dt = np.dtype('U3')
        try:
            typeItem = h5type.getTypeItem(dt)
            self.assertTrue(False)  # expected exception
        except TypeError:
            pass # expected

    def testBaseVLenAsciiTypeItem(self):
        dt = special_dtype(vlen=bytes)
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_STRING')
        self.assertEqual(typeItem['length'], 'H5T_VARIABLE')
        self.assertEqual(typeItem['strPad'], 'H5T_STR_NULLTERM')
        self.assertEqual(typeItem['charSet'], 'H5T_CSET_ASCII')

    def testBaseVLenUnicodeTypeItem(self):
        dt = special_dtype(vlen=str)
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_STRING')
        self.assertEqual(typeItem['length'], 'H5T_VARIABLE')
        self.assertEqual(typeItem['strPad'], 'H5T_STR_NULLTERM')
        self.assertEqual(typeItem['charSet'], 'H5T_CSET_UTF8')

    def testBaseEnumTypeItem(self):
        mapping = {'RED': 0, 'GREEN': 1, 'BLUE': 2}
        dt = special_dtype(enum=(np.int8, mapping))
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_ENUM')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['class'], 'H5T_INTEGER')
        self.assertEqual(baseItem['base'], 'H5T_STD_I8LE')
        self.assertTrue('mapping' in typeItem)
        self.assertEqual(typeItem['mapping']['GREEN'], 1)

    def testBaseArrayTypeItem(self):
        dt = np.dtype('(2,2)<int32')
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_ARRAY')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['class'], 'H5T_INTEGER')
        self.assertEqual(baseItem['base'], 'H5T_STD_I32LE')

    def testCompoundArrayTypeItem(self):
        dt = np.dtype([('a', '<i1'), ('b', 'S1', (10,))])
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        fields = typeItem['fields']
        field_a = fields[0]
        self.assertEqual(field_a['name'], 'a')
        field_a_type = field_a['type']
        self.assertEqual(field_a_type['class'], 'H5T_INTEGER')
        self.assertEqual(field_a_type['base'], 'H5T_STD_I8LE')
        field_b = fields[1]
        self.assertEqual(field_b['name'], 'b')
        field_b_type = field_b['type']
        self.assertEqual(field_b_type['class'], 'H5T_ARRAY')
        self.assertEqual(field_b_type['dims'], (10,))
        field_b_basetype = field_b_type['base']
        self.assertEqual(field_b_basetype['class'], 'H5T_STRING')


    def testOpaqueTypeItem(self):
        dt = np.dtype('V200')
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_OPAQUE')
        self.assertTrue('base' not in typeItem)

    def testVlenDataItem(self):
        dt = special_dtype(vlen=np.dtype('int32'))
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_VLEN')
        self.assertEqual(typeItem['size'], 'H5T_VARIABLE')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['base'], 'H5T_STD_I32LE')

    def testCompoundTypeItem(self):
        dt = np.dtype([("temp", np.float32), ("pressure", np.float32), ("wind", np.int16)])
        typeItem = h5type.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        self.assertTrue('fields' in typeItem)
        fields = typeItem['fields']
        self.assertEqual(len(fields), 3)
        tempField = fields[0]
        self.assertEqual(tempField['name'], 'temp')
        self.assertTrue('type' in tempField)
        tempFieldType = tempField['type']
        self.assertEqual(tempFieldType['class'], 'H5T_FLOAT')
        self.assertEqual(tempFieldType['base'], 'H5T_IEEE_F32LE')

        typeItem = h5type.getTypeResponse(typeItem) # non-verbose format
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        self.assertTrue('fields' in typeItem)
        fields = typeItem['fields']
        self.assertEqual(len(fields), 3)
        tempField = fields[0]
        self.assertEqual(tempField['name'], 'temp')
        self.assertTrue('type' in tempField)
        tempFieldType = tempField['type']
        self.assertEqual(tempFieldType['class'], 'H5T_FLOAT')
        self.assertEqual(tempFieldType['base'], 'H5T_IEEE_F32LE')

    def testCreateBaseType(self):
        dt = h5type.createDataType('H5T_STD_U32BE')
        self.assertEqual(dt.name, 'uint32')
        self.assertEqual(dt.byteorder, '>')
        self.assertEqual(dt.kind, 'u')

        dt = h5type.createDataType('H5T_STD_I16LE')
        self.assertEqual(dt.name, 'int16')
        self.assertEqual(dt.kind, 'i')

        dt = h5type.createDataType('H5T_IEEE_F64LE')
        self.assertEqual(dt.name, 'float64')
        self.assertEqual(dt.kind, 'f')

        dt = h5type.createDataType('H5T_IEEE_F32LE')
        self.assertEqual(dt.name, 'float32')
        self.assertEqual(dt.kind, 'f')

        typeItem = { 'class': 'H5T_INTEGER', 'base': 'H5T_STD_I32BE' }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'int32')
        self.assertEqual(dt.kind, 'i')

    def testCreateBaseStringType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_ASCII', 'length': 6 }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'bytes48')
        self.assertEqual(dt.kind, 'S')

    def testCreateBaseUnicodeType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_UTF8', 'length': 32 }
        try:
            dt = h5type.createDataType(typeItem)
            self.assertTrue(False)  # expected exception
        except TypeError:
            pass

    def testCreateNullTermStringType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_ASCII',
            'length': 6, 'strPad': 'H5T_STR_NULLTERM'}
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'bytes48')
        self.assertEqual(dt.kind, 'S')


    def testCreateVLenStringType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_ASCII', 'length': 'H5T_VARIABLE' }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'object')
        self.assertEqual(dt.kind, 'O')
        self.assertEqual(check_dtype(vlen=dt), bytes)


    def testCreateVLenUTF8Type(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_UTF8', 'length': 'H5T_VARIABLE' }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'object')
        self.assertEqual(dt.kind, 'O')
        self.assertEqual(check_dtype(vlen=dt), str)

    def testCreateVLenDataType(self):
        typeItem = {'class': 'H5T_VLEN', 'base': 'H5T_STD_I32BE'}
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'object')
        self.assertEqual(dt.kind, 'O')

    def testCreateOpaqueType(self):
        typeItem = {'class': 'H5T_OPAQUE', 'size': 200}
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'void1600')
        self.assertEqual(dt.kind, 'V')

    def testCreateCompoundType(self):
        typeItem = {
            'class': 'H5T_COMPOUND', 'fields':
                [{'name': 'temp',     'type': 'H5T_IEEE_F32LE'},
                 {'name': 'pressure', 'type': 'H5T_IEEE_F32LE'},
                 {'name': 'location', 'type': {
                     'length': 'H5T_VARIABLE',
                     'charSet': 'H5T_CSET_ASCII',
                     'class': 'H5T_STRING',
                     'strPad': 'H5T_STR_NULLTERM'}},
                 {'name': 'wind',     'type': 'H5T_STD_I16LE'}]
        }

        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'void144')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(len(dt.fields), 4)
        dtLocation = dt[2]
        self.assertEqual(dtLocation.name, 'object')
        self.assertEqual(dtLocation.kind, 'O')
        self.assertEqual(check_dtype(vlen=dtLocation), bytes)

    def testCreateCompoundTypeUnicodeFields(self):
        typeItem = {
            'class': 'H5T_COMPOUND', 'fields':
                [{'name': u'temp',     'type': 'H5T_IEEE_F32LE'},
                 {'name': u'pressure', 'type': 'H5T_IEEE_F32LE'},
                 {'name': u'wind',     'type': 'H5T_STD_I16LE'}]
        }

        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'void80')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(len(dt.fields), 3)

    def testCreateArrayType(self):
        typeItem = {'class': 'H5T_ARRAY',
                    'base': 'H5T_STD_I64LE',
                    'dims': (3, 5) }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'void960')
        self.assertEqual(dt.kind, 'V')

    def testCreateArrayIntegerType(self):
        typeItem = {'class': 'H5T_INTEGER',
                    'base': 'H5T_STD_I64LE',
                    'dims': (3, 5) }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(dt.name, 'void960')
        self.assertEqual(dt.kind, 'V')

    def testCreateCompoundArrayType(self):
        typeItem = {
            "class": "H5T_COMPOUND",
            "fields": [
                {
                    "type": {
                        "base": "H5T_STD_I8LE",
                        "class": "H5T_INTEGER"
                    },
                    "name": "a"
                },
                {
                    "type": {
                        "dims": [
                            10
                        ],
                        "base": {
                            "length": 1,
                            "charSet": "H5T_CSET_ASCII",
                            "class": "H5T_STRING",
                            "strPad": "H5T_STR_NULLPAD"
                        },
                    "class": "H5T_ARRAY"
                },
                "name": "b"
                }
            ]
        }
        dt = h5type.createDataType(typeItem)
        self.assertEqual(len(dt.fields), 2)
        self.assertTrue('a' in dt.fields.keys())
        self.assertTrue('b' in dt.fields.keys())

    def testRefType(self):
        # todo - special_dtype not implemented
        dt = special_dtype(ref=Reference)
        self.assertEqual(dt.kind, 'S')
        self.assertTrue(dt.metadata['ref'] is Reference)

        reftype = check_dtype(ref=dt)
        self.assertTrue(reftype is Reference)


if __name__ == '__main__':
    #setup test files

    unittest.main()
