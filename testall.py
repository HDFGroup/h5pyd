#!/usr/bin/env python
##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including       s   #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

import os
import sys

hl_tests = ('test_attribute',
            'test_committedtype',
            'test_complex_numbers',
            'test_dataset_compound',
            'test_dataset_create',
            'test_dataset_extend',
            'test_dataset_fancyselect',
            'test_dataset_objref',
            'test_dataset_getitem',
            'test_dataset_pointselect',
            'test_dataset_scalar',
            'test_dataset_setitem',
            'test_dimscale',
            'test_file',
            'test_group',
            'test_table',
            'test_visit',
            'test_vlentype',
            'test_folder')

app_tests = ('test_hsinfo', 'test_tall_inspect', 'test_diamond_inspect',
             'test_shuffle_inspect')

run_hl = True
run_app = True
if len(sys.argv) > 1:
    if sys.argv[1] in ("-h", "--help"):
        print("Usage: python testall.py [--hl_only | --app_only]")
        sys.exit(0)
    if sys.argv[1] == "--hl_only":
        run_app = False
    elif sys.argv[1] == "--app_only":
        run_hl = False
    else:
        print("Unexpected argument")
        sys.exit(1)

#
# Run tests
#
os.chdir('test')
if run_hl:
    os.chdir('hl')
    for test_name in hl_tests:
        print(test_name)
        rc = os.system('python ' + test_name + '.py')
        if rc != 0:
            sys.exit("Failed")
    os.chdir('..')
if run_app:
    os.chdir('apps')
    rc = os.system('python is_hsds.py')
    if rc == 0:
        print("running HSDS app tests")
        # these tests are only support with HSDS
        rc = os.system('python load_files.py')
        if rc != 0:
            sys.exit("load_files.py failed")
        for test_name in app_tests:
            print(test_name)
            rc = os.system('python ' + test_name + '.py')
            if rc != 0:
                sys.exit("Failed")

os.chdir('..')

print("Done!")
