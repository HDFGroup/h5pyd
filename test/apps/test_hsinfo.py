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

import sys
import subprocess
import logging
from common import ut, TestCase


class TestHsinfo(TestCase):


    def test_help(self):

        arg = "-h"
        result = subprocess.run(["hsinfo", "-h"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertTrue(len(result.stdout) > 400)
        self.assertFalse(result.stderr)

if __name__ == '__main__':
    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    ut.main()

