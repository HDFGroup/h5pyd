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
""" Folder.delete_item refuses names that would reach outside the folder """

import logging
import unittest

from stubs import StubServer, scratch_home

import h5pyd

FOLDER = "/home/u/f/"


class TestFolderNames(unittest.TestCase):

    def _serve(self, domains_handler):
        def handler(method, path, params, body):
            if path == "/about":
                return 200, {"state": "READY"}
            if path == "/" and method == "GET":
                return 200, {"class": "folder", "owner": "u"}
            if path == "/domains":
                return domains_handler(params)
            return 200, {}
        return StubServer(handler)

    def _folder(self, srv, **kwargs):
        return h5pyd.Folder(FOLDER, endpoint=srv.endpoint, username="u", password="p",
                            retries=0, **kwargs)

    def test_delete_item_rejects_dot_segments(self):
        with scratch_home(), self._serve(lambda params: (200, {"domains": []})) as srv:
            folder = self._folder(srv, mode="a")
            for name in ("../x.h5", "a/../../x.h5", ".", "..", "", "a//b.h5", "sub/./x.h5", "x.h5/"):
                with self.assertRaises(ValueError, msg=name):
                    folder.delete_item(name)
            # nested names are still allowed
            folder.delete_item("sub/x.h5")
            folder.close()
        deletes = [r for r in srv.requests if r[0] == "DELETE"]
        self.assertEqual(len(deletes), 1)
        self.assertEqual(deletes[0][2]["domain"], "/home/u/f/sub/x.h5")


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    unittest.main()
