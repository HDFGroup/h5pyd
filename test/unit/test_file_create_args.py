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
""" File() passes owner and linked_domain on to the domain create request """

import json
import unittest

from stubs import StubServer, scratch_home

from h5json.objid import createObjId

import h5pyd

DOMAIN = "/home/test_user1/new.h5"
LINKED = "/home/test_user1/old.h5"


class TestFileCreateArgs(unittest.TestCase):

    def setUp(self):
        self.root_id = createObjId("groups")
        self.domain_created = False

    def _handler(self, method, path, params, body):
        root_json = {"id": self.root_id, "created": 1.0, "lastModified": 1.0,
                     "links": {}, "attributes": {}}
        if path == "/about":
            return 200, {"state": "READY", "hsds_version": "1.0.3"}
        if path == "/" and method == "GET":
            if params.get("domain") == DOMAIN and self.domain_created:
                return 200, {"root": self.root_id, "owner": "test_user1"}
            return 404, None
        if path == "/" and method == "PUT":
            self.domain_created = True
            return 201, {"root": self.root_id, "owner": "test_user1"}
        if path == f"/groups/{self.root_id}" and method == "GET":
            return 200, root_json
        return 200, {}

    def _create_body(self, **kwargs):
        with scratch_home():
            with StubServer(self._handler) as srv:
                f = h5pyd.File(DOMAIN, "w", endpoint=srv.endpoint, username="u", password="p",
                               retries=0, **kwargs)
                f.close()
        puts = [r for r in srv.requests if r[0] == "PUT" and r[1] == "/" and r[3]]
        self.assertEqual(len(puts), 1)
        return json.loads(puts[0][3])

    def test_owner_and_linked_domain_sent(self):
        body = self._create_body(owner="someone", linked_domain=LINKED)
        self.assertEqual(body.get("owner"), "someone")
        self.assertEqual(body.get("linked_domain"), LINKED)

    def test_defaults_not_sent(self):
        body = self._create_body()
        self.assertNotIn("owner", body)
        self.assertNotIn("linked_domain", body)


if __name__ == "__main__":
    unittest.main()
