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

from __future__ import absolute_import

import os
import os.path as op
import json
import logging
from .httpconn import HttpConn
from .config import Config


class Folder():

    """
        Represents a folder of domains
    """

    @property
    def domain(self):
        domain = self._domain
        if domain is None:
            domain = ''

        r = domain + '/'

        return r

    @property
    def parent(self):
        if self._domain is None:
            return None
        else:
            return op.dirname(self._domain) + '/'


    @property
    def modified(self):
        """Last modified time of the domain as a datetime object."""
        return self._modified

    @property
    def created(self):
        """creation time of the domain as a datetime object."""
        return self._created

    @property
    def owner(self):
        """Username of the owner of the folder """
        return self._owner

    @property
    def is_folder(self):
        """ is this a proper folder (i.e. domain without root group),
            or a domain """
        if self._obj_class == "folder":
            return True
        else:
            return False


    def __init__(self, domain_name, pattern=None, query=None, mode=None, endpoint=None,
        username=None, password=None, bucket=None, api_key=None, logger=None, owner=None, batch_size=1000, retries=3, **kwds):
        """Create a new Folders object.

        domain_name
            URI of the domain name to access. E.g.: /org/hdfgroup/folder/
        pattern
            Regex filter to apply to any domains in the folder
        query
            Query for root level attributes - only domains matching the query will be returned
        mode
            Access mode: 'r', 'r+', 'w', or 'a'
        endpoint
            Server endpoint.   Defaults to "http://localhost:5000"
        username
            username for authentication
        password
            password for authentication
        bucket
            bucket (or storage container) to use for domain.  If not set, server default bucket will be used
        api_key
            user's api key (for server configurations that use api_key rather than username/password)
        logger
            supply log handler to be used
        owner
            set the owner to be used when new domain is created (defaults to username).  Only valid when used
            by admin users
        linked_domain
            Create new domain using the root of the linked domain
        retries
            Number of retry attempts to be used if a server request fails

        """

        self.log = logging.getLogger("h5pyd")

        if len(domain_name) == 0:
            raise ValueError("Invalid folder name")

        if domain_name[0] != '/':
            raise ValueError("Folder name must start with '/'")

        if domain_name[-1] != '/':
            raise ValueError("Folder name must end with '/'")

        if mode and mode not in ('r', 'r+', 'w', 'w-', 'x', 'a'):
            raise ValueError("Invalid mode; must be one of r, r+, w, w-, x, a")

        self._pattern = pattern
        self._query = query

        if mode is None:
            mode = 'r'

        cfg = Config()  # pulls in state from a .hscfg file (if found).

        if endpoint is None and "hs_endpoint" in cfg:
            endpoint = cfg["hs_endpoint"]

        if username is None and "hs_username" in cfg:
            username = cfg["hs_username"]

        if password is None and "hs_password" in cfg:
            password = cfg["hs_password"]

        if bucket is None and "hs_bucket" in cfg:
            bucket = cfg["hs_bucket"]

        if len(domain_name) <= 1:
            self._domain = None
        else:
            self._domain = domain_name[:-1]
        self._subdomains = None
        self._subdomain_marker = None
        self._batch_size = batch_size

        if api_key is None:
            if "HS_API_KEY" in os.environ:
                api_key = os.environ["HS_API_KEY"]
            elif "hs_api_key" in cfg:
                api_key = cfg["hs_api_key"]
        if not api_key:
            # if Azure AD ids are set, pass them to HttpConn via api_key dict

            ad_app_id = None  # Azure AD HSDS Server id
            if "HS_AD_APP_ID" in os.environ:
                ad_app_id = os.environ["HS_AD_APP_ID"]
            elif "hs_ad_app_id" in cfg:
                ad_app_id = cfg["hs_ad_app_id"]

            ad_tenant_id = None # Azure AD tenant id
            if "HS_AD_TENANT_ID" in os.environ:
                ad_tenant_id = os.environ["HS_AD_TENANT_ID"]
            elif "hs_ad_tenant_id" in cfg:
                ad_tenant_id = cfg["hs_ad_tenant_id"]

            ad_resource_id = None # Azure AD resource id
            if "HS_AD_RESOURCE_ID" in os.environ:
                ad_resource_id = os.environ["HS_AD_RESOURCE_ID"]
            elif "hs_ad_resource_id" in cfg:
                ad_resource_id = cfg["hs_ad_resource_id"]

            ad_client_secret = None # Azure client secret
            if "HS_AD_CLIENT_SECRET" in os.environ:
                ad_client_secret = os.environ["HS_AD_CLIENT_SECRET"]
            elif "hs_ad_client_secret" in cfg:
                ad_client_secret = cfg["hs_ad_client_secret"]

            if ad_app_id and ad_tenant_id and ad_resource_id:
                # contruct dict to pass to HttpConn
                api_key = {"AD_APP_ID": ad_app_id, "AD_TENANT_ID": ad_tenant_id, "AD_RESOURCE_ID": ad_resource_id}
                if ad_client_secret:
                        api_key["AD_CLIENT_SECRET"] = ad_client_secret

        self._http_conn = HttpConn(self._domain, endpoint=endpoint, username=username,
            password=password, bucket=bucket, api_key=api_key, mode=mode, logger=logger, retries=retries)
        self.log = self._http_conn.logging

        domain_json = None

        # try to do a GET from the domain
        if domain_name == '/':
            req = "/domains"
        else:
            req = '/'

        rsp = self._http_conn.GET(req)

        if rsp.status_code in (404, 410) and mode in ('w', 'w-', 'x'):
            # create folder
            body = {"folder": True}
            if owner:
                body["owner"] = owner
            rsp = self._http_conn.PUT(req, body=body)
            if rsp.status_code != 201:
                self._http_conn.close()
                raise IOError(rsp.status_code, rsp.reason)
        elif rsp.status_code != 200:
            # folder must exist
            if rsp.status_code < 500:
                self.log.warning("status_code: {}".format(rsp.status_code))
            else:
                self.log.error("status_code: {}".format(rsp.status_code))
            raise IOError(rsp.status_code, rsp.reason)
        domain_json = json.loads(rsp.text)
        self.log.info("domain_json: {}".format(domain_json))
        if "class" in domain_json:
            if domain_json["class"] != "folder":
                self.log.warning("Not a folder domain")
            self._obj_class = domain_json["class"]
        elif "root" in domain_json:
            # open with Folder but actually has a root group
            self._obj_class = "domain"
        else:
            self._obj_class = "folder"
        self._name = domain_name
        if "created" in domain_json:
            self._created = domain_json['created']
        else:
            self._created = None
        if "lastModified" in domain_json:
            self._modified = domain_json['lastModified']
        else:
            self._modified = None
        if "owner" in domain_json:
            self._owner = domain_json["owner"]
        else:
            self._owner = None

    def getACL(self, username):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        req = '/acls/' + username
        rsp = self._http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.reason)
        rsp_json = json.loads(rsp.text)
        acl_json = rsp_json["acl"]
        return acl_json

    def getACLs(self):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        req = '/acls'
        rsp = self._http_conn.GET(req)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = json.loads(rsp.text)
        acls_json = rsp_json["acls"]
        return acls_json

    def putACL(self, acl):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._http_conn.mode == 'r':
            raise IOError(400, "folder is open as read-onnly")
        if "userName" not in acl:
            raise IOError(404, "ACL has no 'userName' key")
        perm = {}
        for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
            if k not in acl:
                raise IOError(404, "Missing ACL field: {}".format(k))
            perm[k] = acl[k]

        req = '/acls/' + acl['userName']
        rsp = self._http_conn.PUT(req, body=perm)
        if rsp.status_code != 201:
            raise IOError(rsp.status_code, rsp.reason)


    def _getSubdomains(self):
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._subdomains is not None and not self._subdomain_marker:
            # we've got all the subdomains, return 0 to indicate none were fetched
            return 0
        req = '/domains'
        if self._domain is None:
            params = {"domain": '/'}
        else:
            params = {"domain": self._domain + '/'}
        params["verbose"] = 1  # to get lastModified
        if not self._query:
            params["Limit"] = self._batch_size  # get 100 at a time
        if self._pattern:
            params["pattern"] = self._pattern
        if self._query:
            params["query"] = self._query
        if self._subdomain_marker:
            params["Marker"] = self._subdomain_marker
        rsp = self._http_conn.GET(req, params=params)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        rsp_json = json.loads(rsp.text)
        if "domains" not in rsp_json:
            raise IOError(500, "Unexpected Error")
        domains = rsp_json["domains"]
        count = len(domains)
        if self._subdomains is None:
            # setting to an empty list signifies we've done at least one request
            self._subdomains = []
        # append to what we have
        self._subdomains.extend(domains)
        if len(domains) == self._batch_size:
            # save the marker for the next batch
            self._subdomain_marker = domains[-1]["name"]
        else:
            self._subdomain_marker = None  # we got all the domains
        return count


    def close(self):
        """ Clears reference to remote resource.
        """
        self._domain = None
        self._http_conn.close()
        self._http_conn = None

    def __getitem__(self, name):
        """ Get a domain  """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._subdomains is None:
            self._getSubdomains()
        while True:
            domains = self._subdomains
            for domain in domains:
                if op.basename(domain["name"]) == name:
                    return domain
            # see if we can fetch more domains
            count = self._getSubdomains()
            if count == 0:
                break
        return None

    def delete_item(self, name, keep_root=False):
        """ delete domain """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        if self._http_conn.mode == 'r':
            raise IOError(400, "folder is open as read-onnly")
        if self._domain:
            domain = self._domain + '/' + name
        else:
            domain = '/' + name  # top-level delete
        headers = self._http_conn.getHeaders()
        req = '/'
        params = {"domain": domain}
        if keep_root:
            params["keep_root"] = 1
        rsp = self._http_conn.DELETE(req, headers=headers, params=params)
        if rsp.status_code != 200:
            raise IOError(rsp.status_code, rsp.reason)
        self._subdomains = None # reset the cache list
        self._subdomain_marker = None

    def __delitem__(self, name):
        """ Delete domain. """
        self.delete_item(name)

    def __len__(self):
        """ Number of subdomains of this folder """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        count = 1
        while count > 0:
            # keep grabbing subdomains till there are no more to fetch
            count = self._getSubdomains()
        return len(self._subdomains)


    def __iter__(self):
        """ Iterate over subdomain names """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        self._getSubdomains()
        index = 0
        while True:
            if len(self._subdomains) == index:
                # get more if we can
                count = self._getSubdomains()
                if count == 0:
                    break
            domain = self._subdomains[index]
            index += 1
            yield op.basename(domain['name'])


    def __contains__(self, name):
        """ Test if a member name exists """
        if self._http_conn is None:
            raise IOError(400, "folder is not open")
        self._getSubdomains()
        index = 0
        found = False
        while True:
            if len(self._subdomains) == index:
                # get more if we can
                count = self._getSubdomains()
                if count == 0:
                    break
            domain = self._subdomains[index]
            index += 1
            if op.basename(domain['name']) == name:
                found = True
                break
        return found


    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __repr__(self):

        return self.domain
