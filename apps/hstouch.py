import numpy as np
import sys
import os.path as op
import os
import logging
from datetime import datetime
import h5pyd as h5py
from config import Config

#
# Create domain or update timestamp if domain already exist
#

verbose = False
showacls = False
 
cfg = Config()

def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    #print("getFolder", domain)
    dir = h5py.Folder(domain, endpoint=endpoint, username=username, password=password)
    return dir

def getFile(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    #print("getFile", domain)
    fh = h5py.File(domain, mode='r', endpoint=endpoint, username=username, password=password)
    return fh

def createFile(domain):
    #print("createFile", domain)
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    fh = h5py.File(domain, mode='x', endpoint=endpoint, username=username, password=password)
    return fh


   
def touchDomain(domain):

    # get handle to parent folder
    parent_domain = op.dirname(domain) 

    if len(parent_domain) < 2:
        sys.exit("can't create top-level domain")

    if not parent_domain.endswith('/'):
        parent_domain += '/'
    try:
        hparent = getFolder(parent_domain)
    except OSError as oe:
        #print("errno:", oe.errno)
        if oe.errno in (404, 410):   # Not Found
            sys.exit("Parent domain: {} not found".format(parent_domain))
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        else:
            sys.exit("Unexpected error: {}".format(oe))
    
    hdomain = None
    try:
        hdomain = getFile(domain)
    except OSError as oe:
        #print("errno:", oe.errno)
        if oe.errno in (404, 410):   # Not Found
            pass  # domain not found
        else:
            sys.exit("Unexpected error: {}".format(oe))

    if hdomain:
        try:
            r = hdomain['/']
            # create/update attribute to update lastModified timestamp of domain
            r.attrs["hstouch"] = 1
            hdomain.close()
        except OSError as oe:
            sys.exit("Got error updating domain: {}".format(oe))
    else:
        # create domain
        try:
            fh = createFile(domain)
            #print("domain created", fh.id.id)
        except OSError as oe:
            sys.exit("Got error updating domain: {}".format(oe))


#
# Usage
#
def printUsage():
    print("usage: python hstouch.py [-v] [-e endpoint] [-u username] [-p password] [--loglevel debug|info|warning|error] [--logfile <logfile>] domains")
    print("example: python hstouch.py -e http://data.hdfgroup.org:7253 /hdfgroup/data/test/emptydomain.h5")
    sys.exit()

#
# Main
#

domains = []
argn = 1
depth = 2
loglevel = logging.ERROR
logfname=None

while argn < len(sys.argv):
    arg = sys.argv[argn]
    val = None
    if len(sys.argv) > argn + 1:
        val = sys.argv[argn+1]
    
    if arg in ("-h", "--help"):
        printUsage()
    elif arg in ("-v", "--verbose"):
        verbose = True
        argn += 1
    elif arg == "--loglevel":
        val = val.upper()
        if val == "DEBUG":
            loglevel = logging.DEBUG
        elif val == "INFO":
            loglevel = logging.INFO
        elif val in ("WARN", "WARNING"):
            loglevel = logging.WARNING
        elif val == "ERROR":
            loglevel = logging.ERROR
        else:
            printUsage()  
        argn += 2
    elif arg in ("-e", "--endpoint"):
        cfg["hs_endpoint"] = sys.argv[argn+1]
        argn += 2
    elif arg in ("-u", "--username"):
        cfg["hs_username"] = sys.argv[argn+1]
        argn += 2
    elif arg in ("-p", "--password"):
         cfg["hs_password"] = sys.argv[argn+1]
         argn += 2
    elif arg[0] == '-':
         printUsage()
    else:
         domains.append(arg)
         argn += 1
 
if len(domains) == 0:
    # need a domain
    printUsage()

# setup logging
logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
logging.debug("set log_level to {}".format(loglevel))

for domain in domains:
    if not domain.startswith('/'):
        sys.exit("domain: {} must start with a slash".format(domain))
    if domain.endswith('/'):
        sys.exit("domain: {} can not end with slash".format(domain))
    
    touchDomain(domain)

    
    
     


