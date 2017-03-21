import sys
import os.path as op
import logging
import h5pyd as h5py
from config import Config

#
# Delete domain
#

verbose = False
showacls = False
 
cfg = Config()
 

def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    dir = h5py.Folder(domain, endpoint=endpoint, username=username, password=password)
    return dir
 
   
def deleteDomain(domain):

    # get handle to parent folder
    parent_domain = op.dirname(domain)
    base_name = op.basename(domain)

    if len(parent_domain) < 2:
        sys.exit("can't get parent domain")

    if not parent_domain.endswith('/'):
        parent_domain += '/'
    try:
        hparent = getFolder(parent_domain)
    except OSError as oe:
        if oe.errno == 404:   # Not Found
            sys.exit("Parent domain: {} not found".format(parent_domain))
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        else:
            sys.exit("Unexpected error: {}".format(oe))
    
    if base_name not in hparent:
        # note - this may happen if the domain was recently created and not 
        # yet synced to S3
        sys.exit("domain: {} not found".format(domain))

    # delete the domain
    del hparent[base_name]
      
#
# Usage
#
def printUsage():
    print("usage: python hsdel.py [-v] [-e endpoint] [-u username] [-p password] [--loglevel debug|info|warning|error] [--logfile <logfile>] domains")
    print("example: python hsdel.py -e http://data.hdfgroup.org:7253 /hdfgroup/data/test/deleteme.h5")
    sys.exit()

#
# Main
#

domains = []
argn = 1
loglevel = logging.ERROR
logfname=None

while argn < len(sys.argv):
    arg = sys.argv[argn]
    val = None
    if len(sys.argv) > argn + 1:
        val = sys.argv[argn+1]
    
    if arg in ("-h", "--help"):
        printUsage()
    elif arg in ("-e", "--endpoint"):
        cfg["hs_endpoint"] = val
        argn += 2
    elif arg in ("-u", "--username"):
        cfg["hs_username"] = val
        argn += 2
    elif arg in ("-p", "--password"):
         cfg["hs_password"] = val
         argn += 2
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
    elif arg == '--logfile':
        logfname = val
        argn += 2
    elif arg[0] == '-':
         printUsage()
    else:
         domains.append(arg)
         argn += 1
 
if len(domains) == 0:
    # need a domain
    printUsage()


logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
logging.debug("set log_level to {}".format(loglevel))

for domain in domains:
    if not domain.startswith('/'):
        sys.exit("domain: {} must start with a slash".format(domain))
    if domain.endswith('/'):
        sys.exit("domain: {} can not end with slash".format(domain))
    
    deleteDomain(domain)


