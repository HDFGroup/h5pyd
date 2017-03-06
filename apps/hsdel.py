import h5pyd as h5py
import sys
import os.path as op
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
    print("usage: python hsdel.py [-e endpoint] [-u username] [-p password] domains")
    print("example: python hsdel.py -e http://data.hdfgroup.org:7253 /hdfgroup/data/test/deleteme.h5")
    sys.exit()

#
# Main
#

domains = []
argn = 1

while argn < len(sys.argv):
    arg = sys.argv[argn]
    
    if arg in ("-h", "--help"):
        printUsage()
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

for domain in domains:
    if not domain.startswith('/'):
        sys.exit("domain: {} must start with a slash".format(domain))
    if domain.endswith('/'):
        sys.exit("domain: {} can not end with slash".format(domain))
    
    deleteDomain(domain)

    
    
     


