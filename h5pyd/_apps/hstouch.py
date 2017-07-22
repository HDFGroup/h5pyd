import sys
import os.path as op
import logging
import h5pyd as h5py
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Create domain or update timestamp if domain already exist
#
 
cfg = Config()

def getFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    #print("getFolder", domain)
    dir = h5py.Folder(domain, endpoint=endpoint, username=username, password=password)
    return dir

def createFolder(domain):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    #print("getFolder", domain)
    dir = h5py.Folder(domain, mode='x', endpoint=endpoint, username=username, password=password)
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

    make_folder = False
    if domain[-1] == '/':
        make_folder = True
        domain = domain[:-1]

    # get handle to parent folder
    parent_domain = op.dirname(domain) 

    if len(parent_domain) < 2:
        sys.exit("can't create top-level domain")

    if not parent_domain.endswith('/'):
        parent_domain += '/'
    try:
        getFolder(parent_domain)
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
        if not make_folder:
            try:
                r = hdomain['/']
                # create/update attribute to update lastModified timestamp of domain
                r.attrs["hstouch"] = 1
                hdomain.close()
            except OSError as oe:
                sys.exit("Got error updating domain: {}".format(oe))
        else:
            sys.exit("Can not update timestamp of folder object")
        hdomain.close()
    else:
        # create domain
        if not make_folder:
            try:
                fh = createFile(domain)
                if cfg["verbose"]:
                    print("domain created: {}, root id: {}".format(domain, fh.id.id))
                fh.close()
            except OSError as oe:
                sys.exit("Got error updating domain: {}".format(oe))
        else:
            # make folder
            try:
                fh = createFolder(domain + '/')
                if cfg["verbose"]:
                    print("folder created", domain + '/')
                fh.close()
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
def main():
    domains = []
    argn = 1
    depth = 2
    loglevel = logging.ERROR
    logfname=None
    cfg["verbose"] = False

    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
    
        if arg in ("-h", "--help"):
            printUsage()
        elif arg in ("-v", "--verbose"):
            cfg["verbose"] = True
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
       
        touchDomain(domain)

if __name__ == "__main__":
    main()
    
     


