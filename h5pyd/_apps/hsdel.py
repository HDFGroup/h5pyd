import sys
import os.path as op
import logging
import h5pyd as h5py
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Delete domain
#

cfg = Config()

def getFolder(domain, mode='r'):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    dir = h5py.Folder(domain, mode=mode, endpoint=endpoint, username=username, password=password, bucket=bucket)
    return dir


def deleteDomain(domain):

    # get handle to parent folder
    if domain.endswith('/'):
        path = domain[:-1]
    else:
        path = domain
    parent_domain = op.dirname(path)
    base_name = op.basename(path)

    if len(parent_domain) < 2:
        #sys.exit("can't get parent domain")
        parent_domain = '/'

    if not parent_domain.endswith('/'):
        parent_domain += '/'
    try:
        hparent = getFolder(parent_domain, mode='a')
    except IOError as oe:
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
    try:
        del hparent[base_name]
    except IOError as oe:
        if oe.errno == 404:   # Not Found
            # should have caught this in the base_name check...
            sys.exit("domain: {} not found".format(parent_domain))
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        elif oe.errno == 409 and domain.endswith('/'):  # Conflict
            sys.exit("folder has sub-items")
        else:
            sys.exit("Unexpected error: {}".format(oe))
    if cfg["verbose"]:
        if domain.endswith('/'):
            print("Folder: {} deleted".format(domain))
        else:
            print("Domain: {} deleted".format(domain))

#
# Usage
#
def printUsage():
    print("usage: {} [-v] [-e endpoint] [-u username] [-p password] [--loglevel debug|info|warning|error] [--logfile <logfile>] [--bucket <bucket_name>] domains".format(cfg["cmd"]))
    print("example: {} -e http://hsdshdflab.hdfgroup.org /hdfgroup/data/test/deleteme.h5".format(cfg["cmd"]))
    sys.exit()

#
# Main
#
def main():
    domains = []
    argn = 1
    loglevel = logging.ERROR
    logfname=None
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["verbose"] = False

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
        elif arg in ("-b", "--bucket"):
            cfg["hs_bucket"] = val
            argn += 2
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


    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))

    for domain in domains:
        if not domain.startswith('/'):
            sys.exit("domain: {} must start with a slash".format(domain))

        deleteDomain(domain)


if __name__ == "__main__":
    main()
