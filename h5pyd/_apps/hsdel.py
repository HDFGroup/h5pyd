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
    folder = h5py.Folder(domain, mode=mode, endpoint=endpoint, username=username, password=password, bucket=bucket)
    return folder

def exitUnlessIgnore(msg):
    if cfg["ignore"]:
        return
    sys.exit(msg)



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
            msg = f"Parent domain: {parent_domain} not found"
            logging.error(msg)
            exitUnlessIgnore(msg)
        elif oe.errno == 401:  # Unauthorized
            msg = f"Authorization failure opening {parent_domain}"
            logging.error(msg)
            exitUnlessIgnore(msg)
        elif oe.errno == 403:  # Forbidden
            msg = f"Not allowed to open: {parent_domain}"
            logging.error(msg)
            exitUnlessIgnore(msg)
        else:
            msg = f"Unexpected error: {oe}"
            logging.error(msg)
            exitUnlessIgnore(msg)

    if base_name not in hparent:
        # note - this may happen if the domain was recently created and not
        # yet synced to S3
        msg = f"domain: {domain} not found"
        logging.error(msg)
        exitUnlessIgnore(msg)

    # delete the domain
    try:
        del hparent[base_name]
    except IOError as oe:
        if oe.errno == 404:   # Not Found
            # should have caught this in the base_name check...
            msg = f"domain {parent_domain} not found"
            logging.error(msg)
            exitUnlessIgnore(msg)
        elif oe.errno == 401:  # Unauthorized
            msg = "Authorization failure"
            logging.error(msg)
            exitUnlessIgnore(msg)
        elif oe.errno == 403:  # Forbidden
            msg = "Not Allowed"
            logging.error(msg)
            exitUnlessIgnore(msg)
        elif oe.errno == 409 and domain.endswith('/'):  # Conflict
            msg = "folder has sub-items"
            logging.error(msg)
            exitUnlessIgnore(msg)
        else:
            msg = f"Unexpected error: {oe}"
            logging.error(msg)
            exitUnlessIgnore(msg)
    if cfg["verbose"]:
        if domain.endswith('/'):
            msg = f"Folder: {domain} deleted"
        else:
            msg = f"Domain: {domain} deleted"
        cfg.print(msg)
           

#
# Usage
#
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  target")
    print("")
    print("Description:")
    print("    Delete given domains")
    print("       target: one or more domains to be deleted")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("Examples:")
    print(f"     {cmd} /home/myfolder/file1.h5  /home/myfolder/file2.h5")
    print(f"     {cmd} hdf5://home/myfolder/file2.h5  hdf5://home/afolder/")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit()

#
# Main
#
def main():

    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        domains = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if len(domains) == 0:
        # need a domain
        usage()

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")

    for domain in domains:
        deleteDomain(domain)


if __name__ == "__main__":
    main()
