
import sys
import logging
import time
from datetime import datetime
import h5pyd
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Print objects in a domain in the style of the hsls utilitiy
#


cfg = Config()


#
# Usage
#
def printUsage():
    print("Usage: {} [-h] [--loglevel debug|info|warning|error] [--logfile <logfile>] [-c oonf_file] [-e endpoint] [-u username] [-p password] [-b bucket] [domain]".format(cfg["cmd"]))
    print("")
    print("Description:")
    print("    Get status information from server, or domain stats if domain is provided")
    print("")
    print("Options:")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -b | --bucket <bucket> :: bucket name (for use when domain is provided)")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     -H | --human-readable :: with -v, print human readable sizes (e.g. 123M)")
    print("     --rescan :: refresh domain stats (for use when domain is provided)")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     -h | --help    :: This message.")
    sys.exit()
#
#
#
def getUpTime(start_time):
    now = int(time.time())
    sec = now - start_time
    days = sec // (24*60*60)
    sec -= 24*60*60*days
    hrs = sec // (60*60)
    sec -= 60*60*hrs
    mins = sec // 60
    sec -= 60*mins
    if days:
        ret_str = "{} days, {} hours {} min {} sec".format(days, hrs, mins, sec)
    elif hrs:
        ret_str =  "{} hours {} min {} sec".format(hrs, mins, sec)
    elif mins:
        ret_str =  "{} min {} sec".format(mins, sec)
    else:
        ret_str =  "{} sec".format(sec)

    return ret_str

def format_size(n):
    if n is None or n == ' ':
        return ' ' * 8
    symbol = ' '
    if not cfg["human_readable"]:
        return str(n)
    # convert to common storage unit
    for s in ('B', 'K', 'M', 'G', 'T'):
        if n < 1024:
            symbol = s
            break
        n /= 1024
    if symbol == 'B':
        return "{:}B".format(n)
    else:
        return "{:.1f}{}".format(n, symbol)

def getServerInfo(cfg):
    """ get server state and print """
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    try:
        info = h5pyd.getServerInfo(username=username, password=password, endpoint=endpoint)
        print("server name: {}".format(info["name"]))
        if "state" in info:
            print("server state: {}".format(info['state']))
        print("endpoint: {}".format(endpoint))
        print("username: {}".format(info["username"]))
        print("password: {}".format(info["password"]))
        if info['state'] == "READY":
            home_folder = getHomeFolder()
            if home_folder:
                print("home: {}".format(home_folder))

        if "hsds_version" in info:
            print("server version: {}".format(info["hsds_version"]))
        if "node_count" in info:
            print("node count: {}".format(info["node_count"]))
        elif "h5serv_version" in info:
            print("server version: {}".format(info["h5serv_version"]))
        if "start_time" in info:
            uptime = getUpTime(info["start_time"])
            print("up: {}".format(uptime))
        print("h5pyd version: {}".format(h5pyd.version.version))


    except IOError as ioe:
        if ioe.errno == 401:
            print("username/password not valid for username: {}".format(username))
        else:
            print("Error: {}".format(ioe))

def getDomainInfo(domain, cfg):
    """ get info about the domain and print """
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    if "rescan" in cfg and cfg["rescan"]:
        mode = "r+"  # need write intent
    else:
        mode = 'r'

    if domain.endswith('/'):
        is_folder = True
    else:
        is_folder = False

    try:
        if is_folder:
            f = h5pyd.Folder(domain, mode=mode, endpoint=endpoint, username=username,
                   password=password, bucket=bucket, use_cache=True)
        else:
            f = h5pyd.File(domain, mode=mode, endpoint=endpoint, username=username,
                   password=password, bucket=bucket, use_cache=False)
    except IOError as oe:
        if oe.errno in (404, 410):   # Not Found
            sys.exit("domain: {} not found".format(domain))
        elif oe.errno == 401:  # Unauthorized
            sys.exit("Authorization failure")
        elif oe.errno == 403:  # Forbidden
            sys.exit("Not allowed")
        else:
            sys.exit("Unexpected error: {}".format(oe))

    timestamp = datetime.fromtimestamp(int(f.modified))
    if not is_folder and f.last_scan:
        last_scan = datetime.fromtimestamp(int(f.last_scan))
    else:
        last_scan = None

    if is_folder:
        print("folder: {}".format(domain))
        print("    owner:           {}".format(f.owner))
        print("    last modified:   {}".format(timestamp))
    else:
        if "rescan" in cfg and cfg["rescan"]:
            f.run_scan()

        # report HDF objects (groups, datasets, and named datatypes) vs. allocated chunks
        num_objects = f.num_groups + f.num_datatypes + f.num_datasets
        if f.num_chunks > 0:
            num_chunks = f.num_chunks
        else:
            # older storeinfo format doesn't have num_chunks, so calculate
            num_chunks = f.num_objects - num_objects

        print("domain: {}".format(domain))
        print("    owner:           {}".format(f.owner))
        print("    id:              {}".format(f.id.id))
        print("    last modified:   {}".format(timestamp))
        if last_scan:
            print("    last scan:       {}".format(last_scan))
        if f.md5_sum:
            print("    md5 sum:         {}".format(f.md5_sum))
        print("    total_size:      {}".format(format_size(f.total_size)))
        print("    allocated_bytes: {}".format(format_size(f.allocated_bytes)))
        if f.metadata_bytes:
            print("    metadata_bytes:  {}".format(format_size(f.metadata_bytes)))
        if f.linked_bytes:
            print("    linked_bytes:    {}".format(format_size(f.linked_bytes)))
        print("    num objects:     {}".format(num_objects))
        print("    num chunks:      {}".format(num_chunks))
        if f.num_linked_chunks:
            print("    linked chunks:   {}".format(f.num_linked_chunks))


    f.close()

#
# Get folder in /home/ that is owned by given user
#
def getHomeFolder():
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    if not username:
        return None
    dir = h5pyd.Folder('/home/', username=username, password=password, endpoint=endpoint)  # get folder object for root
    homefolder = None
    for name in dir:
        # we should come across the given domain
        if username.startswith(name):
            # check any folders where the name matches at least part of the username
            # e.g. folder: "/home/bob/" for username "bob@acme.com"
            path = '/home/' + name + '/'
            try:
                f = h5pyd.Folder(path, username=username, password=password, endpoint=endpoint)
            except IOError as ioe:
                logging.info("find home folder - got ioe: {}".format(ioe))
                continue
            except Exception as e:
                logging.warn("find home folder - got exception: {}".format(e))
                continue
            if f.owner == username:
                homefolder = path
            f.close()
            if homefolder:
                break

    dir.close()
    return homefolder
#
# Main
#
def main():
    argn = 1
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["loglevel"] = logging.ERROR
    cfg["logfname"] = None
    cfg["human_readable"] = False
    domains = []


    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
        if arg == "--loglevel":
            val = val.upper()
            if val == "DEBUG":
                cfg["loglevel"] = logging.DEBUG
            elif val == "INFO":
                cfg["loglevel"] = logging.INFO
            elif val in ("WARN", "WARNING"):
                cfg["loglevel"] = logging.WARNING
            elif val == "ERROR":
                cfg["loglevel"] = logging.ERROR
            else:
                printUsage()
            argn += 2
        elif arg == '--logfile':
            cfg["logfname"] = val
            argn += 2
        elif arg in ("-h", "--help"):
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
        elif arg == "--rescan":
            cfg["rescan"] = True
            argn += 1
        elif arg == "-H":
             cfg["human_readable"] = True
             argn += 1
        else:
            domains.append(arg)
            argn += 1

    # setup logging

    logging.basicConfig(filename=cfg["logfname"], format='%(levelname)s %(asctime)s %(message)s', level=cfg["loglevel"])
    logging.debug("set log_level to {}".format(cfg["loglevel"]))

    endpoint = cfg["hs_endpoint"]
    if not endpoint or endpoint[-1] == '/' or endpoint[:4] != "http":
        print("WARNING: endpoint: {} doesn't appear to be valid".format(endpoint))

    if not domains:
        getServerInfo(cfg)
    else:
        for domain in domains:
            getDomainInfo(domain, cfg)


if __name__ == "__main__":
    main()
