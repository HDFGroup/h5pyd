
import sys
import os.path as op
import logging
from datetime import datetime
import time
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
    print("usage: python hsinfo.py [-h] [--loglevel debug|info|warning|error] [--logfile <logfile>] [-c oonf_file] [-e endpoint] [-u username] [-p password]")
    print("example: python hsinfo.py  -e http://data.hdfgroup.org:7253")
    print("")
    print("Options:")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://example.com:8080")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
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
#
# Main
#
def main():
    argn = 1
    depth = 2
    cfg["loglevel"] = logging.ERROR
    cfg["logfname"] = None


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
        else:
            printUsage()

    # setup logging
     
    logging.basicConfig(filename=cfg["logfname"], format='%(asctime)s %(message)s', level=cfg["loglevel"])
    logging.debug("set log_level to {}".format(cfg["loglevel"]))

    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
 
    print("endpoint:", endpoint) 
    try:
        info = h5pyd.getServerInfo(username=username, password=password, endpoint=endpoint)
        print("server name:", info["name"])
        if "state" in info:
            print("server state:", info['state'])
        print("username:", info["username"])
        print("password:", info["password"])
    
        if "hsds_version" in info:
            print("server version:", info["hsds_version"])
        elif "h5serv_version" in info:
            print("server version", info["h5serv_version"])
        if "start_time" in info:
            uptime = getUpTime(info["start_time"])
            print("up: {}".format(uptime))
        print("h5pyd version:", h5pyd.version.version)
        
    except IOError as ioe:
        if ioe.errno == 401:
            print("username/password not valid for username:", username)
        else:
            print("Error:", ioe)

if __name__ == "__main__":
    main()


