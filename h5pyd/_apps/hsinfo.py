
import sys
import logging
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
    print("usage: {} [-h] [--loglevel debug|info|warning|error] [--logfile <logfile>] [-c oonf_file] [-e endpoint] [-u username] [-p password]".format(cfg["cmd"]))
    print("example: {} -e http://data.hdfgroup.org:7253".format(cfg["cmd"]))
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
# Get folder in /home/ that is owned by given user
#
def getHomeFolder(username):
    if not username:
        return None
    dir = h5pyd.Folder('/home/')  # get folder object for root
    homefolder = None
    for name in dir:
        # we should come across the given domain
        if username.startswith(name):
            # check any folders where the name matches at least part of the username
            # e.g. folder: "/home/bob/" for username "bob@acme.com"
            path = '/home/' + name + '/'
            f = h5pyd.Folder(path)
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
 
    print("endpoint: {}".format(endpoint)) 
    if not endpoint or endpoint[-1] == '/' or endpoint[:4] != "http":
        print("WARNING: endpoint: {} doesn't appear to be valid".format(endpoint))
    try:
        info = h5pyd.getServerInfo(username=username, password=password, endpoint=endpoint)
        print("server name: {}".format(info["name"]))
        if "state" in info:
            print("server state: {}".format(info['state']))
        print("username: {}".format(info["username"]))
        print("password: {}".format(info["password"]))
        home_folder = getHomeFolder(username)
        if home_folder:
            print("home: {}".format(home_folder))
    
        if "hsds_version" in info:
            print("server version: {}".format(info["hsds_version"]))
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

if __name__ == "__main__":
    main()


