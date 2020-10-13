import sys
import logging
from h5pyd._hl.h5commands import HSLSCommand

#
# Usage
#
def printUsage(cmd):
    print("usage: {} [-r] [-v] [-h] [--showacls] [--showattrs] [--loglevel debug|info|warning|error] [--logfile <logfile>] [-e endpoint] [-u username] [-p password] [--bucket bucketname] domains".format(cmd))
    print("example: {} -r -e http://hsdshdflab.hdfgroup.org /shared/tall.h5".format(cmd))
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -H | --human-readable :: with -v, print human readable sizes (e.g. 123M)")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --showacls :: print domain ACLs")
    print("     --showattrs :: print attributes")
    print("     --pattern  :: <regex>  :: list domains that match the given regex")
    print("     --query :: <query> list domains where the attributes of the root group match the given query string")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     -h | --help    :: This message.")
    sys.exit()

def main():
    argn = 1
    domains = []
    verbose = False
    showacls=False
    showattrs=False
    human_readable=False
    pattern=None
    query=None
    recursive=False
    loglevel=None
    logfile=None
    endpoint=None
    username=None
    password=None
    bucket=None


    cmd = sys.argv[0].split('/')[-1]
    if cmd.endswith(".py"):
        cmd = "python " + cmd
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn + 1]
        if arg in ("-r", "--recursive"):
            recursive = True
            argn += 1
        elif arg in ("-v", "--verbose"):
            verbose = True
            argn += 1
        elif arg in ("-H", "--human-readable"):
            human_readable = True
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
                printUsage(cmd)
            argn += 2
        elif arg == '--logfile':
            logfile = val
            argn += 2
        elif arg in ("-showacls", "--showacls"):
            showacls = True
            argn += 1
        elif arg in ("-showattrs", "--showattrs"):
            showattrs = True
            argn += 1
        elif arg in ("-h", "--help"):
            printUsage(cmd)
        elif arg in ("-e", "--endpoint"):
            endpoint = val
            argn += 2
        elif arg in ("-u", "--username"):
            username = val
            argn += 2
        elif arg in ("-p", "--password"):
            password = val
            argn += 2
        elif arg in ("-b", "--bucket"):
            bucket = val
            argn += 2
        elif arg == "--pattern":
            pattern = val
            argn += 2
        elif arg == "--query":
            query = val
            argn += 2

        elif arg[0] == '-':
            printUsage(cmd)
        else:
            domains.append(arg)
            argn += 1

    cmd = HSLSCommand(endpoint, username, password)
    cmd.execute(domains, verbose=verbose, showacls=showacls, showattrs=showattrs, human_readable=human_readable, pattern=pattern, 
query=query, cmd=cmd, recursive=recursive, loglevel=loglevel, logfile=logfile, bucket=bucket)

if __name__ == "__main__":
    main()
