##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

import sys
import logging
import numpy as np

try:
    import h5py
    import h5pyd
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e))
    sys.exit(1)

try:
    import s3fs
    S3FS_IMPORT = True
except ImportError:
    S3FS_IMPORT = False

if __name__ == "__main__":
    from config import Config
    from chunkiter import ChunkIterator
else:
    from .config import Config
    from .chunkiter import ChunkIterator

cfg = Config()

def diff_attrs(src, tgt, ctx):
    """ compare attributes of src and tgt """
    msg = "checking attributes of {}".format(src.name)
    logging.debug(msg)

    if len(src.attrs) != len(tgt.attrs):
        msg = "<{}> have a different number of attribute from <{}>".format(src.name, tgt.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(msg)
        ctx["differences"] += 1
        return False

    for name in src.attrs:
        msg = "checking attribute {} of {}".format(name, src.name)
        logging.debug(msg)
        if ctx["verbose"]:
            print(msg)
        if name not in tgt.attrs:
            msg = "<{}>  has attribute {} not found in <{}>".format(src.name, name, tgt.name)
            logging.info(msg)
            if not ctx["quiet"]:
                print(msg)
            ctx["differences"] += 1
            return False
        src_attr = src.attrs[name]
        tgt_attr = tgt.attrs[name]
        if isinstance(src_attr, np.ndarray):
            # compare shape, type, and values
            if src_attr.dtype != tgt_attr.dtype:
                msg = "Type of attribute {} of <{}> is different".format(name, src.name)
                logging.info(msg)
                if not ctx["quiet"]:
                    print(msg)
                ctx["differences"] += 1
                return False
            if src_attr.shape != tgt_attr.shape:
                msg = "Shape of attribute {} of <{}> is different".format(name, src.name)
                logging.info(msg)
                if not ctx["quiet"]:
                    print(msg)
                ctx["differences"] += 1
                return False
            if hash(src_attr.tostring()) != hash(tgt_attr.tostring()):
                msg = "values for attribute {} of <{}> differ".format(name, src.name)
                logging.info(msg)
                if not ctx["quiet"]:
                    print(msg)
                ctx["differences"] += 1
                return False
        elif src_attr != tgt_attr:
            # returned as int or string, just compare values
            msg = "<{}>  has attribute {} different than <{}>".format(src.name, name, tgt.name)
            logging.info(msg)

            if not ctx["quiet"]:
                print(msg)
            ctx["differences"] += 1
            return False

    # of of attribute iteration
    return True


def diff_group(src, ctx):
    """ compare group in src and tgt
    """
    msg = "checking group <{}>".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)

    fout = ctx["fout"]

    if src.name not in fout:
        msg = "<{}> not found in target".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(msg)
        ctx["differences"] += 1
        return False

    tgt = fout[src.name]

    # printed when there is a difference
    output = "group: <{}> and <{}>".format(src.name, tgt.name)
    if len(src) != len(tgt):
        msg = "{} group have a different number of links from {}".format(src.name, tgt.name)
        logging.info(msg)
        if ctx["verbose"]:
            print(msg)
        if not ctx["quiet"]:
            print(output)
        ctx["differences"] += 1
        return False

    for title in src:
        if ctx["verbose"]:
            print("got link: '{}' of group <{}>".format(title, src.name))
        if title not in tgt:
            msg = "<{}> group has link {} not found in <{}>".format(src.name, title, tgt.name)
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
            if not ctx["quiet"]:
                print(output)
            ctx["differences"] += 1
            return False

        lnk_src = src.get(title, getlink=True)
        lnk_src_type = lnk_src.__class__.__name__
        lnk_tgt = tgt.get(title, getlink=True)
        lnk_tgt_type = lnk_tgt.__class__.__name__
        if lnk_src_type != lnk_tgt_type:
            msg = "<{}> group has link {} of different type than found in <{}>".format(src.name, title, tgt.name)
            logging.info(msg)
            if ctx["verbose"]:
                print(msg)
            if not ctx["quiet"]:
                print(output)
            ctx["differences"] += 1
            return False

        if lnk_src_type == "HardLink":
            logging.debug("Got hardlink: {}".format(title))
            # TBD: handle the case where multiple hardlinks point to same object
        elif lnk_src_type == "SoftLink":
            msg = "Got SoftLink({}) with title: {}".format(lnk_src.path, title)
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if lnk_src.path != lnk_tgt.path:
                msg = "<{}> group has link {} with different path than <{}>".format(src.name, title, tgt.name)
                if ctx["verbose"]:
                    print(msg)
                if not ctx["quiet"]:
                    print(output)
                ctx["differences"] += 1
                return False
        elif lnk_src_type == "ExternalLink":
            msg = "<{}> group has ExternalLink {} ({}, {})".format(src.name, title, lnk_src.filename, lnk_src.path)
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if lnk_src.filename != lnk_tgt.filename:
                msg = "<{}> group has external link {} with different filename than <{}>".format(src.name, title, tgt.name)
                if ctx["verbose"]:
                    print(msg)
                if not ctx["quiet"]:
                    print(output)
                ctx["differences"] += 1
                return False
            if lnk_src.path != lnk_tgt.path:
                msg = "<{}> group has external link {} with different path than <{}>".format(src.name, title, tgt.name)
                if ctx["verbose"]:
                    print(msg)
                if not ctx["quiet"]:
                    print(output)
                ctx["differences"] += 1
                return False
        else:
            msg = "Unexpected link type: {}".format(lnk_src_type)
            logging.warning(msg)
            if ctx["verbose"]:
                print(msg)
    # end link iteration

    if not ctx["noattr"]:
        result = diff_attrs(src, tgt, ctx)
    else:
        result = True
    return result



def diff_datatype(src, ctx):
    """ compare datatype objects in src and tgt
    """
    msg = "checking datatype <{}>".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)

    fout = ctx["fout"]

    if src.name not in fout:
        msg = "<{}> not found in target".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(msg)
        ctx["differences"] += 1
        return False
    tgt = fout[src.name]

    if tgt.dtype != src.dtype:
        msg = "Type of <{}> is different".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(msg)
        ctx["differences"] += 1
        return False

    if not ctx["noattr"]:
        result = diff_attrs(src, tgt, ctx)
    else:
        result = True
    return result


def diff_dataset(src, ctx):
    """ compare dataset in src and tgt
    """
    msg = "checking dataset <{}>".format(src.name)
    logging.info(msg)
    if ctx["verbose"]:
        print(msg)

    fout = ctx["fout"]

    if src.name not in fout:
        msg = "<{}> not found in target".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(msg)
        ctx["differences"] += 1
        return False
    tgt = fout[src.name]

    try:
        tgt_shape = tgt.shape
    except AttributeError:
        msg = "<{}> in target not a dataset".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(msg)
        ctx["differences"] += 1
        return False

    # printed when there is a difference
    output = "dataset: <{}> and <{}>".format(src.name, tgt.name)
    if tgt_shape != src.shape:
        msg = "Shape of <{}> is different".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(output)
        ctx["differences"] += 1

        return False

    if tgt.dtype != src.dtype:
        msg = "Type of <{}> is different".format(src.name)
        logging.info(msg)
        if not ctx["quiet"]:
            print(output)
        ctx["differences"] += 1
        return False

    # TBD - check fillvalue

    if ctx["nodata"]:
        # skip data compare
        return True

    try:
        it = ChunkIterator(src)

        for s in it:
            msg = "checking dataset data for slice: {}".format(s)
            logging.debug(msg)

            arr_src = src[s]
            msg = "got src array {}".format(arr_src.shape)
            logging.debug(msg)
            arr_tgt = tgt[s]
            msg = "got tgt array {}".format(arr_tgt.shape)
            logging.debug(msg)

            if hash(arr_src.tostring()) != hash(arr_tgt.tostring()):
                msg = "values for dataset {} differ for slice: {}".format(src.name, s)
                logging.info(msg)
                if not ctx["quiet"]:
                    print(output)
                ctx["differences"] += 1
                return False

    except (IOError, TypeError) as e:
        msg = "ERROR : failed to copy dataset data : {}".format(str(e))
        logging.error(msg)
        print(msg)

    if not ctx["noattr"]:
        result = diff_attrs(src, tgt, ctx)
    else:
        result = True
    return result



def diff_file(fin, fout, verbose=False, nodata=False, noattr=False, quiet=False):
    ctx = {}
    ctx["fin"] = fin
    ctx["fout"] = fout
    ctx["verbose"] = verbose
    ctx["nodata"] = nodata
    ctx["noattr"] = noattr
    ctx["quiet"] = quiet
    ctx["differences"] = 0


    def object_diff_helper(name, obj):
        class_name = obj.__class__.__name__

        if class_name in ("Dataset", "Table"):
            diff_dataset(obj, ctx)
        elif class_name == "Group":
            diff_group(obj, ctx)
        elif class_name == "Datatype":
            diff_datatype(obj, ctx)
        else:
            logging.error("no handler for object class: {}".format(type(obj)))

    # check links in root group
    diff_group(fin, ctx)

    # build a rough map of the file using the internal function above
    fin.visititems(object_diff_helper)
    return ctx["differences"]


#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    {} [ OPTIONS ]  file  domain".format(cfg["cmd"])))
    print("")
    print("Description:")
    print("    Diff HDF5 file with domain")
    print("       file: HDF5 file ")
    print("       domain: domain")
    print("")
    print("Options:")
    print("     -v | --verbose :: verbose output")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://hsdshdflab.hdfgroup.org")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     --bucket <bucket_name> :: Storage bucket")
    print("     --nodata :: Do not compare dataset data")
    print("     --noattr :: Do not compare attributes")
    print("     --quiet :: Do not produce output")
    print("     -h | --help    :: This message.")
    print("")
#end print_usage


#----------------------------------------------------------------------------------
def print_config_example():
    print("# default")
    print("hs_username = <username>")
    print("hs_password = <passwd>")
    print("hs_endpoint = http://hsdshdflab.hdfgroup.org")
#print_config_example

#----------------------------------------------------------------------------------
def main():

    loglevel = logging.ERROR
    verbose = False
    nodata = False
    noattr = False
    quiet = False
    cfg["cmd"] = sys.argv[0].split('/')[-1]
    if cfg["cmd"].endswith(".py"):
        cfg["cmd"] = "python " + cfg["cmd"]
    cfg["logfname"] = None
    logfname=None
    rc = 0
    s3 = None  # s3fs instance

    src_files = []
    argn = 1
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None

        if arg[0] == '-' and len(src_files) > 0:
            # options must be placed before filenames
            print("options must precead source files")
            usage()
            sys.exit(-1)
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1]
        if arg in ("-v", "--verbose"):
            verbose = True
            argn += 1
        elif arg == "--nodata":
            nodata = True
            argn += 1
        elif arg == "--noattr":
            noattr = True
            argn += 1
        elif arg in ("-q", "--quiet"):
            quiet = True
            argn += 1
        elif arg == "--loglevel":
            if val == "debug":
                loglevel = logging.DEBUG
            elif val == "info":
                loglevel = logging.INFO
            elif val == "warning":
                loglevel = logging.WARNING
            elif val == "error":
                loglevel = logging.ERROR
            else:
                print("unknown loglevel")
                usage()
                sys.exit(-1)
            argn += 2
        elif arg == '--logfile':
            logfname = val
            argn += 2
        elif arg in ("-b", "--bucket"):
            cfg["hs_bucket"] = val
            argn += 2
        elif arg in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif arg in ("-e", "--endpoint"):
            cfg["hs_endpoint"] = val
            argn += 2
        elif arg in ("-u", "--username"):
            cfg["hs_username"] = val
            argn += 2
        elif arg in ("-p", "--password"):
            cfg["hs_password"] = val
            argn += 2
        elif arg == '--cnf-eg':
            print_config_example()
            sys.exit(0)
        elif arg[0] == '-':
            usage()
            sys.exit(-1)
        else:
            src_files.append(arg)
            argn += 1

    # setup logging
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(filename)s:%(lineno)d %(message)s', level=loglevel)
    logging.debug("set log_level to {}".format(loglevel))

    # end arg parsing
    logging.info("username: {}".format(cfg["hs_username"]))
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))
    logging.info("verbose: {}".format(verbose))

    if len(src_files) < 2:
        # need at least a src and destination
        usage()
        sys.exit(-1)
    file_path = src_files[0]
    domain_path = src_files[1]

    logging.info("file: {}".format(file_path))
    logging.info("domain: {}".format(domain_path))
    if domain_path[0] != '/' or domain_path[-1] == '/':
        print("domain must be an absolute path, non-folder domain")
        usage()
        sys.exit(-1)


    if cfg["hs_endpoint"] is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(cfg["hs_endpoint"]))

    try:

        # get a handle to input file
        if file_path.startswith("s3://"):
            if not S3FS_IMPORT:
                sys.stderr.write("Install S3FS package to load s3 files")
                sys.exit(1)

            if not s3:
                s3 = s3fs.S3FileSystem(use_ssl=False)
            try:
                fin = h5py.File(s3.open(file_path, "rb"), mode="r")
            except IOError as ioe:
                logging.error("Error opening file {}: {}".format(file_path, ioe))
                sys.exit(1)
        else:
            # regular h5py open
            try:
                fin = h5py.File(file_path, mode='r')
            except IOError as ioe:
                logging.error("Error opening file {}: {}".format(domain_path, ioe))
                sys.exit(1)

        # get the  domain
        try:
            username = cfg["hs_username"]
            password = cfg["hs_password"]
            endpoint = cfg["hs_endpoint"]
            bucket = cfg["hs_bucket"]
            fout = h5pyd.File(domain_path, 'r', endpoint=endpoint, username=username, password=password, bucket=bucket)
        except IOError as ioe:
            if ioe.errno == 404:
                logging.error("domain: {} not found".format(domain_path))
            if ioe.errno == 403:
                logging.error("No read access to domain: {}".format(domain_path))
            else:
                logging.error("Error opening file {}: {}".format(domain_path, ioe))
            sys.exit(1)


        # do the actual load
        if quiet:
            verbose = False
        rc = diff_file(fin, fout, verbose=verbose, nodata=nodata, noattr=noattr, quiet=quiet)

        if not quiet and rc > 0:
            print("{} differences found".format(rc))

        logging.info("diff_file done")

    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)

    sys.exit(rc)


# __main__
if __name__ == "__main__":
    main()
