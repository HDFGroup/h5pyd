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
    sys.stderr.write(f"ERROR : {str(e)} : install it to use this utility...\n")
    sys.exit(1)

try:
    import s3fs

    S3FS_IMPORT = True
except ImportError:
    S3FS_IMPORT = False

if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

cfg = Config()

def getFile(domain, mode="r"):
    username = cfg["hs_username"]
    password = cfg["hs_password"]
    endpoint = cfg["hs_endpoint"]
    bucket = cfg["hs_bucket"]
    fh = h5pyd.File(domain, mode=mode, endpoint=endpoint, username=username,
                   password=password, bucket=bucket, use_cache=True)
    return fh


def diff_attrs(src, tgt, ctx):
    """compare attributes of src and tgt"""
    msg = "checking attributes of {}".format(src.name)
    logging.debug(msg)

    if len(src.attrs) != len(tgt.attrs):
        msg = "<{}> have a different number of attribute from <{}>".format(
            src.name, tgt.name
        )
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
            msg = "<{}>  has attribute {} not found in <{}>".format(
                src.name, name, tgt.name
            )
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
                msg = "Shape of attribute {} of <{}> is different".format(
                    name, src.name
                )
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
            msg = "<{}>  has attribute {} different than <{}>".format(
                src.name, name, tgt.name
            )
            logging.info(msg)

            if not ctx["quiet"]:
                print(msg)
            ctx["differences"] += 1
            return False

    # of of attribute iteration
    return True


def diff_group(src, ctx):
    """compare group in src and tgt"""
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
        msg = "{} group have a different number of links from {}".format(
            src.name, tgt.name
        )
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
            msg = "<{}> group has link {} not found in <{}>".format(
                src.name, title, tgt.name
            )
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
            msg = "<{}> group has link {} of different type than found in <{}>".format(
                src.name, title, tgt.name
            )
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
                msg = "<{}> group has link {} with different path than <{}>".format(
                    src.name, title, tgt.name
                )
                if ctx["verbose"]:
                    print(msg)
                if not ctx["quiet"]:
                    print(output)
                ctx["differences"] += 1
                return False
        elif lnk_src_type == "ExternalLink":
            msg = "<{}> group has ExternalLink {} ({}, {})".format(
                src.name, title, lnk_src.filename, lnk_src.path
            )
            if ctx["verbose"]:
                print(msg)
            logging.info(msg)
            if lnk_src.filename != lnk_tgt.filename:
                msg = "<{}> group has external link {} with different filename than <{}>".format(
                    src.name, title, tgt.name
                )
                if ctx["verbose"]:
                    print(msg)
                if not ctx["quiet"]:
                    print(output)
                ctx["differences"] += 1
                return False
            if lnk_src.path != lnk_tgt.path:
                msg = "<{}> group has external link {} with different path than <{}>".format(
                    src.name, title, tgt.name
                )
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
    """compare datatype objects in src and tgt"""
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
    """compare dataset in src and tgt"""
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

    if src.shape is None:
        # null shape dataset
        return True

    if len(src.shape) == 0:
        # scalar dataset
        if src[()] == tgt[()]:
            is_equal = True
        else:
            is_equal = False
        if is_equal:
            return True
        else:
            msg = "values for scalar datasets {} differ".format(src.name)
            logging.info(msg)
            if not ctx["quiet"]:
                print(msg)
            else:
                print("quiet output differ")
            ctx["differences"] += 1
            return False
        

    if src.chunks is None:
        # assume that the dataset is small enough that we can 
        # read all the values into memory.
        # TBD: use some sort  of psuedo-chunk iteration for large
        # contiguous datasetsChunkIter
        arr_src = src[...]
        arr_tgt = tgt[...]
        is_equal = np.array_equal(arr_src, arr_tgt)
        if is_equal:
            return True
        else:
            msg = "values for datasets {} differ".format(src.name)
            logging.info(msg)
            if not ctx["quiet"]:
                print(msg)
            ctx["differences"] += 1
            return False

    # chunked datasets, compare chunk by chunk
    try:
        it = src.iter_chunks()

        for s in it:
            msg = "checking dataset data for slice: {}".format(s)
            logging.debug(msg)

            arr_src = src[s]
            if len(s) > 0:
                msg = "got src array {}".format(arr_src.shape)
                logging.debug(msg)
            arr_tgt = tgt[s]
            if len(s) > 0:
                msg = "got tgt array {}".format(arr_tgt.shape)
                logging.debug(msg)

            is_equal = True
            if isinstance(arr_src, np.ndarray):
                if isinstance(arr_tgt, np.ndarray):
                    is_equal = np.array_equal(arr_src, arr_tgt)
                else:
                    is_equal = False # type not the same
            else:
                # just compare the objects directly
                if arr_src != arr_tgt:
                    is_equal = False
            
            if not is_equal:
                msg = "values for dataset {} differ for slice: {}".format(src.name, s)
                logging.info(msg)
                if not ctx["quiet"]:
                    print(msg)
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


# ----------------------------------------------------------------------------------
def usage():
    option_names = cfg.get_names()
    cmd = cfg.get_cmd()
    print("Usage:\n")
    print(f"    {cmd} [ OPTIONS ]  hdf5_file  domain")
    print("")
    print("Description:")
    print("    Compare an HDF5 file to a domain")
    print("       hdf5_file: hdf5_file")
    print("       domain: domain")
    print("")
    print("Options:")
    for name in option_names:
        help_msg = cfg.get_help_message(name)
        if help_msg:
            print(f"    {help_msg}")  
    print("")
    print("Examples:")
    print(f"     {cmd} myfile.h5  /home/myfolder/myfile.h5")
    print(f"     {cmd} s3://myybucket/myfile.h5  /home/myfolder/myfile.h5")
    print("")
    print(cfg.get_see_also(cmd))
    print("")
    sys.exit(1)

# end print_usage


# ----------------------------------------------------------------------------------
def print_config_example():
    print("# default")
    print("hs_username = <username>")
    print("hs_password = <passwd>")
    print("hs_endpoint = http://hsdshdflab.hdfgroup.org")


# print_config_example

# ----------------------------------------------------------------------------------
def main():

    cfg.setitem("nodata", False, flags=["--nodata",], help="do not compare dataset data")
    cfg.setitem("noattr", False, flags=["--noattr",], help="do not compare attributes")
    cfg.setitem("quiet", False, flags=["--quiet",], help="surpress normal output")
    cfg.setitem("help", False, flags=["-h", "--help"], help="this message")

    try:
        args = cfg.set_cmd_flags(sys.argv[1:])
    except ValueError as ve:
        print(ve)
        usage()

    if cfg["quiet"] and cfg["verbose"]:
        msg = "--quiet and --verbose options can't be used together"        
        sys.exit(msg)

    if len(args) < 2:
        # need at least source and target
        usage()
    file_path = args[0]
    domain_path = args[1]

    # setup logging
    logfname = cfg["logfile"]
    loglevel = cfg.get_loglevel()
    logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
    logging.debug(f"set log_level to {loglevel}")


    rc = 0
    s3 = None  # s3fs instance

    cfg.print(f"file: {file_path}")
    cfg.print(f"domain: {domain_path}")

    if domain_path[-1] == "/":
        msg = "domain can't be a folder"
        logging.error(msg)
        sys.exit(msg)

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
                msg = f"Error opening file {file_path}: {ioe}"
                logging.error(msg)
                sys.exit(msg)
        else:
            # regular h5py open
            try:
                fin = h5py.File(file_path, mode="r")
            except IOError as ioe:
                msg = f"Error opening file {domain_path}: {ioe}"
                logging.error(msg)
                sys.exit(msg)

        # get the  domain
        try:
            fout = getFile(domain_path)
        except IOError as ioe:
            if ioe.errno == 404:
                msg = f"domain: {domain_path} not found"
                logging.error(msg)
            elif ioe.errno == 403:
                msg = f"No read access to domain: {domain_path}"
                logging.error(msg)
            else:
                msg = f"Error opening file: {domain_path}: {ioe}"
                logging.error(msg)
            sys.exit(msg)

        # do the actual diff
        kwargs = {}
        kwargs["verbose"] = cfg["verbose"]
        kwargs["nodata"] = cfg["nodata"]
        kwargs["noattr"] = cfg["noattr"]
        kwargs["quiet"] = cfg["quiet"]
        rc = diff_file(fin, fout, **kwargs)
    

        if not cfg["quiet"] and rc > 0:
            print(f"{rc} differences found")

        cfg.print(f"diff done for {file_path}")

    except KeyboardInterrupt:
        logging.error("Aborted by user via keyboard interrupt.")
        sys.exit(1)

    sys.exit(rc)


# __main__
if __name__ == "__main__":
    main()
