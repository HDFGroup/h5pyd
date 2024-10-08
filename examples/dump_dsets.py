import sys
import h5py
import h5pyd
import s3fs
from h5pyd import H5Image
import numpy as np
import time
import logging

CHUNKS_PER_PAGE = 8  # chunks_per_page for H5Image
USE_CACHE = False    # use_cache setting for h5pyd.File


def is_dataset(obj):
    # this should work with either h5py or h5pyd
    if obj.__class__.__name__ == "Dataset":
        return True
    else:
        return False


def visit(name, obj):
    if not is_dataset(obj):
        return
    arr = obj[...]  # read entire dataset into numpy array
    arr_mean = np.nan
    arr_min = np.nan
    arr_max = np.nan
    try:
        arr_mean = arr.mean()
        arr_min = arr.min()
        arr_max = arr.max()
    except Exception:
        # ignore errors on datasets that are not numeric
        logging.warning(f"unable to use mean on {name}, dtype: {arr.dtype}")
    print(f"{name:40} mean: {arr_mean:6.2f} min: {arr_min:0.2f} max: {arr_max:6.2f}")


def main():

    loglevel = logging.ERROR
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(f"usage: python {sys.argv[0]} <file_path>")
        sys.exit(1)

    filename = sys.argv[1]

    start_time = time.time()

    if filename.startswith("s3://"):
        # use s3fs for access HDF5 files
        s3 = s3fs.S3FileSystem(anon=True)
        f = h5py.File(s3.open(filename, "rb"))
    elif filename.startswith("hdf5://"):
        # open HSDS domain
        f = h5pyd.File(filename, use_cache=USE_CACHE)
        if "h5image" in f:
            # HDF5 file image, open with h5py and H5Image
            f.close()
            f = h5py.File(H5Image(filename, chunks_per_page=CHUNKS_PER_PAGE))
    else:
        # open HDF5 file
        f = h5py.File(filename)

    try:
        f.visititems(visit)
    except Exception as e:
        logging.error(f"got exection {type(e)}: {e}")

    f.close()

    stop_time = time.time()
    print("")
    print(f"done - {(stop_time - start_time):4.3f} s")


main()
