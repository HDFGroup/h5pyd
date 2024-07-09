import h5pyd
import numpy as np
import random
import time

DOMAIN_PATH = "/home/test_user1/test/multi_mgr_benchmark.h5"
DATASET_COUNT = 200
DSET_SHAPE = (10,)
DSET_DTYPE = np.int32


def generate_range(ds_shape: tuple):
    # generate a tuple of random indices for one dataset
    indices = []
    for axis_length in ds_shape:
        index = random.randint(0, axis_length - 1)
        indices.append(index)
    return tuple(indices)


def generate_index_query(h5file):
    # generate a list of index tuples
    query = []
    for ds in h5file.values():
        ds_shape = ds.shape
        indices = generate_range(ds_shape)
        query.append(indices)
    return query


def benchmark_multimanager(h5file, num=10):
    """
    Benchmark retrieving one random entry from every dataset in an h5file
    using the MultiManager.
    """
    ds_names = list(h5file.keys())
    datsets = [h5file[name] for name in ds_names]
    mm = h5pyd.MultiManager(datsets)

    # prepare queries to exclude from runtime
    queries = []
    for i in range(num):
        query = generate_index_query(h5file)
        queries.append(query)

    # accessing the data
    t0 = time.time()
    for query in queries:
        mm[query]

    runtime = time.time() - t0
    print(f"Mean runtime multimanager: {runtime/num:.4f} s")
    # 100ms for case with 6 datasets


def benchmark_sequential_ds(h5file, num=10):
    """
    Benchmark retrieving one random entry from every dataset in
    an h5file by sequentially looping through the datasets
    """
    # prepare queries to exclude this code from runtime
    index_lists = []
    for i in range(num):
        index_list = []
        for ds in h5file.values():
            indices = generate_range(ds.shape)
            index_list.append(indices)
        index_lists.append(index_list)

    # accessing the data
    t0 = time.time()
    for index_list in index_lists:
        for indices, ds in zip(index_list, h5file.values()):
            ds[indices]

    runtime = time.time() - t0
    print(f"Mean runtime sequentially: {runtime/num:.4f} s")
    # ~ 400ms for case with 6 datasests


def run_benchmark(f):
    """
    Initialize datasets if not done previously
    Then run sequential and multimanager tests
    """

    for i in range(DATASET_COUNT):
        dset_name = f"dset_{i:04d}"
        if dset_name not in f:
            data = np.random.randint(0, 100, size=DSET_SHAPE, dtype=DSET_DTYPE)
            f.create_dataset(dset_name, data=data)

    benchmark_sequential_ds(f)

    benchmark_multimanager(f)


#
# main
#

# create domain if it does not exist already
with h5pyd.File(DOMAIN_PATH, "a") as f:
    run_benchmark(f)
