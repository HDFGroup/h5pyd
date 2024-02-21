import numpy as np
import time

from h5pyd._hl.dataset import MultiManager
import h5pyd as h5py


def write_datasets_multi(datasets, num_iters=1):
    mm = MultiManager(datasets)
    data = np.reshape(np.arange(np.prod(datasets[0].shape)), datasets[0].shape)
    for i in range(num_iters):
        mm[...] = [data] * len(datasets)


def write_datasets_serial(datasets, num_iters=1):
    data = np.reshape(np.arange(np.prod(datasets[0].shape)), datasets[0].shape)
    for i in range(num_iters):
        for d in datasets:
            d[...] = data


def read_datasets_multi(datasets, num_iters=1):
    mm = MultiManager(datasets)
    for i in range(num_iters):
        out = mm[...]
    return out


def read_datasets_serial(datasets, num_iters=1):
    for i in range(num_iters):
        for d in datasets:
            out = d[...]
    return out


def read_datasets_multi_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)
    mm = MultiManager(datasets=datasets)

    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        out = mm[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]

    return out


def read_datasets_serial_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)

    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        for d in datasets:
            out = d[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]

    return out


def write_datasets_multi_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)
    data_in = np.reshape(np.arange(np.prod(shape)), shape)

    mm = MultiManager(datasets=datasets)

    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        write_data = data_in[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]
        mm[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]] = [write_data] * count


def write_datasets_serial_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)
    data_in = np.reshape(np.arange(np.prod(shape)), shape)

    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        write_data = data_in[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]

        for d in datasets:
            d[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]] = write_data


def test_thread_error(f):
    dset1 = f.create_dataset("d1", data=np.arange(100), shape=(100,), dtype=np.int32)
    dset2 = f.create_dataset("d2", data=np.reshape(np.arange(100), (10, 10)), shape=(10, 10), dtype=np.int32)
    mm = MultiManager([dset1, dset2])
    out = mm[0:15, 0:15]  # Only valid for dset 2
    print(out)
    return out


if __name__ == '__main__':
    print("Executing multi read/write benchmark")
    shape = (100, 100, 100)
    count = 64
    num_iters = 50
    dt = np.int32

    fs = [h5py.File("/home/test_user1/h5pyd_multi_bm_" + str(i), mode='w') for i in range(count)]
    data_in = np.zeros(shape, dtype=dt)
    datasets = [f.create_dataset("data", shape, dtype=dt, data=data_in) for f in fs]

    print("Dataset creation finished")

    print("Testing with multiple HTTP Connections")

    now = time.time()
    read_datasets_multi(datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg multi time to read from {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    now = time.time()
    read_datasets_serial(datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg serial time to read from {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    now = time.time()
    write_datasets_multi(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg multi time to write to {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    now = time.time()
    write_datasets_serial(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg serial time to write to {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    print("Testing random selections with multiple connections")

    now = time.time()
    read_datasets_multi_selections(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters
    print(f"Avg multi time to read from random selections = {(avg_time):6.4f}")

    now = time.time()
    read_datasets_serial_selections(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters
    print(f"Avg serial time to read from random selections = {(avg_time):6.4f}")

    now = time.time()
    write_datasets_multi_selections(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters
    print(f"Avg multi time to write to random selections = {(avg_time):6.4f}")

    now = time.time()
    write_datasets_serial_selections(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters
    print(f"Avg serial time to write to random selections = {(avg_time):6.4f}")

    print("Testing with shared HTTP connection")

    f = h5py.File("/home/test_user1/h5pyd_multi_bm_shared", mode='w')
    datasets = [f.create_dataset("data" + str(i), data=data_in, dtype=dt) for i in range(count)]

    now = time.time()
    read_datasets_multi(datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg multi time to read from {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    now = time.time()
    read_datasets_serial(datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg serial time to read from {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    now = time.time()
    write_datasets_multi(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg multi time to write to {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")

    now = time.time()
    write_datasets_serial(datasets=datasets, num_iters=num_iters)
    then = time.time()
    avg_time = (then - now) / num_iters

    print(f"Avg serial time to write to {np.prod(shape)} elems in {count} datasets = {(avg_time):6.4f}")
