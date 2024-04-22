import numpy as np
import time

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import subprocess
import re

from h5pyd import MultiManager
import h5pyd as h5py
from common import getTestFileName

# Flag to stop resource usage collection thread after a benchmark finishes
stop_stat_collection = False


def write_datasets_multi(datasets, num_iters=1):
    mm = MultiManager(datasets)
    data = np.reshape(np.arange(np.prod(datasets[0].shape)), datasets[0].shape)

    start = time.time()
    for i in range(num_iters):
        mm[...] = [data] * len(datasets)
    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def write_datasets_serial(datasets, num_iters=1):
    data = np.reshape(np.arange(np.prod(datasets[0].shape)), datasets[0].shape)

    start = time.time()
    for i in range(num_iters):
        for d in datasets:
            d[...] = data
    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def read_datasets_multi(datasets, num_iters=1):
    mm = MultiManager(datasets)

    start = time.time()
    for i in range(num_iters):
        out = mm[...]
        if out is None:
            raise ValueError("Read failed!")

    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def read_datasets_serial(datasets, num_iters=1):
    start = time.time()
    for i in range(num_iters):
        for d in datasets:
            out = d[...]
            if out is None:
                raise ValueError("Read failed!")

    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def read_datasets_multi_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)
    mm = MultiManager(datasets=datasets)

    start = time.time()
    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        out = mm[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]
        if out is None:
            raise ValueError("Read failed!")
    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def read_datasets_serial_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)

    start = time.time()
    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        for d in datasets:
            out = d[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]
            if out is None:
                raise ValueError("Read failed!")
    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def write_datasets_multi_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)
    data_in = np.reshape(np.arange(np.prod(shape)), shape)

    mm = MultiManager(datasets=datasets)

    start = time.time()
    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        write_data = data_in[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]
        mm[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]] = [write_data] * count
    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def write_datasets_serial_selections(datasets, num_iters=1):
    shape = datasets[0].shape
    rank = len(shape)
    data_in = np.reshape(np.arange(np.prod(shape)), shape)

    start = time.time()
    for i in range(num_iters):
        # Generate random selection
        sel = np.random.randint(0, shape[0], size=rank * 2)
        write_data = data_in[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]]

        for d in datasets:
            d[sel[0]:sel[1], sel[2]:sel[3], sel[4]:sel[5]] = write_data
    end = time.time()
    avg_time = (end - start) / num_iters

    return avg_time


def test_thread_error(f):
    dset1 = f.create_dataset("d1", data=np.arange(100), shape=(100,), dtype=np.int32)
    dset2 = f.create_dataset("d2", data=np.reshape(np.arange(100), (10, 10)), shape=(10, 10), dtype=np.int32)
    mm = MultiManager([dset1, dset2])
    out = mm[0:15, 0:15]  # Only valid for dset 2
    print(out)
    return out


def get_docker_stats(test_name):
    global stop_stat_collection
    sn_stat_instances = 0
    dn_stat_instances = 0
    sn_count = 0
    dn_count = 0

    if test_name in stats:
        raise ValueError(f"Test name conflict on name \"{test_name}\"")

    test_stats = {"time": 0.0, "dn_cpu": 0.0, "dn_mem": 0.0, "sn_cpu": 0.0, "sn_mem": 0.0}

    while True:
        if stop_stat_collection:
            stop_stat_collection = False
            return test_stats

        stats_out = subprocess.check_output(['docker', 'stats', '--no-stream'])

        lines = stats_out.splitlines()

        # Count SNs and DNs on first stat check
        if sn_count == 0:
            for line in lines[1:]:
                line = line.decode('utf-8')
                # Replace all substrings of whitespace with single space
                line = re.sub(" +", " ", line)
                words = line.split(' ')
                container_name = words[1]

                if "_dn_" in container_name:
                    dn_count += 1
                elif "_sn_" in container_name:
                    sn_count += 1

        for line in lines[1:]:
            line = line.decode('utf-8')
            # Replace all substrings of whitespace with single space
            line = re.sub(" +", " ", line)
            words = line.split(' ')

            container_name = words[1]
            cpu_percent = float((words[2])[:-1])
            mem_percent = float((words[6])[:-1])

            # Update average usage values
            if "_dn_" in container_name:
                dn_stat_instances += 1
                ratio = (dn_stat_instances - 1) / dn_stat_instances
                test_stats["dn_cpu"] = (test_stats["dn_cpu"] * ratio) + cpu_percent / dn_stat_instances
                test_stats["dn_mem"] = (test_stats["dn_mem"] * ratio) + mem_percent / dn_stat_instances
            elif "_sn_" in container_name:
                sn_stat_instances += 1
                ratio = (sn_stat_instances - 1) / sn_stat_instances
                test_stats["sn_cpu"] = (test_stats["sn_cpu"] * ratio) + cpu_percent / sn_stat_instances
                test_stats["sn_mem"] = (test_stats["sn_mem"] * ratio) + mem_percent / sn_stat_instances
            else:
                # Ignore other docker containers
                pass

        # Query docker for stats once per second
        time.sleep(1)


def run_benchmark(test_name, test_func, stats, datasets, num_iters):
    global stop_stat_collection
    # For each section, execute docker resource usage readout at simultaneously on a second thread
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        futures.append(executor.submit(test_func, datasets, num_iters))
        futures.append(executor.submit(get_docker_stats, test_name))
        time_elapsed = 0.0

        for f in as_completed(futures):
            try:
                ret = f.result()
                if isinstance(ret, float):
                    # Benchmark returned; terminate docker stats computation
                    time_elapsed = ret
                    stop_stat_collection = True
                elif isinstance(ret, dict):
                    # Stat collection returned
                    stats[test_name] = ret
                    stats[test_name]["time"] = time_elapsed

            except Exception as exc:
                executor.shutdown(wait=False)
                raise ValueError(f"Error during benchmark threading for {test_name}: {exc}")


if __name__ == '__main__':
    print("Executing multi read/write benchmark")
    shape = (100, 100, 100)
    count = 4 # 64
    num_iters = 50
    dt = np.int32
    stats = {}

    fs = []

    for i in range(count):
        filename = getTestFileName(f"bm_{i:04d}", subfolder="multi_bm")
        f = h5py.File(filename, mode='w')
        fs.append(f)

    data_in = np.zeros(shape, dtype=dt)
    datasets = [f.create_dataset("data", shape, dtype=dt, data=data_in) for f in fs]

    print(f"Created {count} datasets, each with {np.prod(shape)} elements")
    print(f"Benchmarks will be repeated {num_iters} times")

    print("Testing with multiple HTTP Connections...")

    run_benchmark("Read Multi (Multiple HttpConn)", read_datasets_multi, stats, datasets, num_iters)
    run_benchmark("Read Serial (Multiple HttpConn)", read_datasets_serial, stats, datasets, num_iters)

    run_benchmark("Write Multi (Multiple HttpConn)", write_datasets_multi, stats, datasets, num_iters)
    run_benchmark("Write Serial (Multiple HttpConn)", write_datasets_serial, stats, datasets, num_iters)

    print("Testing with shared HTTP connection...")

    filename = getTestFileName("bm_shared", subfolder="multi_bm")
    f = h5py.File(filename, mode='w')
    datasets = [f.create_dataset("data" + str(i), data=data_in, dtype=dt) for i in range(count)]

    run_benchmark("Read Multi (Shared HttpConn)", read_datasets_multi, stats, datasets, num_iters)
    run_benchmark("Read Serial (Shared HttpConn)", read_datasets_serial, stats, datasets, num_iters)

    run_benchmark("Write Multi (Shared HttpConn)", write_datasets_multi, stats, datasets, num_iters)
    run_benchmark("Write Serial (Shared HttpConn)", write_datasets_serial, stats, datasets, num_iters)

    # Display results
    for test_name in stats:
        time_elapsed = stats[test_name]["time"]
        dn_cpu = stats[test_name]["dn_cpu"]
        dn_mem = stats[test_name]["dn_mem"]
        sn_cpu = stats[test_name]["sn_cpu"]
        sn_mem = stats[test_name]["sn_mem"]

        print(f"{test_name} - Time: {(time_elapsed):6.4f}, DN CPU%: {(dn_cpu):6.4f},\
        DN MEM%: {(dn_mem):6.4f}, SN CPU%: {(sn_cpu):6.4f}, SN MEM%: {(sn_mem):6.4f}")
