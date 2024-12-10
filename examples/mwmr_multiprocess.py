"""
    Demonstrate the use of  multiple processes to write to a HSDS domain

    Usage:
        mwmr_multiprocess.py FILENAME [dataset_count] [process_count] [attr_count] [sn_count]

        FILENAME:  name of HSDS domain to write to
        dataset_count: number of datasets to create. Default: 10
        process_count: number of processes. Default: 2
        attr_count: numberof attributes per dataset.  Default: 100
        sn_count: number of HSDS SN nodes to target. Default: 1


    This script will start up the specified number of processes.  Each writer to
    create one or more datasets and then write attr_count attributes to each dataset.

    Note: if sn count is greater than 1, start HSDS with the options: "./runall.sh N N".
    where N is sn_count.  This will create N SN processes and N DN processes.
"""

import sys
import time
import logging
from multiprocessing import Process
import h5pyd


class H5Writer(Process):
    def __init__(self, endpoint, fname, dsetnames, attr_count):
        super().__init__()
        self._endpoint = endpoint
        self._fname = fname
        self._dsetnames = dsetnames
        self._attr_count = attr_count
        self.log = logging.getLogger('writer')

        self.log.info(f"process start, dsetnames: {dsetnames}")

    def run(self):
        self.log = logging.getLogger('writer')
        self.log.info(f"Creating file {self._fname} with endpoint: {self._endpoint}")
        f = h5pyd.File(self._fname, mode='a', endpoint=self._endpoint)

        for dsetname in self._dsetnames:
            self.log.info(f"creating dataset: {dsetname}")
            dset = f.create_dataset(dsetname, (0,), dtype="i4")

            self.log.info(f"dataset {dsetname} created")

            # Write attributes
            for i in range(self._attr_count):
                attr_name = f"a{i:06d}"
                dset.attrs[attr_name] = f"This is attribute {attr_name} in dataset {dsetname}"

        # all done - close the file handle
        f.close()


#
# main
#
if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)10s  %(asctime)s  %(name)10s  %(message)s', level=logging.ERROR)
    domain_name = None

    dataset_count = 10
    proc_count = 2
    attr_count = 100
    sn_count = 1

    # change this if this is not the host and/or port HSDS is running on
    base_port = 5101
    host = "localhost"

    usage = f"usage: {sys.argv[0]} domain [dataset_count] [proc_count] [attr_count] [sn_count]"

    if len(sys.argv) > 1:
        if sys.argv[1] in ("-h", "--help"):
            print(usage)
            sys.exit(0)
        fname = sys.argv[1]
    if len(sys.argv) > 2:
        dataset_count = int(sys.argv[2])
    if len(sys.argv) > 3:
        proc_count = int(sys.argv[3])
    if len(sys.argv) > 4:
        attr_count = int(sys.argv[4])
    if len(sys.argv) > 5:
        sn_count = int(sys.argv[5])

    if not fname:
        print(usage)
        sys.exit(0)

    print(f"dataset_count: {dataset_count}")
    print(f"proc_count: {proc_count}")
    print(f"attr_count: {attr_count}")
    print(f"sn count: {sn_count}")

    # create a list of empty lists
    # this will hold the dataset names for each process
    dset_names = []
    for _ in range(proc_count):
        dset_names.append([])

    # split up the set of dataset names into roughly equal groups per process
    next = 0
    for i in range(dataset_count):
        dset_name = f"dset_{i:06d}"
        dset_names[next].append(dset_name)
        next = (next + 1) % proc_count

    # create domain here with 'w' mode to re-initialized in case already created
    f = h5pyd.File(fname, 'w')

    start_time = time.time()
    logging.info("Starting writers")
    writers = []
    for i in range(proc_count):
        port = base_port + (i % sn_count)
        endpoint = f"http://{host}:{port}"
        writer = H5Writer(endpoint, fname, dset_names[i], attr_count)
        writer.start()
        writers.append(writer)

    logging.info("Waiting for writers to finish")

    for i in range(proc_count):
        writer = writers[i]
        writer.join()  # block till this process is done

    finish_time = time.time()
    elapsed_time = finish_time - start_time
    print("done!")
    print(f"    elapsed time: {elapsed_time:6.2f} sec")
    f.close()
