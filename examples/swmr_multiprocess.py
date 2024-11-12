"""
    Demonstrate the use of h5pyd or h5py in SWMR mode to write to a dataset (appending)
    from one process while monitoring the growing dataset from another process.

    Usage:
        swmr_multiprocess.py [FILENAME] [BLOCKSIZE] [LOOPCOUNT] [COMPRESSION]

        FILENAME:  name of file or HSDS domain to monitor. Default: swmrmp.h5,
        BLOCKSIZE: number of elements to write to the dataset in each loop iteration. Default: 4
        LOOPCOUNT: number of loop iterations.  Default: 5
        COMPRESSION: compression filter to use.  Default: None

    To utilize h5pyd and HSDS use the hdf5:// prefix.  e.g.: hdf5://home/test_user1/swmrmp.h5

    This script will start up two processes: a writer and a reader. The writer
    will open/create the file (FILENAME) in SWMR mode, create a dataset and start
    appending data to it. After each append the dataset is flushed and an event
    sent to the reader process. Meanwhile the reader process will wait for events
    from the writer and when triggered it will refresh the dataset and read the
    current shape of it.  After the last iteration's data has been read, statistics for
    elapsed time, amount of data transfered, and data transfer speed will be printed.
"""

import sys
import time
import numpy as np
import logging
from multiprocessing import Process, Event
import h5py
import h5pyd


class SwmrReader(Process):
    def __init__(self, event, fname, dsetname, block_size, loop_count, sleep_time=0.1):
        super().__init__()
        self._event = event
        self._fname = fname
        self._dsetname = dsetname
        self._total_rows = block_size * loop_count
        self._sleep_time = sleep_time
        self._timeout = 5

    def run(self):
        self.log = logging.getLogger('reader')
        self.log.info("Waiting for initial event")
        assert self._event.wait(self._timeout)
        self._event.clear()

        self.log.info(f"Opening file {self._fname}")
        if self._fname.startswith("hdf5://"):
            f = h5pyd.File(self._fname, 'r', libver='latest', swmr=True)
        else:
            f = h5py.File(self._fname, 'r', libver='latest', swmr=True)

        assert f.swmr_mode
        dset = f[self._dsetname]
        last_count = 0
        try:
            # monitor and read loop
            while True:
                self.log.debug("Refreshing dataset")
                dset.refresh()
                row_count = dset.shape[0]
                if row_count > last_count:
                    self.log.info(f"Read {row_count - last_count} rows added")
                    if row_count > last_count + block_size:
                        # This selection should have data updated after a resize
                        arr = dset[last_count:(last_count + block_size)]
                        self.log.info(f"Read data read, min: {arr.min()} max: {arr.max()}")
                    last_count = row_count
                else:
                    self.log.info(f"Read - sleeping for {self._sleep_time}")
                    time.sleep(self._sleep_time)  # no updates so sleep for a bit
                if row_count >= self._total_rows:
                    self.log.info("Read - all data consumed")
                    break
        finally:
            f.close()


class SwmrWriter(Process):
    def __init__(self, event, fname, dsetname, block_size, loop_count, compression):
        super().__init__()
        self._event = event
        self._fname = fname
        self._dsetname = dsetname
        self._block_size = block_size
        self._loop_count = loop_count
        self._compression = compression

    def run(self):
        self.log = logging.getLogger('writer')
        self.log.info(f"Creating file {self._fname}")

        if self._fname.startswith("hdf5://"):
            f = h5pyd.File(self._fname, 'w', libver='latest')
        else:
            f = h5py.File(self._fname, 'w', libver='latest')

        try:
            kwargs = {"dtype": np.dtype("int64"), "chunks": (1024 * 256,), "maxshape": (None,)}
            if compression:
                kwargs["compression"] = compression
            dset = f.create_dataset(self._dsetname, (0,), **kwargs)
            assert not f.swmr_mode

            self.log.info("SWMR mode")
            f.swmr_mode = True
            assert f.swmr_mode
            self.log.debug("Sending initial event")
            self._event.set()

            # Write loop
            for i in range(self._loop_count):
                new_shape = ((i + 1) * self._block_size,)
                self.log.info(f"Resizing dset shape: {new_shape}")
                dset.resize(new_shape)
                self.log.debug("Writing data")
                dset[i * self._block_size:] = np.random.randint(0, high=100, size=self._block_size)
                # dset.write_direct( arr, np.s_[:], np.s_[i*len(arr):] )
                if isinstance(dset.id.id, int):
                    # flush operation is only required for h5py
                    self.log.debug("Flushing data")
                    dset.flush()
            # add one extra row to trigger last data read
            new_shape = ((self._loop_count * self._block_size) + 1,)
            dset.resize(new_shape)
        finally:
            f.close()


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)10s  %(asctime)s  %(name)10s  %(message)s', level=logging.INFO)
    fname = 'swmrmp.h5'
    dsetname = 'data'
    block_size = 4
    loop_count = 5
    compression = None
    if len(sys.argv) > 1:
        if sys.argv[1] in ("-h", "--help"):
            print(f"usage: {sys.argv[0]} [filename] [blocksize] [loopcount] [compression]")
            sys.exit(0)
        fname = sys.argv[1]
        if not fname.endswith(".h5"):
            print("use .h5 extension for filename")
            sys.exit(0)
    if len(sys.argv) > 2:
        block_size = int(sys.argv[2])
    if len(sys.argv) > 3:
        loop_count = int(sys.argv[3])
    if len(sys.argv) > 4:
        compression = sys.argv[4]

    event = Event()
    reader = SwmrReader(event, fname, dsetname, block_size, loop_count)
    writer = SwmrWriter(event, fname, dsetname, block_size, loop_count, compression)

    start_time = time.time()

    logging.info("Starting reader")
    reader.start()
    logging.info("Starting writer")
    writer.start()

    logging.info("Waiting for writer to finish")
    writer.join()
    logging.info("Waiting for reader to finish")
    reader.join()

    finish_time = time.time()
    bytes_read = block_size * loop_count * np.dtype("int64").itemsize
    kb = 1024
    mb = kb * 1024
    gb = mb * 1024
    if bytes_read > gb:
        data_read = f"{bytes_read / gb:6.2f} GB"
    elif bytes_read > mb:
        data_read = f"{bytes_read / mb:6.2f} MB"
    elif bytes_read > kb:
        data_read = f"{bytes_read / kb:6.2f} KB"
    else:
        data_read = f"{bytes_read} bytes"
    elapsed_time = finish_time - start_time
    mb_per_sec = bytes_read / (elapsed_time * mb)
    print("done!")
    print(f"    elapsed time: {elapsed_time:6.2f} sec")
    print(f"    data copied:  {data_read}")
    print(f"    throughput:   {mb_per_sec:6.2f} mb/sec")
