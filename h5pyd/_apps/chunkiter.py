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


import numpy as np

CHUNK_BASE = 16*1024    # Multiplier by which chunks are adjusted
CHUNK_MIN = 8*1024      # Soft lower limit (8k)
CHUNK_MAX = 1024*1024   # Hard upper limit (1M)

def guess_chunk(shape, maxshape, typesize):
    """ Guess an appropriate chunk layout for a dataset, given its shape and
    the size of each element in bytes.  Will allocate chunks only as large
    as MAX_SIZE.  Chunks are generally close to some power-of-2 fraction of
    each axis, slightly favoring bigger values for the last index.

    Undocumented and subject to change without warning.
    """
    # pylint: disable=unused-argument

    # For unlimited dimensions we have to guess 1024
    shape = tuple((x if x!=0 else 1024) for i, x in enumerate(shape))

    ndims = len(shape)
    if ndims == 0:
        raise ValueError("Chunks not allowed for scalar datasets.")

    chunks = np.array(shape, dtype='=f8')
    if not np.all(np.isfinite(chunks)):
        raise ValueError("Illegal value in chunk tuple")

    # Determine the optimal chunk size in bytes using a PyTables expression.
    # This is kept as a float.
    dset_size = np.product(chunks)*typesize
    target_size = CHUNK_BASE * (2**np.log10(dset_size/(1024.*1024)))

    if target_size > CHUNK_MAX:
        target_size = CHUNK_MAX
    elif target_size < CHUNK_MIN:
        target_size = CHUNK_MIN

    idx = 0
    while True:
        # Repeatedly loop over the axes, dividing them by 2.  Stop when:
        # 1a. We're smaller than the target chunk size, OR
        # 1b. We're within 50% of the target chunk size, AND
        #  2. The chunk is smaller than the maximum chunk size

        chunk_bytes = np.product(chunks)*typesize

        if (chunk_bytes < target_size or \
         abs(chunk_bytes-target_size)/target_size < 0.5) and \
         chunk_bytes < CHUNK_MAX:
            break

        if np.product(chunks) == 1:
            break  # Element size larger than CHUNK_MAX

        chunks[idx%ndims] = np.ceil(chunks[idx%ndims] / 2.0)
        idx += 1

    return tuple(int(x) for x in chunks)
    
class ChunkIterator:
    """
    Class to iterate through list of chunks given h5py dset
    """
    def __init__(self, dset):
        self._shape = dset.shape
        if dset.chunks is None:
             # guess a chunk shape
            self._layout =  guess_chunk(self._shape, None, dset.dtype.itemsize)
            print(f"guess chunk: {self._layout}")
        elif isinstance(dset.chunks, dict):
            self._layout = dset.chunks["dims"]
        else:
            self._layout = dset.chunks
        self._rank = len(dset.shape)
        if self._rank == 0:
            self._chunk_index = [0,]
        else:
            self._chunk_index = [0,] * self._rank

    def __iter__(self):
        return self

    def __next__(self):

        def get_ret(item):
            if len(item) == 1:
                return item[0]
            else:
                return tuple(item)
        if self._layout == ():
            # special case for scalar datasets
            if self._chunk_index[0] > 0:
                raise StopIteration()
            self._chunk_index[0] += 1
            return ()

        slices = []
        if self._chunk_index[0] * self._layout[0] >= self._shape[0]:
            # ran past the last chunk, end iteration
            raise StopIteration()

        for dim in range(self._rank):
            start = self._chunk_index[dim] * self._layout[dim]
            stop = start + self._layout[dim]
            if stop > self._shape[dim]:
                stop = self._shape[dim]  # trim to end of dimension
            s = slice(start, stop, 1)
            slices.append(s)

        # bump up the last index and carry forward if we run outside the selection
        dim = self._rank - 1
        while dim >= 0:
            c = self._layout[dim]
            self._chunk_index[dim] += 1

            chunk_end = self._chunk_index[dim] * c
            if chunk_end < self._shape[dim]:
                # we still have room to extend along this dimensions
                return get_ret(slices)

            if dim > 0:
                # reset to the start and continue iterating with higher dimension
                self._chunk_index[dim] = 0
            dim -= 1
        return get_ret(slices)
