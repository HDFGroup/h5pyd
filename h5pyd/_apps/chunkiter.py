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
import six
class ChunkIterator:
    """
    Class to iterate through list of chunks given h5py dset
    """
    def __init__(self, dset):       
        self._shape = dset.shape
        if dset.chunks is None:
            # treat the dataset as one chunk
            self._layout = dset.shape 
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
        if self._layout is ():
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
        
    if six.PY2:
        next = __next__

