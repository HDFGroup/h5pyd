##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

from __future__ import absolute_import
import numpy

from .dataset import Dataset
from .objectid import DatasetID


class Cursor():
    """
      Cursor for retreiving rows from a table
      buffer_rows can be used to control how many rows
      will be fetched from the server
    """
    def __init__(self, table, query=None, start=None, stop=None, limit=0, field=None, condvars=None, buffer_rows=None):
        self._table = table
        self._query = query
        DEFAULT_BUFFER_BYTES = 1000000
        if buffer_rows is None:
            buffer_rows = DEFAULT_BUFFER_BYTES // table.dtype.itemsize
        if buffer_rows < 1:
            buffer_rows = 1
        self._buffer_rows = buffer_rows

        if start is None:
            self._start = 0
        else:
            self._start = start
        if stop is None:
            self._stop = table.nrows
        else:
            self._stop = stop
        self._limit = limit
        self._field = field
        self._condvars = condvars

    def __iter__(self):
        """ Iterate over the rows, fetching buffer_rows rows (or the query matches among
        them) per request.

        BEWARE: Modifications to the yielded data are *NOT* written to file.
        """
        rows_read = 0

        for buf_start in range(self._start, self._stop, self._buffer_rows):
            buf_stop = min(buf_start + self._buffer_rows, self._stop)
            slices = (slice(buf_start, buf_stop),)
            if self._query is None:
                arr = self._table.__getitem__(slices)
            else:
                # only the rows in this buffer that match the query
                arr = self._table.__getitem__(slices, query=self._query)
            if arr is None:
                continue
            for row in arr:
                if self._limit > 0 and rows_read >= self._limit:
                    return
                yield row
                rows_read += 1


class Table(Dataset):

    """
        Represents an HDF5 dataset
    """
    def __init__(self, bind, track_order=None, fields=None):
        """ Create a new Table object by binding to a low-level DatasetID.
        """

        if not isinstance(bind, DatasetID):
            raise ValueError(f"{bind} is not a DatasetID")
        Dataset.__init__(self, bind, track_order=track_order)

        if len(self._dtype) < 1:
            raise ValueError("Table type must be compound")

        if self.id.rank > 1:
            raise ValueError("Table must be one-dimensional")

        colnames = []
        for field in self._dtype.descr:
            # each element should be a tuple ('fieldname', dt)
            name = field[0]
            colnames.append(field[0])
        if fields is not None:
            for name in fields:
                if name not in colnames:
                    raise ValueError(f"{name} not found")
            self._fields = fields  # restrict this view of the dataset to just these fields
        else:
            self._fields = colnames

    @property
    def colnames(self):
        """Numpy-style attribute giving the number of dimensions"""

        return self._fields

    @property
    def nrows(self):
        return self.shape[0]

    def read(self, start=0, stop=None, field=None, out=None):
        """Read rows from table
        """
        if stop is None:
            stop = self.shape[0]
        return Cursor(self, start=start, stop=stop, field=field).__iter__()

    def read_where(self, condition, field=None,
                   start=0, stop=None, limit=0):
        """Read rows from table using pytable-style condition
        """
        # unlike a plain hyperslab read, the rows a query matches aren't
        # known ahead of time - flush any pending local changes first so
        # the query runs against a single, consistent (server) state
        # rather than having to reconcile local vs. remote row-by-row
        self.id.db.flush()
        if stop is None:
            stop = self.shape[0]
        kwargs = {'start': start, 'stop': stop, 'query': condition}
        if field is not None:
            kwargs['field'] = field
        if limit > 0:
            kwargs['limit'] = limit
        return Cursor(self, **kwargs).__iter__()

    def update_where(self, condition, value, start=0, stop=None, limit=0):
        """Modify rows in table using pytable-style condition
        """
        if not isinstance(value, dict):
            raise ValueError("expected value to be a dict")
        if stop is None:
            stop = self.shape[0]
        if stop <= start:
            raise ValueError("stop must be greater than start")
        # flush before, for the same reason as read_where (the update
        # starts with the same kind of query, to find matching rows) - and
        # flush after, so the update itself is persisted immediately rather
        # than left as a local-only pending change until some later flush
        self.id.db.flush()
        slices = (slice(start, stop, 1),)
        indices = self.query(condition, selection=slices, update_value=value, limit=limit)
        self.id.db.flush()
        return indices

    def get_where_list(self, condition, start=0, stop=None, limit=0):
        """ Return indices of rows matching the given condition
        """
        if stop is None:
            stop = self.shape[0]
        if stop <= start:
            raise ValueError("stop must be greater than start")
        slices = (slice(start, stop, 1),)
        indices = self.query(condition, selection=slices, limit=limit)
        # cnvert this to a list of ints (rather than a list of list),
        # since we are dealing with a 1-d dataset
        result = [int(index[0]) for index in indices]
        return result

    def append(self, rows):
        """ Append rows to end of table
        """
        self.log.info("Table append")

        count = len(rows)
        # resize the dataset to hold the new rows
        numrows = self.shape[0]
        self.resize((numrows + count,))
        self[numrows:numrows + count] = rows
