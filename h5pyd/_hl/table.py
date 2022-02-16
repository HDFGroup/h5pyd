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
from .base import  _decode   
from .base import bytesToArray
from .dataset import Dataset
from .objectid import DatasetID
from . import selections as sel
from .h5type import Reference
from .h5type import check_dtype
from .h5type import getQueryDtype   


class Cursor():
    """
      Cursor for retreiving rows from a table
      buffer_rows can be used to control how many rows
      will be fetched from the server
    """
    def __init__(self, table, query=None, start=None, stop=None, buffer_rows=None):
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

    def __iter__(self):
        """ Iterate over the first axis.  TypeError if scalar.

        BEWARE: Modifications to the yielded data are *NOT* written to file.
        """
        nrows = self._stop - self._start

        arr = None
        query_complete = False

        for indx in range(self._stop - self._start):
            if indx % self._buffer_rows == 0:
                # grab another buffer
                read_count = self._buffer_rows
                if nrows - indx < read_count:
                    read_count = nrows - indx
                if self._query is None:
                    arr = self._table[indx+self._start:read_count+indx+self._start]
                else:
                    # call table to return query result
                    if query_complete:
                        arr = None  # nothing more to fetch
                    else:
                        arr = self._table.read_where(self._query, start=indx+self._start, limit=read_count)
                        if arr is not None and arr.shape[0] < read_count:
                            query_complete = True  # we've gotten all the rows
            if arr is not None and indx%self._buffer_rows < arr.shape[0]:
                yield arr[indx%self._buffer_rows]

class Table(Dataset):

    """
        Represents an HDF5 dataset
    """
    def __init__(self, bind):
        """ Create a new Table object by binding to a low-level DatasetID.
        """

        if not isinstance(bind, DatasetID):
            raise ValueError("%s is not a DatasetID" % bind)
        Dataset.__init__(self, bind)

        if len(self._dtype) < 1:
            raise ValueError("Table type must be compound")

        if len(self._shape) > 1:
            raise ValueError("Table must be one-dimensional")


    @property
    def colnames(self):
        """Numpy-style attribute giving the number of dimensions"""
        names = []
        for field in self._dtype.descr:
            # each element should be a tuple ('fieldname', dt)
            names.append(field[0])
        return names

    @property
    def nrows(self):
        return self._shape[0]

    def read(self, start=None, stop=None, step=None, field=None, out=None):
        if start is None:
            start = 0
        if stop is None:
            stop = self._shape[0]
        if step is None:
            step = 1
        arr = self[start:stop:step]
        if field is not None:
            #TBD - read just the field once the service supports it
            tmp = arr[field]
            arr = tmp
        if out is not None:
            # TBD - read direct
            numpy.copyto(out, arr)
        else:
            return arr



    def read_where(self, condition, condvars=None, field=None, start=None, stop=None, step=None, limit=0, include_index=True):
        """Read rows from table using pytable-style condition
        """
        names = ()  # todo
        def readtime_dtype(basetype, names):
            """ Make a NumPy dtype appropriate for reading """

            if len(names) == 0:  # Not compound, or we want all fields
                return basetype

            if basetype.names is None:  # Names provided, but not compound
                raise ValueError("Field names only allowed for compound types")

            for name in names:  # Check all names are legal
                if not name in basetype.names:
                    raise ValueError("Field %s does not appear in this type." % name)

            return numpy.dtype([(name, basetype.fields[name][0]) for name in names])

        new_dtype = getattr(self._local, 'astype', None)
        if new_dtype is not None:
            mtype = readtime_dtype(new_dtype, names)
        else:
            # This is necessary because in the case of array types, NumPy
            # discards the array information at the top level.
            mtype = readtime_dtype(self.dtype, names)
        # todo - will need the following once we have binary transfers
        # mtype = h5t.py_create(new_dtype)
        rsp_type = getQueryDtype(mtype)
        

        # Perform the dataspace selection
        if start or stop:
            if not start:
                start = 0
            if not stop:
                stop = self._shape[0]
        else:
            start = 0
            stop = self._shape[0]

        selection_arg = slice(start, stop)
        selection = sel.select(self, selection_arg)

        if selection.nselect == 0:
            return numpy.ndarray(selection.mshape, dtype=mtype)

        # setup for pagination in case we can't read everthing in one go
        data = []  # one ndarray for each request response
        cursor = start
        page_size = stop - start
        total_rows = 0

        while True:
            # Perfom the actual read
            req = "/datasets/" + self.id.uuid + "/value"
            params = {}
            params["query"] = condition
            if limit > 0:
                params["Limit"] = limit - total_rows
            self.log.info("req - cursor: {} page_size: {}".format(cursor, page_size))
            end_row = cursor+page_size
            if end_row > stop:
                end_row = stop
            selection_arg = slice(cursor, end_row)
            selection = sel.select(self, selection_arg)

            sel_param = selection.getQueryParam()
            self.log.debug("query param: {}".format(sel_param))
            if sel_param:
                params["select"] = sel_param
            try:
                self.log.debug("params: {}".format(params))
                rsp = self.GET(req, params=params)
                if isinstance(rsp, bytes):
                    # binary response
                    arr = bytesToArray(rsp, rsp_type, None) 
                    count = len(arr)   
                    self.log.info(f"got {count} rows binary data")
                else:
                    values = rsp["value"]
                    count = len(values)
                    if "index" in rsp:
                        # older server version that returns index as a seperate key
                        indices = rsp["index"]
                        if len(indices) != count:
                            raise ValueError(f"expected {count} indicies, but got: {len(indices)}")
                    else:
                        indices = None
                    count = len(values)
                    self.log.info(f"got {count} rows json data")
                    # convert to numpy array
                    arr = numpy.empty((count,), dtype=rsp_type)
                    for i in range(count):
                        if indices is not None:
                            e = [indices[i],]
                            e.extend(values[i])
                        else:
                            e = values[i]
                        arr[i] = tuple(e)
                        
                    self.log.info("got {} rows".format(count))
                total_rows += count
                data.append(arr)

                # advance to next page
                cursor += page_size
            except IOError as ioe:
                if ioe.errno == 413 and page_size > 1024:
                    # too large a query target, try reducing the page size
                    # if it is not already relatively small (1024)
                    page_size //= 2
                    page_size += 1  # bump up to avoid tiny pages in the last iteration
                    self.log.info("Got 413, reducing page_size to: {}".format(page_size))
                else:
                    # otherwise, just raise the exception
                    self.log.info("Unexpected exception: {}".format(ioe.errno))
                    raise ioe
            if cursor >= stop or (limit > 0 and total_rows == limit):
                self.log.info("completed iteration, returning: {} rows".format(len(data)))
                break

        # need some special conversion for compound types --
        # each element must be a tuple, but the JSON decoder
        # gives us a list instead.

        if len(data) == 0:
            raise ValueError("unexpected list size")
        # combine arrays
        if len(data) > 1:
            ret_arr = numpy.empty((total_rows,), dtype=rsp_type)
            start = 0
            for arr in data:
                nrows = len(arr)
                ret_arr[start:(start+nrows)] = arr[:]
                start += nrows
        else:
            ret_arr = data[0]

        return ret_arr


    def update_where(self, condition, value, start=None, stop=None, step=None, limit=None):
        """Modify rows in table using pytable-style condition
        """
        if not isinstance(value, dict):
            raise ValueError("expected value to be a dict")

        # Perform the dataspace selection
        if start or stop:
            if not start:
                start = 0
            if not stop:
                stop = self._shape[0]
        else:
            start = 0
            stop = self._shape[0]

        selection_arg = slice(start, stop)
        selection = sel.select(self, selection_arg)
        sel_param = selection.getQueryParam()
        params = {}
        params["query"] = condition
        if limit:
            params["Limit"] = limit
        self.log.debug(f"query param: {sel_param}")
        if sel_param:
            params["select"] = sel_param

        req = "/datasets/" + self.id.uuid + "/value"

        rsp = self.PUT(req, body=value, format="json", params=params)
        indices = None
        arr = None
        if "index" in rsp:
            indices = rsp["index"]
        elif "value" in rsp:
            # new-style return type - index is first element in each row
            indices = []
            for row in rsp["value"]:
                indices.append(row[0])
        else:
            raise ValueError("unexpected response from PUT query")
        if indices:
            arr = numpy.array(indices)

        return arr

    def create_cursor(self, condition=None,  start=None, stop=None):
        """Return a cursor for iteration
        """
        return Cursor(self, query=condition, start=start, stop=stop)



    def append(self, rows):
        """ Append rows to end of table
        """
        self.log.info("Table append")
        if not self.id.uuid.startswith("d-"):
            # Append ops only work with HSDS
            raise ValueError("append not supported")

        if self._item_size != "H5T_VARIABLE":
            use_base64 = True   # may need to set this to false below for some types
        else:
            use_base64 = False  # never use for variable length types
            self.log.debug("Using JSON since type is variable length")

        val = rows  # for compatibility with dataset code...
        # get the val dtype if we're passed a numpy array
        val_dtype = None
        try:
            val_dtype = val.dtype
        except AttributeError:
            pass # not a numpy object, just leave dtype as None

        if isinstance(val, Reference):
            # h5pyd References are just strings
            val = val.tolist()

        # Generally we try to avoid converting the arrays on the Python
        # side.  However, for compound literals this is unavoidable.
        # For h5pyd, do extra check and convert type on client side for efficiency
        vlen = check_dtype(vlen=self.dtype)
        if vlen is not None and vlen not in (bytes, str):
            self.log.debug("converting ndarray for vlen data")
            try:
                val = numpy.asarray(val, dtype=vlen)
            except ValueError:
                try:
                    val = numpy.array([numpy.array(x, dtype=vlen)
                                       for x in val], dtype=self.dtype)
                except ValueError:
                    pass
            if vlen == val_dtype:
                if val.ndim > 1:
                    tmp = numpy.empty(shape=val.shape[:-1], dtype=object)
                    tmp.ravel()[:] = [i for i in val.reshape(
                        (numpy.product(val.shape[:-1]), val.shape[-1]))]
                else:
                    tmp = numpy.array([None], dtype=object)
                    tmp[0] = val
                val = tmp

        elif isinstance(val, numpy.ndarray):
            # convert array if needed
            # TBD - need to handle cases where the type shape is different
            self.log.debug("got numpy array")
            if val.dtype != self.dtype and val.dtype.shape == self.dtype.shape:
                self.log.info("converting {} to {}".format(val.dtype, self.dtype))
                # convert array
                tmp = numpy.empty(val.shape, dtype=self.dtype)
                tmp[...] = val[...]
                val = tmp
        else:
            val = numpy.asarray(val, order='C', dtype=self.dtype)

        self.log.debug("rows shape: {}".format(val.shape))
        self.log.debug("data dtype: {}".format(val.dtype))

        if len(val.shape) != 1:
            raise ValueError("rows must be one-dimensional")

        numrows = val.shape[0]

        req = "/datasets/" + self.id.uuid + "/value"

        params = {}
        body = {}


        format = "json"

        if use_base64:

            # server is HSDS, use binary data, use param values for selection
            format = "binary"
            body = val.tobytes()
            self.log.debug("writing binary data, {} bytes".format(len(body)))
            params["append"] = numrows
        else:
            if type(val) is not list:
                val = val.tolist()
            val = _decode(val)
            self.log.debug("writing json data, {} elements".format(len(val)))
            self.log.debug("data: {}".format(val))
            body['value'] = val
            body['append'] = numrows

        self.PUT(req, body=body, format=format, params=params)

        # if we get here, the request was successful, adjust the shape
        total_rows = self._shape[0] + numrows
        self._shape = (total_rows,)
