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

# We use __getitem__ side effects, which pylint doesn't like.
# pylint: disable=pointless-statement

"""
    High-level access to HDF5 dataspace selections
"""

from __future__ import absolute_import

import numpy as np

H5S_SEL_POINTS = 0
H5S_SELECT_SET = 1
H5S_SELECT_APPEND = 2
H5S_SELECT_PREPEND = 3
H5S_SELECT_OR = 4
H5S_SELECT_NONE = 5
H5S_SELECT_ALL = 6
H5S_SELECT_HYPERSLABS = 7
H5S_SELECT_NOTB = 8


def select(obj, args):
    """ High-level routine to generate a selection from arbitrary arguments
    to __getitem__.  The arguments should be the following:

    obj
        Datatset object

    args
        Either a single argument or a tuple of arguments.  See below for
        supported classes of argument.

    Argument classes:

    Single Selection instance
        Returns the argument.

    numpy.ndarray
        Must be a boolean mask.  Returns a PointSelection instance.

    RegionReference
        Returns a Selection instance.

    Indices, slices, ellipses only
        Returns a SimpleSelection instance

    Indices, slices, ellipses, lists or boolean index arrays
        Returns a FancySelection instance.
    """

    if not isinstance(args, tuple):
        args = (args,)

    # TBD - handle NULL Space object

    if obj.shape == ():
        # scalar object
        sel = ScalarSelection(obj.shape, args)
        return sel


    #print("select, len(args):", len(args))
    # "Special" indexing objects
    if len(args) == 1:

        arg = args[0]

        if isinstance(arg, Selection):
            if arg.shape != obj.shape:
                raise TypeError("Mismatched selection shape")
            return arg

        elif isinstance(arg, np.ndarray) or isinstance(arg, list):
            sel = PointSelection(obj.shape)
            sel[arg]
            return sel
        """
        #todo - RegionReference
        elif isinstance(arg, h5r.RegionReference):
            sid = h5r.get_region(arg, dsid)
            if shape != sid.shape:
                raise TypeError("Reference shape does not match dataset shape")

            return Selection(shape, spaceid=sid)
        """

    for a in args:
        if not isinstance(a, slice) and a is not Ellipsis:
            try:
                int(a)
            except Exception:
                sel = FancySelection(obj.shape)
                sel[args]
                return sel

    sel = SimpleSelection(obj.shape)
    sel[args]
    return sel




class Selection(object):

    """
        Base class for HDF5 dataspace selections.  Subclasses support the
        "selection protocol", which means they have at least the following
        members:

        __init__(shape)   => Create a new selection on "shape"-tuple
        __getitem__(args) => Perform a selection with the range specified.
                             What args are allowed depends on the
                             particular subclass in use.

        id (read-only) =>      h5py.h5s.SpaceID instance
        shape (read-only) =>   The shape of the dataspace.
        mshape  (read-only) => The shape of the selection region.
                               Not guaranteed to fit within "shape", although
                               the total number of points is less than
                               product(shape).
        nselect (read-only) => Number of selected points.  Always equal to
                               product(mshape).

        broadcast(target_shape) => Return an iterable which yields dataspaces
                                   for read, based on target_shape.

        The base class represents "unshaped" selections (1-D).
    """

    def __init__(self, shape, *args, **kwds):
        """ Create a selection.  Shape may be None if spaceid is given. """

        shape = tuple(shape)
        self._shape = shape

        self._select_type = H5S_SELECT_ALL

    @property
    def select_type(self):
        """ SpaceID instance """
        return self._select_type

    @property
    def shape(self):
        """ Shape of whole dataspace """
        return self._shape

    @property
    def nselect(self):
        """ Number of elements currently selected """

        return self.getSelectNpoints()

    @property
    def mshape(self):
        """ Shape of selection (always 1-D for this class) """
        return (self.nselect,)

    def getSelectNpoints(self):
        npoints = None
        if self._select_type == H5S_SELECT_NONE:
            npoints = 0
        elif self._select_type == H5S_SELECT_ALL:
            dims = self._shape
            npoints = 1
            for nextent in dims:
                npoints *= nextent
        else:
            raise IOError("Unsupported select type")
        return npoints


    def broadcast(self, target_shape):
        """ Get an iterable for broadcasting """
        if np.product(target_shape) != self.nselect:
            raise TypeError("Broadcasting is not supported for point-wise selections")
        yield self._id

    def __getitem__(self, args):
        raise NotImplementedError("This class does not support indexing")

class PointSelection(Selection):

    """
        Represents a point-wise selection.  You can supply sequences of
        points to the three methods append(), prepend() and set(), or a
        single boolean array to __getitem__.
    """
    def __init__(self, shape,  *args, **kwds):
        """ Create a Point selection.   """
        Selection.__init__(self, shape, *args, **kwds)
        self._points = []

    @property
    def points(self):
        """ selection points """
        return self._points


    def getSelectNpoints(self):
        npoints = None
        if self._select_type == H5S_SELECT_NONE:
            npoints = 0
        elif self._select_type == H5S_SELECT_ALL:
            dims = self._shape
            npoints = 1
            for nextent in dims:
                npoints *= nextent
        elif self._select_type == H5S_SEL_POINTS:
            dims = self._shape
            rank = len(dims)
            if len(self._points) == rank and not type(self._points[0]) in (list, tuple, np.ndarray):
                npoints = 1
            else:
                npoints = len(self._points)
        else:
            raise IOError("Unsupported select type")
        return npoints


    def _perform_selection(self, points, op):
        """ Internal method which actually performs the selection """
        if isinstance(points, np.ndarray) or True:
            points = np.asarray(points, order='C', dtype='u8')
            if len(points.shape) == 1:
                #points.shape = (1,points.shape[0])
                pass


        if self._select_type != H5S_SEL_POINTS:
            op = H5S_SELECT_SET
        self._select_type = H5S_SEL_POINTS

        if op == H5S_SELECT_SET:
            self._points = points
        elif op == H5S_SELECT_APPEND:
            self._points.extent(points)
        elif op == H5S_SELECT_PREPEND:
            tmp = self._points
            self._points = points
            self._points.extend(tmp)
        else:
            raise ValueError("Unsupported operation")

    #def _perform_list_selection(points, H5S_SELECT_SET):


    def __getitem__(self, arg):
        """ Perform point-wise selection from a NumPy boolean array """
        if  isinstance(arg, list):
            points = arg
        else:
            if not (isinstance(arg, np.ndarray) and arg.dtype.kind == 'b'):
                raise TypeError("PointSelection __getitem__ only works with bool arrays")
            if not arg.shape == self._shape:
                raise TypeError("Boolean indexing array has incompatible shape")

            points = np.transpose(arg.nonzero())
        self.set(points)
        return self

    def append(self, points):
        """ Add the sequence of points to the end of the current selection """
        self._perform_selection(points, H5S_SELECT_APPEND)

    def prepend(self, points):
        """ Add the sequence of points to the beginning of the current selection """
        self._perform_selection(points, H5S_SELECT_PREPEND)

    def set(self, points):
        """ Replace the current selection with the given sequence of points"""
        """
        if isinstance(points, list):
            # selection with list of points
            self._perform_list_selection(points, H5S_SELECT_SET)

        else:
            # selection with boolean ndarray
        """
        self._perform_selection(points, H5S_SELECT_SET)


class SimpleSelection(Selection):

    """ A single "rectangular" (regular) selection composed of only slices
        and integer arguments.  Can participate in broadcasting.
    """

    @property
    def mshape(self):
        """ Shape of current selection """
        return self._mshape

    @property
    def start(self):
        return self._sel[0]

    @property
    def count(self):
        return self._sel[1]

    @property
    def step(self):
        return self._sel[2]

    def __init__(self, shape, *args, **kwds):
        Selection.__init__(self, shape, *args, **kwds)
        rank = len(self._shape)
        self._sel = ((0,)*rank, self._shape, (1,)*rank, (False,)*rank)
        self._mshape = self._shape
        self._select_type = H5S_SELECT_ALL

    def __getitem__(self, args):

        if not isinstance(args, tuple):
            args = (args,)

        #print "__getitem__", args

        if self._shape == ():
            if len(args) > 0 and args[0] not in (Ellipsis, ()):
                raise TypeError("Invalid index for scalar dataset (only ..., () allowed)")
            self._select_type = H5S_SELECT_ALL
            return self

        start, count, step, scalar = _handle_simple(self._shape,args)
        self._sel = (start, count, step, scalar)

        #self._id.select_hyperslab(start, count, step)
        self._select_type = H5S_SELECT_HYPERSLABS

        self._mshape = tuple(x for x, y in zip(count, scalar) if not y)

        return self

    def getSelectNpoints(self):
        """Return number of elements in current selection
        """
        #print("SimpleSelection.getSelectNPoints")
        npoints = None
        if self._select_type == H5S_SELECT_NONE:
            npoints = 0
        elif self._select_type == H5S_SELECT_ALL:
            dims = self._shape
            npoints = 1
            for nextent in dims:
                npoints *= nextent
        elif self._select_type == H5S_SELECT_HYPERSLABS:
            #print("sel hyperslabs, count:", self.count, "step:", self.step)
            dims = self._shape
            npoints = 1
            rank = len(dims)
            for i in range(rank):
                npoints *= self.count[i]
        else:
            raise IOError("Unsupported select type")
        return npoints

    def getQueryParam(self):
        param = ''
        rank = len(self._shape)
        if rank == 0:
            return None

        param += "["
        for i in range(rank):
            start = self.start[i]
            stop = start + (self.count[i] * self.step[i])
            if stop > self._shape[i]:
                stop = self._shape[i]
            dim_sel = str(start) + ':' + str(stop)
            if self.step[i] != 1:
                dim_sel += ':' + str(self.step[i])
            if i != rank-1:
                dim_sel += ','
            param += dim_sel
        param += ']'
        return param

    def broadcast(self, target_shape):
        """ Return an iterator over target dataspaces for broadcasting.

        Follows the standard NumPy broadcasting rules against the current
        selection shape (self._mshape).
        """
        if self._shape == ():
            if np.product(target_shape) != 1:
                raise TypeError("Can't broadcast %s to scalar" % target_shape)
            self._id.select_all()
            yield self._id
            return

        start, count, step, scalar = self._sel

        rank = len(count)
        target = list(target_shape)

        tshape = []
        for idx in range(1,rank+1):
            if len(target) == 0 or scalar[-idx]:     # Skip scalar axes
                tshape.append(1)
            else:
                t = target.pop()
                if t == 1 or count[-idx] == t:
                    tshape.append(t)
                else:
                    raise TypeError("Can't broadcast %s -> %s" % (target_shape, count))
        tshape.reverse()
        tshape = tuple(tshape)

        chunks = tuple(x//y for x, y in zip(count, tshape))
        nchunks = int(np.product(chunks))

        if nchunks == 1:
            yield self._id
        else:
            sid = self._id.copy()
            sid.select_hyperslab((0,)*rank, tshape, step)
            for idx in range(nchunks):
                offset = tuple(x*y*z + s for x, y, z, s in zip(np.unravel_index(idx, chunks), tshape, step, start))
                sid.offset_simple(offset)
                yield sid


class FancySelection(Selection):

    """
        Implements advanced NumPy-style selection operations in addition to
        the standard slice-and-int behavior.

        Indexing arguments may be ints, slices, lists of indicies, or
        per-axis (1D) boolean arrays.

        Broadcasting is not supported for these selections.
    """

    @property
    def mshape(self):
        return self._mshape

    @property
    def hyperslabs(self):
        return self._hyperslabs


    def __init__(self, shape, *args, **kwds):
        Selection.__init__(self, shape, *args, **kwds)
        self._mshape = self._shape
        self._hyperslabs = []

    def __getitem__(self, args):
        #print("args:", args)

        if not isinstance(args, tuple):
            args = (args,)

        args = _expand_ellipsis(args, len(self._shape))

        # First build up a dictionary of (position:sequence) pairs

        sequenceargs = {}
        for idx, arg in enumerate(args):
            if not isinstance(arg, slice):
                if hasattr(arg, 'dtype') and arg.dtype == np.dtype('bool'):
                    if len(arg.shape) != 1:
                        raise TypeError("Boolean indexing arrays must be 1-D")
                    arg = arg.nonzero()[0]
                try:
                    sequenceargs[idx] = list(arg)
                except TypeError:
                    pass
                else:
                    if sorted(arg) != list(arg):
                        raise TypeError("Indexing elements must be in increasing order")

        if len(sequenceargs) > 1:
            raise TypeError("Only one indexing vector or array is currently allowed for advanced selection")
        if len(sequenceargs) == 0:
            raise TypeError("Advanced selection inappropriate")

        vectorlength = len(list(sequenceargs.values())[0])
        if not all(len(x) == vectorlength for x in sequenceargs.values()):
            raise TypeError("All sequence arguments must have the same length %s" % sequenceargs)

        # Now generate a vector of selection lists,
        # consisting only of slices and ints

        argvector = []
        for idx in range(vectorlength):
            entry = list(args)
            for position, seq in sequenceargs.items():
                entry[position] = seq[idx]
            argvector.append(entry)

        # "OR" all these selection lists together to make the final selection

        #self._id.select_none()
        self._hyperslabs = []
        count = ()
        for idx, vector in enumerate(argvector):
            start, count, step, scalar = _handle_simple(self._shape, vector)
            #print("select_hyperslab:", start, count, step)
            #self._id.select_hyperslab(start, count, step, H5S_SELECT_OR)
            self._hyperslabs.append( {"start": start, "count": count, "step": step} )

        # Final shape excludes scalars, except where
        # they correspond to sequence entries

        mshape = list(count)
        for idx in range(len(mshape)):
            if idx in sequenceargs:
                mshape[idx] = len(sequenceargs[idx])
            elif scalar[idx]:
                mshape[idx] = 0

        self._mshape = tuple(x for x in mshape if x != 0)

    def broadcast(self, target_shape):
        if not target_shape == self._mshape:
            raise TypeError("Broadcasting is not supported for complex selections")
        yield self._id

def _expand_ellipsis(args, rank):
    """ Expand ellipsis objects and fill in missing axes.
    """
    n_el = sum(1 for arg in args if arg is Ellipsis)
    if n_el > 1:
        raise ValueError("Only one ellipsis may be used.")
    elif n_el == 0 and len(args) != rank:
        args = args + (Ellipsis,)

    final_args = []
    n_args = len(args)
    for arg in args:

        if arg is Ellipsis:
            final_args.extend( (slice(None,None,None),)*(rank-n_args+1) )
        else:
            final_args.append(arg)

    if len(final_args) > rank:
        raise TypeError("Argument sequence too long")

    return final_args

def _handle_simple(shape, args):
    """ Process a "simple" selection tuple, containing only slices and
        integer objects.  Return is a 4-tuple with tuples for start,
        count, step, and a flag which tells if the axis is a "scalar"
        selection (indexed by an integer).

        If "args" is shorter than "shape", the remaining axes are fully
        selected.
    """
    args = _expand_ellipsis(args, len(shape))

    start = []
    count = []
    step  = []
    scalar = []

    for arg, length in zip(args, shape):
        if isinstance(arg, slice):
            #print "translate slice"
            x,y,z = _translate_slice(arg, length)
            s = False
        else:
            try:
                x,y,z = _translate_int(int(arg), length)
                s = True
            except TypeError:
                raise TypeError('Illegal index "%s" (must be a slice or number)' % arg)
        start.append(x)
        count.append(y)
        step.append(z)
        scalar.append(s)

    return tuple(start), tuple(count), tuple(step), tuple(scalar)

def _translate_int(exp, length):
    """ Given an integer index, return a 3-tuple
        (start, count, step)
        for hyperslab selection
    """
    if exp < 0:
        exp = length+exp

    if not 0<=exp<length:
        raise ValueError("Index (%s) out of range (0-%s)" % (exp, length-1))

    return exp, 1, 1

def _translate_slice(exp, length):
    """ Given a slice object, return a 3-tuple
        (start, count, step)
        for use with the hyperslab selection routines
    """
    start, stop, step = exp.indices(length)
        # Now if step > 0, then start and stop are in [0, length];
        # if step < 0, they are in [-1, length - 1] (Python 2.6b2 and later;
        # Python issue 3004).

    if step < 1:
        raise ValueError("Step must be >= 1 (got %d)" % step)
    if stop < start:
        raise ValueError("Reverse-order selections are not allowed")

    count = 1 + (stop - start - 1) // step

    return start, count, step

def guess_shape(sid):
    """ Given a dataspace, try to deduce the shape of the selection.

    Returns one of:
        * A tuple with the selection shape, same length as the dataspace
        * A 1D selection shape for point-based and multiple-hyperslab selections
        * None, for unselected scalars and for NULL dataspaces
    """

    sel_class = sid.get_simple_extent_type()    # Dataspace class
    sel_type = sid.get_select_type()            # Flavor of selection in use

    if sel_class == 'H5S_NULL':
        # NULL dataspaces don't support selections
        return None

    elif sel_class == 'H5S_SCALAR':
        # NumPy has no way of expressing empty 0-rank selections, so we use None
        if sel_type == H5S_SELECT_NONE: return None
        if sel_type == H5S_SELECT_ALL: return tuple()

    elif sel_class != 'H5S_SIMPLE':
        raise TypeError("Unrecognized dataspace class %s" % sel_class)

    # We have a "simple" (rank >= 1) dataspace

    N = sid.get_select_npoints()
    rank = len(sid.shape)

    if sel_type == H5S_SELECT_NONE:
        return (0,)*rank

    elif sel_type == H5S_SELECT_ALL:
        return sid.shape

    elif sel_type == H5S_SEL_POINTS:
        # Like NumPy, point-based selections yield 1D arrays regardless of
        # the dataspace rank
        return (N,)

    elif sel_type != H5S_SELECT_HYPERSLABS:
        raise TypeError("Unrecognized selection method %s" % sel_type)

    # We have a hyperslab-based selection

    if N == 0:
        return (0,)*rank

    bottomcorner, topcorner = (np.array(x) for x in sid.get_select_bounds())

    # Shape of full selection box
    boxshape = topcorner - bottomcorner + np.ones((rank,))

    def get_n_axis(sid, axis):
        """ Determine the number of elements selected along a particular axis.

        To do this, we "mask off" the axis by making a hyperslab selection
        which leaves only the first point along the axis.  For a 2D dataset
        with selection box shape (X, Y), for axis 1, this would leave a
        selection of shape (X, 1).  We count the number of points N_leftover
        remaining in the selection and compute the axis selection length by
        N_axis = N/N_leftover.
        """

        if(boxshape[axis]) == 1:
            return 1

        start = bottomcorner.copy()
        start[axis] += 1
        count = boxshape.copy()
        count[axis] -= 1

        # Throw away all points along this axis
        masked_sid = sid.copy()
        masked_sid.select_hyperslab(tuple(start), tuple(count), op=H5S_SELECT_NOTB)

        N_leftover = masked_sid.get_select_npoints()

        return N//N_leftover


    shape = tuple(get_n_axis(sid, x) for x in range(rank))

    if np.product(shape) != N:
        # This means multiple hyperslab selections are in effect,
        # so we fall back to a 1D shape
        return (N,)

    return shape




class ScalarSelection(Selection):

    """
        Implements slicing for scalar datasets.
    """

    @property
    def mshape(self):
        return self._mshape


    def __init__(self, shape,  *args, **kwds):
        Selection.__init__(self, shape, *args, **kwds)
        arg = None
        if len(args) > 0:
            arg = args[0]
        if arg == ():
            self._mshape = None
            self._select_type = H5S_SELECT_ALL
        elif arg == (Ellipsis,):
            self._mshape = ()
            self._select_type = H5S_SELECT_ALL
        else:
            raise ValueError("Illegal slicing argument for scalar dataspace")
