.. currentmodule:: h5pyd
.. _dataset:


Datasets
========

Datasets are very similar to NumPy arrays.  They are homogeneous collections of
data elements, with an immutable datatype and (hyper)rectangular shape.
Unlike NumPy arrays, they support a variety of transparent storage features
such as compression, error-detection, and chunked I/O.

They are represented in h5pyd by a thin proxy class which supports familiar
NumPy operations like slicing, along with a variety of descriptive attributes:

  - **shape** attribute
  - **size** attribute
  - **ndim** attribute
  - **dtype** attribute
  - **nbytes** attribute

h5pyd supports most NumPy dtypes, and uses the same character codes (e.g.
``'f'``, ``'i8'``) and dtype machinery as
`Numpy <https://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html>`_.
See :ref:`faq` for the list of dtypes h5pyd supports.


.. _dataset_create:

Creating datasets
-----------------

New datasets are created using either :meth:`Group.create_dataset` or
:meth:`Group.require_dataset`.  Existing datasets should be retrieved using
the group indexing syntax (``dset = group["name"]``).

To initialise a dataset, all you have to do is specify a name, shape, and
optionally the data type (defaults to ``'f'``)::

    >>> dset = f.create_dataset("default", (100,))
    >>> dset = f.create_dataset("ints", (100,), dtype='i8')

.. note:: This is not the same as creating an :ref:`Empty dataset <dataset_empty>`.

You may also initialize the dataset to an existing NumPy array by providing the `data` parameter::

    >>> arr = np.arange(100)
    >>> dset = f.create_dataset("init", data=arr)

Assigning an array into a group works like specifying ``data`` and no other
parameters::

    >>> f["init"] = arr

Keywords ``shape`` and ``dtype`` may be specified along with ``data``; if so,
they will override ``data.shape`` and ``data.dtype``.  It's required that
(1) the total number of points in ``shape`` match the total number of points
in ``data.shape``, and that (2) it's possible to cast ``data.dtype`` to
the requested ``dtype``.

.. _dataset_slicing:

Reading & writing data
----------------------

HDF5 datasets reuse the NumPy slicing syntax to read and write to the file.
Slice specifications are translated directly to HDF5 "hyperslab"
selections, and are a fast and efficient way to access data in the file. The
following slicing arguments are recognized:

    * Indices: anything that can be converted to a Python long
    * Slices (i.e. ``[:]`` or ``[0:10]``)
    * Field names, in the case of compound data
    * At most one ``Ellipsis`` (``...``) object
    * An empty tuple (``()``) to retrieve all data or `scalar` data

Here are a few examples (output omitted).

    >>> dset = f.create_dataset("MyDataset", (10,10,10), 'f')
    >>> dset[0,0,0]
    >>> dset[0,2:10,1:9:3]
    >>> dset[:,::2,5]
    >>> dset[0]
    >>> dset[1,5]
    >>> dset[0,...]
    >>> dset[...,6]
    >>> dset[()]

There's more documentation on what parts of numpy's :ref:`fancy indexing <dataset_fancy>` are available in h5pyd.

For compound data, it is advised to separate field names from the
numeric slices::

    >>> dset.fields("FieldA")[:10]   # Read a single field
    >>> dset[:10]["FieldA"]          # Read all fields, select in NumPy

To retrieve the contents of a `scalar` dataset, you can use the same
syntax as in NumPy:  ``result = dset[()]``.  In other words, index into
the dataset using an empty tuple.

For simple slicing, broadcasting is supported:

    >>> dset[0,:,:] = np.arange(10)  # Broadcasts to (10,10)

Broadcasting is implemented on HSDS and using it can reduce the 
amount of data that needs to be transferred to the server.
It is supported for the above "simple" (integer, slice and ellipsis) slicing only.

.. warning::
   Currently h5pyd does not support nested compound types 

Multiple indexing
~~~~~~~~~~~~~~~~~

Indexing a dataset once loads a numpy array into memory.
If you try to index it twice to write data, you may be surprised that nothing
seems to have happened:

   >>> f = h5pyd.File('/my_hdf5_file.h5', 'w')
   >>> dset = f.create_dataset("test", (2, 2))
   >>> dset[0][1] = 3.0  # No effect!
   >>> print(dset[0][1])
   0.0

The assignment above only modifies the loaded array. It's equivalent to this:

   >>> new_array = dset[0]
   >>> new_array[1] = 3.0
   >>> print(new_array[1])
   3.0
   >>> print(dset[0][1])
   0.0

To write to the dataset, combine the indexes in a single step:

   >>> dset[0, 1] = 3.0
   >>> print(dset[0, 1])
   3.0

.. _dataset_iter:

Length and iteration
~~~~~~~~~~~~~~~~~~~~

As with NumPy arrays, the ``len()`` of a dataset is the length of the first
axis, and iterating over a dataset iterates over the first axis.  However,
modifications to the yielded data are not recorded in the file.  Resizing a
dataset while iterating has undefined results.

.. _dataset_chunks:

Chunked storage
---------------

In HSDS the default storage layout for HDF5 datasets is `chunked`; in
other words, the dataset is divided up into regularly-sized pieces which are 
each stored separately in the storage system (i.e. a file when POSIX storage is used
or as a object for AWS S3 or Azure Blob Storage).  

Chunked storage makes it possible to resize datasets, and because the data
is stored in fixed-size chunks, to use compression filters.  Also in HSDS,
having a dataset divided into smaller chunks enables parallelization for dataset
read and write operations.

To set the chunked storage layout, set the keyword ``chunks`` to a tuple indicating
the chunk shape::

    >>> dset = f.create_dataset("chunked", (1000, 1000), chunks=(100, 100))

Data will be read and written in blocks with shape (100,100); for example,
the data in ``dset[0:100,0:100]`` will be stored together in the file, as will
the data points in range ``dset[400:500, 100:200]``.

Chunking has performance implications.  It's recommended to keep the total
size of your chunks between 1 MiB and 8 MiB.

Since picking a chunk shape can be confusing, you can have h5pyd guess a chunk
shape for you::

    >>> dset = f.create_dataset("autochunk", (1000, 1000), chunks=True)
    >>> dset.chunks
    (313, 32)

The iter_chunks method returns an iterator that can be used to perform chunk by chunk
reads or writes::

    >>> for s in dset.iter_chunks():
    >>>     arr = dset[s]  # get numpy array for chunk


.. _dataset_resize:

Resizable datasets
------------------

In HDF5, datasets can be resized once created up to a maximum size,
by calling :meth:`Dataset.resize`.  You specify this maximum size when creating
the dataset, via the keyword ``maxshape``::

    >>> dset = f.create_dataset("resizable", (10,10), maxshape=(500, 20))

Any (or all) axes may also be marked as "unlimited", in which case they may
be increased up to the HDF5 per-axis limit of 2**64 elements.  Indicate these
axes using ``None``::

    >>> dset = f.create_dataset("unlimited", (10, 10), maxshape=(None, 10))

For a 1D dataset, ``maxshape`` can be an integer instead of a tuple. But to make an
unlimited 1D dataset, ``maxshape`` must be a tuple ``(None,)``. Passing ``None`` gives
the default behaviour, where the initial size is also the maximum.

.. note:: Resizing an array with existing data works differently than in NumPy; if
    any axis shrinks, the data in the missing region is discarded.  Data does
    not "rearrange" itself as it does when resizing a NumPy array.


.. _dataset_compression:

Filter pipeline
---------------

Chunked data may be transformed by the HDF5 `filter pipeline`.  The most
common use is applying transparent compression.  Data is compressed by HSDS 
before writing to storage, and automatically decompressed when read.  Once the dataset
is created with a particular compression filter applied, data may be read
and written as normal with no special steps required.

Enable compression with the ``compression`` keyword to
:meth:`Group.create_dataset`::

    >>> dset = f.create_dataset("zipped", (100, 100), compression="gzip")

Options for each filter may be specified with ``compression_opts``::

    >>> dset = f.create_dataset("zipped_max", (100, 100), compression="gzip", compression_opts=9)

Lossless compression filters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GZIP filter (``"gzip"``)
    Available with HSDS and every installation of HDF5, so it's best where portability is
    required.  Good compression, moderate speed.  ``compression_opts`` sets the
    compression level and may be an integer from 0 to 9, default is 4.

'blosclz', 'lz4', 'lz4hc', 'gzip', 'zstd',

BLOSCLZ filter (``"blosclz"``)
    TBD

LZ4 filter (``"lz4"``)
    TBD

LZ4HC filter (``"lz4hc"``)
    TBD

ZSTD filter (``"zste"``)
    TBD


.. note::

    Since compression is done server-side, custom compression filters as used
    with the HDF5 library are not supported. 
 
.. _dataset_scaleoffset:

Scale-Offset filter
~~~~~~~~~~~~~~~~~~~

Filters enabled with the ``compression`` keywords are *lossless*; what comes
out of the dataset is exactly what you put in.  HDF5 also includes a lossy
filter which trades precision for storage space.

Works with integer and floating-point data only.  Enable the scale-offset
filter by setting :meth:`Group.create_dataset` keyword ``scaleoffset`` to an
integer.

For integer data, this specifies the number of bits to retain.  Set to 0 to have
HDF5 automatically compute the number of bits required for lossless compression
of the chunk.  For floating-point data, indicates the number of digits after
the decimal point to retain.

.. note::

    In the current HSDS release, the Scale-offset filter is not supported.  It
    can be specified with dataset_create in h5pyd, but will have no effect on
    how data actually gets stored on the server.

 
.. _dataset_shuffle:

Shuffle filter
~~~~~~~~~~~~~~

Block-oriented compressors like GZIP or BLOSCZL work better when presented with
runs of similar values.  Enabling the shuffle filter rearranges the bytes in
the chunk and may improve compression ratio.   

Enable by setting :meth:`Group.create_dataset` keyword ``shuffle`` to True.

.. _dataset_fletcher32:

Fletcher32 filter
~~~~~~~~~~~~~~~~~

Adds a checksum to each chunk to detect data corruption.  Attempts to read
corrupted chunks will fail with an error.  No significant speed penalty.
Obviously shouldn't be used with lossy compression filters.

Enable by setting :meth:`Group.create_dataset` keyword ``fletcher32`` to True.

.. note::

    In the current HSDS release, the Fletche32 filter is not supported.  It
    can be specified with dataset_create in h5pyd, but will have no effect on
    how data actually gets accessed on the server.

.. _dataset_multi_block:

Multi-Block Selection
---------------------

The full H5Sselect_hyperslab API is exposed via the MultiBlockSlice object.
This takes four elements to define the selection (start, count, stride and
block) in contrast to the built-in slice object, which takes three elements.
A MultiBlockSlice can be used in place of a slice to select a number of (count)
blocks of multiple elements separated by a stride, rather than a set of single
elements separated by a step.

For an explanation of how this slicing works, see the `HDF5 documentation <https://support.hdfgroup.org/documentation/hdf5/latest/_l_b_dset_sub_r_w.html>`_.

For example::

    >>> dset[...]
    array([ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10])
    >>> dset[MultiBlockSlice(start=1, count=3, stride=4, block=2)]
    array([ 1,  2,  5,  6,  9, 10])

They can be used in multi-dimensional slices alongside any slicing object,
including other MultiBlockSlices. For a more complete example of this,
see the multiblockslice_interleave.py example script.

.. note::

    Multi-Block selection is not currently supported.

.. _dataset_fancy:

Fancy indexing
--------------

A subset of the NumPy fancy-indexing syntax is supported.  Use this with
caution, as the underlying HDF5 mechanisms may have different performance
than you expect.

For any axis, you can provide an explicit list of points you want; for a
dataset with shape (10, 10)::

    >>> dset.shape
    (10, 10)
    >>> result = dset[0, [1,3,8]]
    >>> result.shape
    (3,)
    >>> result = dset[1:6, [5,8,9]]
    >>> result.shape
    (5, 3)

.. note::

    With h5py and the HDF5 library, performance using Fancy indexing can
    be sub-optimal for selections using a large number of coordinates.  This is generally not
    an issue with h5pyd and HSDS.   Typically you should see much better performance
    using Fancy Indexing compared with iteration through a list of coordinates.

You can also use fancy-indexing on multiple axes:

    >>> dse.shape
    (10, 10, 10)
    >>> result = dset[[2,6,8], :, [3,5,9]]
    >>> result.shape
    (3,10)

.. note::

    Fancy-indexing on multiple axes is not supported with h5py and the HDF5 library.

NumPy boolean "mask" arrays can also be used to specify a selection.  The
result of this operation is a 1-D array with elements arranged in the
standard NumPy (C-style) order.  Behind the scenes, this generates a laundry
list of points to select, so be careful when using it with large masks::

    >>> arr = numpy.arange(100).reshape((10,10))
    >>> dset = f.create_dataset("MyDataset", data=arr)
    >>> result = dset[arr > 50]
    >>> result.shape
    (49,)

.. _dataset_empty:

Creating and Reading Empty (or Null) datasets and attributes
------------------------------------------------------------

HDF5 has the concept of Empty or Null datasets and attributes. These are not
the same as an array with a shape of (), or a scalar dataspace in HDF5 terms.
Instead, it is a dataset with an associated type, no data, and no shape. In
h5pyd, we represent this as either a dataset with shape ``None``, or an
instance of ``h5pyd.Empty``. Empty datasets and attributes cannot be sliced.

To create an empty attribute, use ``h5pyd.Empty`` as per :ref:`attributes`::

    >>> obj.attrs["EmptyAttr"] = h5pyd.Empty("f")

Similarly, reading an empty attribute returns ``h5pyd.Empty``::

    >>> obj.attrs["EmptyAttr"]
    h5pyd.Empty(dtype="f")

Empty datasets can be created either by defining a ``dtype`` but no
``shape`` in ``create_dataset``::

    >>> grp.create_dataset("EmptyDataset", dtype="f")

or by ``data`` to an instance of ``h5pyd.Empty``::

    >>> grp.create_dataset("EmptyDataset", data=h5pyd.Empty("f"))

An empty dataset has shape defined as ``None``, which is the best way of
determining whether a dataset is empty or not. An empty dataset can be "read" in
a similar way to scalar datasets, i.e. if ``empty_dataset`` is an empty
dataset::

    >>> empty_dataset[()]
    h5pyd.Empty(dtype="f")

The dtype of the dataset can be accessed via ``<dset>.dtype`` as per normal.
As empty datasets cannot be sliced, some methods of datasets such as
``read_direct`` will raise a ``TypeError`` exception if used on a empty dataset.

Reference
---------

.. class:: Dataset(identifier)

    Dataset objects are typically created via :meth:`Group.create_dataset`,
    or by retrieving existing datasets from a file.  Call this constructor to
    create a new Dataset bound to an existing
    :class:`DatasetID <low:h5pyd.DatasetID>` identifier.

    .. method:: __getitem__(args)

        NumPy-style slicing to retrieve data.  See :ref:`dataset_slicing`.

    .. method:: __setitem__(args)

        NumPy-style slicing to write data.  See :ref:`dataset_slicing`.

    .. method:: __bool__()

        Check that the dataset is accessible.
        A dataset could be inaccessible for several reasons. For instance, the
        dataset, or the file it belongs to, may have been closed elsewhere.

        >>> f = h5pyd.open(domain_path)
        >>> dset = f["MyDS"]
        >>> f.close()
        >>> if dset:
        ...     print("dataset accessible")
        ... else:
        ...     print("dataset inaccessible")
        dataset inaccessible

    
    .. method:: flush()

        Force any pending persist updates to to be sent to the server.

    .. method:: read_direct(array, source_sel=None, dest_sel=None)

        Read from an HDF5 dataset directly into a NumPy array, which can
        avoid making an intermediate copy as happens with slicing. The
        destination array must be C-contiguous and writable, and must have
        a datatype to which the source data may be cast.  Data type conversion
        will be carried out on the fly.

        `source_sel` and `dest_sel` indicate the range of points in the
        dataset and destination array respectively.  Use the output of
        ``numpy.s_[args]``::

            >>> dset = f.create_dataset("dset", (100,), dtype='int64')
            >>> arr = np.zeros((100,), dtype='int32')
            >>> dset.read_direct(arr, np.s_[0:10], np.s_[50:60])

    .. method:: write_direct(source, source_sel=None, dest_sel=None)

        Write data directly to HDF5 from a NumPy array.
        The source array must be C-contiguous.  Selections must be
        the output of numpy.s_[<args>].
        Broadcasting is supported for simple indexing.


    .. method:: astype(dtype)

        Return a wrapper allowing you to read data as a particular
        type::

            >>> dset = f.create_dataset("bigint", (1000,), dtype='int64')
            >>> out = dset.astype('int16')[:]
            >>> out.dtype
            dtype('int16')

        TBD: this example is not working

    .. method:: asstr(encoding=None, errors='strict')

       Only for string datasets. Returns a wrapper to read data as Python
       string objects::

           >>> s = dataset.asstr()[0]

       encoding and errors work like ``bytes.decode()``, but the default
       encoding is defined by the datatype - ASCII or UTF-8.
       This is not guaranteed to be correct.

    .. method:: fields(names)

        Get a wrapper to read a subset of fields from a compound data type::

            >>> 2d_coords = dataset.fields(['x', 'y'])[:]

        If names is a string, a single field is extracted, and the resulting
        arrays will have that dtype. Otherwise, it should be an iterable,
        and the read data will have a compound dtype.

        .. versionadded:: 3.0

    .. method:: iter_chunks

       Iterate over chunks in a chunked dataset. The optional ``sel`` argument
       is a slice or tuple of slices that defines the region to be used.
       If not set, the entire dataspace will be used for the iterator.

       For each chunk within the given region, the iterator yields a tuple of
       slices that gives the intersection of the given chunk with the
       selection area. This can be used to :ref:`read or write data in that
       chunk <dataset_slicing>`.

       A TypeError will be raised if the dataset is not chunked.

       A ValueError will be raised if the selection region is invalid.

    .. method:: resize(size, axis=None)

        Change the shape of a dataset.  `size` may be a tuple giving the new
        dataset shape, or an integer giving the new length of the specified
        `axis`.

        Datasets may be resized only up to :attr:`Dataset.maxshape`.

    .. method:: len()

        Return the size of the first axis.

    .. method:: make_scale(name='')

       Make this dataset an HDF5 :ref:`dimension scale <dimension_scales>`.

       You can then attach it to dimensions of other datasets like this::

           other_ds.dims[0].attach_scale(ds)

       You can optionally pass a name to associate with this scale.

    .. attribute:: shape

        NumPy-style shape tuple giving dataset dimensions.

    .. attribute:: dtype

        NumPy dtype object giving the dataset's type.

    .. attribute:: size

        Integer giving the total number of elements in the dataset.

    .. attribute:: nbytes

        Integer giving the total number of bytes required to load the full dataset into RAM (i.e. `dset[()]`).
        This may not be the amount of disk space occupied by the dataset,
        as datasets may be compressed when written or only partly filled with data.
        This value also does not include the array overhead, as it only describes the size of the data itself.
        Thus the real amount of RAM occupied by this dataset may be slightly greater.

    .. attribute:: ndim

        Integer giving the total number of dimensions in the dataset.

    .. attribute:: maxshape

        NumPy-style shape tuple indicating the maximum dimensions up to which
        the dataset may be resized.  Axes with ``None`` are unlimited.

    .. attribute:: chunks

        Tuple giving the chunk shape, or None if chunked storage is not used.
        See :ref:`dataset_chunks`.

    .. attribute:: compression

        String with the currently applied compression filter, or None if
        compression is not enabled for this dataset.  See :ref:`dataset_compression`.

    .. attribute:: compression_opts

        Options for the compression filter.  See :ref:`dataset_compression`.

    .. attribute:: scaleoffset

        Setting for the HDF5 scale-offset filter (integer), or None if
        scale-offset compression is not used for this dataset.
        See :ref:`dataset_scaleoffset`.

    .. attribute:: shuffle

        Whether the shuffle filter is applied (T/F).  See :ref:`dataset_shuffle`.

    .. attribute:: fletcher32

        Whether Fletcher32 checksumming is enabled (T/F).  See :ref:`dataset_fletcher32`.

    .. attribute:: fillvalue

        Value used when reading uninitialized portions of the dataset, or None
        if no fill value has been defined, in which case HDF5 will use a
        type-appropriate default value.  Can't be changed after the dataset is
        created.

    .. attribute:: external

       This is used by h5py to indicate if the dataset is stored in one or more external files.
       With h5pyd this always returns ``None``.  

    .. attribute:: is_virtual

       True if this dataset is a :doc:`virtual dataset </vds>`, otherwise False.
       Always returns False in h5pyd.

    .. attribute:: dims

        Access to :ref:`dimension_scales`.

    .. attribute:: is_scale

        Return ``True`` if the dataset is also a :ref:`dimension scale <dimension_scales>`,
        ``False`` otherwise.

    .. attribute:: attrs

        :ref:`attributes` for this dataset.

    .. attribute:: id

        The dataset's low-level identifier; an instance of
        :class:`DatasetID <low:h5py.h5d.DatasetID>`.

    .. attribute:: ref

        An HDF5 object reference pointing to this dataset.  See
        :ref:`refs_object`.

    .. attribute:: regionref

        Proxy object for creating HDF5 region references.  Not
        yet supported in h5pyd.  
        See :ref:`refs_region`.

    .. attribute:: name

        String giving the full path to this dataset.

    .. attribute:: file

        :class:`File` instance in which this dataset resides

    .. attribute:: parent

        :class:`Group` instance containing this dataset.

    .. attribute:: num_chunks

        Returns the number of allocated chunks used by this dataset.
        Non-synchronous, value may not reflect the current state of
        the dataset.

Multi-Manager
-------------

Accessing a large number of selections in a dataset, or selections from a 
large number of datasets can be slow to access selection by selection.
For better performance in this case, you can use the Multi-Manager class.
This class enables multiple selections to be read or written in parallel for
better performance.

TBD
        
