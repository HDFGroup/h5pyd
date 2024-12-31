.. currentmodule:: h5py
.. _file:


File Objects
============

File objects serve as your entry point into the world of HDF5.  In addition
to the File-specific capabilities listed here, every File instance is
also an :ref:`HDF5 group <group>` representing the `root group` of the file.

Note: Python "File-like" objects are not supported.

.. _file_open:

Opening & creating domains
--------------------------

HSDS domains work generally like standard Python file objects.  They support
standard modes like r/w/a, and should be closed when they are no longer in
use.  However, there is obviously no concept of "text" vs "binary" mode.

    >>> f = h5py.File('myfile.hdf5','r')

The file name may be a string (i.e. Python 3 unicode string). Valid modes are:

    ========  ================================================
     r        Readonly, file must exist (default)
     r+       Read/write, file must exist
     w        Create file, truncate if exists
     w- or x  Create file, fail if exists
     a        Read/write if exists, create otherwise
    ========  ================================================

   Files are opened read-only by default. So the file mode parameter is 
   only required for one of the writable modes.

.. _file_driver:

Unsupported options
-------------------

The following options are used with h5py.File, but are not supported with h5pyd:

* driver
* libver
* userblock_size
* rdcc_nbytes
* rdcc_w0
* rdcc_nslots
* fs_strategy
* fs_persist
* fs_page_size
* fs_threshold
* page_buf_size
* min_meta_keep
* min_raw_keep
* locking
* alignment_threshold
* alignment_interval
* meta_block_size

For the most part these relate to concepts that don't apply to HSDS, so are not included.

.. _file_closing:

Closing files
-------------

If you call :meth:`File.close`, or leave a ``with h5py.File(...)`` block,
the file will be closed and any objects (such as groups or datasets) you have
from that file will become unusable. This is equivalent to what HDF5 calls
'strong' closing.

If a file object goes out of scope in your Python code, the file will only
be closed when there are no remaining objects belonging to it. This is what
HDF5 calls 'weak' closing.

.. code-block::

    with h5py.File('/a_folder/f1.h5', 'r') as f1:
        ds = f1['dataset']

    # ERROR - can't access dataset, because f1 is closed:
    ds[0]

    def get_dataset():
        f2 = h5py.File('f2.h5', 'r')
        return f2['dataset']
    ds = get_dataset()

    # OK - f2 is out of scope, but the dataset reference keeps it open:
    ds[0]

    del ds  # Now f2.h5 will be closed

..


Reference
---------

.. note::

    Unlike Python file objects, the attribute :attr:`File.name` gives the
    HDF5 name of the root group, "``/``". To access the domain  name, use
    :attr:`File.filename`.

.. class:: File(name, mode='r', driver=None, libver=None, userblock_size=None, \
    swmr=False, rdcc_nslots=None, rdcc_nbytes=None, rdcc_w0=None, \
    track_order=None, fs_strategy=None, fs_persist=False, fs_threshold=1, \
    fs_page_size=None, page_buf_size=None, min_meta_keep=0, min_raw_keep=0, \
    locking=None, alignment_threshold=1, alignment_interval=1, **kwds)

    Open or create a new HSDS domain.

    Note that in addition to the :class:`File`-specific methods and properties
    listed below, :class:`File` objects inherit the full interface of
    :class:`Group`.

    :param name:    Name of domain (`str`), or an instance of
                    :class:`h5f.FileID` to bind to an existing
                    file identifier, or a file-like object
                    (see :ref:`file_fileobj`).
    :param mode:    Mode in which to open file; one of
                    ("w", "r", "r+", "a", "w-").  See :ref:`file_open`.
    :param driver:  File driver to use; see :ref:`file_driver`.
    :param libver:  Compatibility bounds; see :ref:`file_version`.
    :param userblock_size:  Size (in bytes) of the user block.  If nonzero,
                    must be a power of 2 and at least 512.  See
                    :ref:`file_userblock`.
    :param swmr:    If ``True`` open the file in single-writer-multiple-reader
                    mode. Only used when mode="r".
    :param rdcc_nbytes:  Total size of the raw data chunk cache in bytes. The
                    default size is :math:`1024^2` (1 MiB) per dataset.
    :param rdcc_w0: Chunk preemption policy for all datasets.  Default value is
                    0.75.
    :param rdcc_nslots:  Number of chunk slots in the raw data chunk cache for
                    this file.  Default value is 521.
    :param track_order:  Track dataset/group/attribute creation order under
                    root group if ``True``.  Default is
                    ``h5.get_config().track_order``.
    :param fs_strategy: The file space handling strategy to be used.
            Only allowed when creating a new file. One of "fsm", "page",
            "aggregate", "none", or ``None`` (to use the HDF5 default).
    :param fs_persist: A boolean to indicate whether free space should be
            persistent or not. Only allowed when creating a new file. The
            default is False.
    :param fs_page_size: File space page size in bytes. Only use when
            fs_strategy="page". If ``None`` use the HDF5 default (4096 bytes).
    :param fs_threshold: The smallest free-space section size that the free
            space manager will track. Only allowed when creating a new file.
            The default is 1.
    :param page_buf_size: Page buffer size in bytes. Only allowed for HDF5 files
            created with fs_strategy="page". Must be a power of two value and
            greater or equal than the file space page size when creating the
            file. It is not used by default.
    :param min_meta_keep: Minimum percentage of metadata to keep in the page
            buffer before allowing pages containing metadata to be evicted.
            Applicable only if ``page_buf_size`` is set. Default value is zero.
    :param min_raw_keep: Minimum percentage of raw data to keep in the page
            buffer before allowing pages containing raw data to be evicted.
            Applicable only if ``page_buf_size`` is set. Default value is zero.
    :param locking: The file locking behavior. One of:

            - False (or "false") --  Disable file locking
            - True (or "true")   --  Enable file locking
            - "best-effort"      --  Enable file locking but ignore some errors
            - None               --  Use HDF5 defaults

            .. warning::

                The HDF5_USE_FILE_LOCKING environment variable can override
                this parameter.

            Only available with HDF5 >= 1.12.1 or 1.10.x >= 1.10.7.
    :param alignment_threshold: Together with ``alignment_interval``, this
            property ensures that any file object greater than or equal
            in size to the alignment threshold (in bytes) will be
            aligned on an address which is a multiple of alignment interval.
    :param alignment_interval: This property should be used in conjunction with
            ``alignment_threshold``. See the description above. For more
            details, see :ref:`file_alignment`.
    :param meta_block_size: Determines the current minimum size, in bytes, of
            new metadata block allocations. See :ref:`file_meta_block_size`.
    :param kwds:    Driver-specific keywords; see :ref:`file_driver`.

    .. method:: __bool__()

        Check that the file descriptor is valid and the file open:

            >>> f = h5py.File(filename)
            >>> f.close()
            >>> if f:
            ...     print("file is open")
            ... else:
            ...     print("file is closed")
            file is closed

    .. method:: close()

        Close this file.  All open objects will become invalid.

    .. method:: flush()

        Request that the HDF5 library flush its buffers to disk.

    .. attribute:: id

        Low-level identifier (an instance of :class:`FileID <low:h5py.h5f.FileID>`).

    .. attribute:: filename

        Name of this file on disk, as a Unicode string.

    .. attribute:: mode

        String indicating if the file is open readonly ("r") or read-write
        ("r+").  Will always be one of these two values, regardless of the
        mode used to open the file.

    .. attribute:: swmr_mode

       True if the file access is using :doc:`/swmr`. Use :attr:`mode` to
       distinguish SWMR read from write.

    .. attribute:: driver

        String giving the driver used to open the file.  Refer to
        :ref:`file_driver` for a list of drivers.

    .. attribute:: libver

        2-tuple with library version settings.  See :ref:`file_version`.

    .. attribute:: userblock_size

        Size of user block (in bytes).  Generally 0.  See :ref:`file_userblock`.

    .. attribute:: meta_block_size

        Minimum size, in bytes, of metadata block allocations. Default: 2048.
        See :ref:`file_meta_block_size`.
