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

.. class:: File(name, mode='r',  swmr=False,  track_order=None)

    Open or create a new HSDS domain.

    Note that in addition to the :class:`File`-specific methods and properties
    listed below, :class:`File` objects inherit the full interface of
    :class:`Group`.

    :param name:    Name of domain (`str`), or an instance of
                    :class:`h5f.FileID` to bind to an existing
                    file identifier.
    :param mode:    Mode in which to open file; one of
                    ("w", "r", "r+", "a", "w-").  See :ref:`file_open`.
    :param endpoint: HSDS http endpoint.  If None, the endpoint given by HS_ENDPOINT environment
                    variable will be used if set.  Otherwise, the endpoint given in the 
                    .hscfg file will be used
    :param username: HSDS username.  If None, the username given by the HS_USERNAME environment
                     variable will be used if set.  Otherwise, the username given in the
                     .hscfg file will be used
    :param password: HSDS password.  If None, the password given by the HS_PASSWORD environment
                    variable will be used if set.  Otherwise, the password given in the
                    .hscfg file will be used
    :param bucket: Name of bucket the domain is expected to be found in.  If None, the 
                   default HSDS bucket name will be used
    :param api_key: API key (e.g. a JSON Web Token) to use for authentication.  If provided,
                    username and password parameters will be ignored
    :param session: Keep http connection alive between requests (more efficient than 
                    re-creating the connection on each request)
    :param use_cache: Save domain state locally rather than fetching needed state from HSDS 
                    as needed.  Set use_cache to False when opening a domain if you expect
                    other clients to be modifying domain metadata (e.g. adding links or attributes).
    :param swmr:    If ``True`` open the file in single-writer-multiple-reader.  Has the same 
                    effect as setting use_cache to False.
                    mode. Only used when mode="r".
    :param libver:  For compatibility with h5py - library version bounds.  Has no effect other
                    than returning given value as a property.
    :param owner:  For new domains, the owner username to be used for the domain.  Can only be
                   set if username is an HSDS admin user.  If owner is None, username will be 
                   assigned as owner.
    :param linked_domain: For new domain, use the root object of the linked_domain.
    :param logger:  Logger object ot be used for logging.
    :param track_order:  Track dataset/group/attribute creation order under
                    root group if ``True``.  Default is
                    ``h5.get_config().track_order``.
    :param retries: Number of retries to use if an http request fails
                    (e.g. on a 503 Service Unavailable response).
    :param timeout: Number of seconds to wait on a http response before failing.

    

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


    .. attribute:: version

        HSDS version string

