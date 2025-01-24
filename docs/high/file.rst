.. currentmodule:: h5pyd
.. _file:


File Objects
============

File objects serve as your entry point into the world of HDF5.  While an h5py file object
corresponds to a POSIX file, in h5pyd a file object represents a HSDS ``domain`` .
Like HDF5 files, an HSDS domain is a hierarchical collection of groups and datasets.  
Unlike an HDF5 file though, the storage for a domain is managed by HSDS and clients may
not have direct access to the storage medium (e.g. an S3 bucket in which the user does not
have authorization to access directly).

For the most part you work with file objects in h5pyd in the same manner as you would with h5py.
The primary difference being that while you provide a file path in the h5py file constructor, you
use a domain path in h5pyd (see "Opening & Creating domains" below).

In addition
to the File-specific capabilities listed here, every File instance is
also an :ref:`HDF5 group <group>` representing the `root group` of the domain.


.. _file_open:

Opening & creating domains
--------------------------

File objects in h5pyd work generally like standard Python file objects.  They support
standard modes like r/w/a, and should be closed when they are no longer in
use.  However, there is obviously no concept of "text" vs "binary" mode.

Domains are identified by a sequence of folder names and finally the domain name 
all delimitated by ``/`` characters.
While the result looks like a standard POSIX path,
the path is only relevant to the particular HSDS instance you are connecting to.
You may prefer to use the  optional ``hdf5://`` prefix as a reminder that the path
is not actually referencing a POSIX file.

    >>> f = h5pyd.File('/home/test_user1/mydomain.h5', 'r')
    >>> f = h5pyd.File('hdf5:://home/test_user1/mydomain.h5', 'r')  # equivalent

The first argument is the path to the domain.  The path must be a string (i.e. Python 3 unicode string) and
must be an absolute path (starting with '/' or 'hdf5://').  If you are unsure about what domains are present,
you can use the ``hsls`` utility to list the contents of a folder.  E.g. ``$ hsls /home/test_user1/``.

.. note::

    Python "File-like" objects are not supported as the domain path.

Domains live in buckets, and if a bucket name is not provided, the default bucket that has been 
configured in the HSDS instance will be used.  To explicitly give a bucket name, use the bucket parameter:

   >>> f = h5py.File('/home/test_user1/mydomain.h5','r', bucket='mybucket')

The second argument is the domain access mode.
Valid modes are:

    ========  ================================================
     r        Readonly, domain must exist (default)
     r+       Read/write, domain must exist
     w        Create domain, delete existing domain if found
     w- or x  Create domain, fail if exists
     a        Read/write if exists, create otherwise
    ========  ================================================

Domains are opened read-only by default. So the file mode parameter is 
only required for one of the writable modes.

.. note::

    Unlike with h5py and the HDF5 library, there's no concept of file locking.  The
    same domain can be opened multiple times in the same or different thread, or even on a
    different machine.  Multiple clients can access the same domain for modification, but this won't
    result in the domain becoming corrupted (though nothing in HSDS guards against clients over-writing
    each others updates).

Whatever the mode used, if the domain is not configured to authorize the desired action, a 
``403 - Forbidden`` error will be raised.  See the next sections on authentication and authorization.

In addition to instantiating a file object with a domain path, you can pass the low level id of an 
existing file object.  The two file objects will share the root group, but methods (e.g. flush)
can be invoked independently.  

    >>> f = h5pyd.File('/home/test_user1/mydomain.h5', 'w')  # create a new domain
    >>> g = h5pyd.File(f.id, "r")  # this handle can only be used for reading
    >>> f.close()  # close f, g is still open
    >>> g.filename  # returns '/home/test_user1/mydomain.h5'

.. _file_authentication:

Authentication
--------------

In most cases HSDS will reject requests that don't provide some form of authentication.  
The HSDS username and password can be supplied by using the ``username`` and ``password``
arguments.  In addition the desired HSDS endpoint can be specified using ``endpoint``.
For example:

.. code-block::

    path = "hdf5://home/test_user1/mydomain.h5"
    username = "test_user1"
    password = "12345"
    endpoint = "http://hsds.hdf.test"
    f = h5pyd.File(path, 'r', username=username, password=password, endpoint=endpoint)

..

The username, password, and endpoint provided will be stored with the File object and used to
authenticate any requests sent to HSDS for operations on this domain.  If the username
and password given are invalid, a ``401 - Unauthorized`` error will be raised. 

Of course it's not best practice to hardcode usernames and passwords, so alternatively the environment variables
``HS_USERNAME``, ``HS_PASSWORD``, and ``HS_ENDPOINT`` can be used to store the user credentials and endpoint.  If 
username, password, and endpoint arguments are not provided, the respective environment variables will be used
if set.

If neither named parameters or environment variables are supplied, this information will be read from
the file ``.hscfg`` in the users home directory.  The ``.hscfg`` can be created using the ``hsconfigure`` 
utility (see: tbd).

Finally, if no credentials are found using any of these methods, anonymous requests (http requests that don't include 
an authentication header) will be used.
Depending on the permission settings of the domain and whether the HSDS instance has been configured to allow
anonymous requests, this will allow read-only actions on the domain.

.. _file_authorization:

Authorization
-------------

HSDS uses the concept of ``Access Control Lists (ACLs)`` to determine what actions a given user can perform on a domain.
A domain can have one or more ACLs associated with it.   Each ACL consist of the following fields:

* user (string) - user or group name (a group is a set of users)
* create (T/F) - permission to create new objects (domains, groups, datasets, etc.)
* read (T/F) - permission to read data and metadata (e.g. list links in a group)
* update (T/F) - permission to modify metadata or dataset data
* delete (T/F) - permission to delete objects (including the domain itself)
* readACL (T/F) - permission to view permissions (i.e. read the list of ACLs for a domain)
* updateACL (T/F) - permission to add, delete, or modify ACLs

When HSDS receives a request, it will determine what type of action is requiring (read, update, delete, etc.), and
then review the ACLs for the domain to determine if the action is authorized.  If there is an ACL for the particular 
user making the request, then the relevant flag for that ACL will be used.  Otherwise, if there is a group ACL which
authorizes the request and the user is a member of that group, the request will be authorized.  There is a special
group name: ``default`` that includes all users.  In any case, if no authorizing ACL is found, 
a `403 - Forbidden`` error will be raised.

When a new domain is created (e.g. by using h5pyd.File with the `w` access mode), an ACL that gives
the owner of the domain (the authenticated user making the request unless the 'owner' argument is given) full control.  
Other users would not have permissions to even read the domain.  
These permissions can be adjusted, or new ACLs added programmatically (using tbd),
or using the ``hsacl`` tool (see: tbd).

Folders (every domain lives in specific folder) also have ACLs.  To create a new domain, the authenticating user
needs to have create permissions for the domain's folder.

Finally, there are special users that can be configured in HSDS known as ``admin`` users.  Admin users can perform any
action regardless of the ACLs.  With great power comes great responsibility, so it's best practice to only use 
admin credentials when there's no alternative (e.g. you accidentally removed permissions for a domain you own).


.. _file_cache:

Caching
-------

When a domain is open for reading, h5pyd will by default, cache certain metadata from  the domain 
(e.g. links in a group), so that it doesn't 
have to repeatedly request information from the HSDS instance associated with the domain.   This is good for performance
(requests to HSDS generally have higher latency than reading from a file), but in cases where the domain is being actively modified, 
it may not be what you want.  For example, suppose a sensor of some sort was setup so that readings from the previous time 
period was appended to a dataset every second.  By default, h5pyd won't know to check that the dataset shape has
been modified, so a program written to plot real-time readings wouldn't see any updates.
To avoid this, setting ``use_swmr`` to True will instruct h5pyd to not cache any data, so 
any operation will fetch the current data from HSDS.  See: (tbd) for more details.  

.. _file_flush:

Flushing
--------

For performance reasons, HSDS will not immediately write updates to a domain while processing 
the request that made the update.
Rather, the modifications will live in a server-side memory cache of "dirty" objects.  
These objects will get written to storage periodically (every one second by default).  
This is very similar in concept to how writes to a POSIX file don't immediately
get written to disk, but will be managed by the file controller.  
With h5pyd, if HSDS unfortunately crashed just after processing a series of 
PUT or POST requests, these changes would not get published to the storage device and as a result be lost.

If you need to make absolutely certain that recent updates have been persisted, use the flush method.  This call
won't return until HSDS has verified that all pending updates have been written to permanent storage.


.. _file_closing:

Closing domains
---------------

Objects in HSDS are stateless - i.e. at the level of the REST interface, the server doesn't
utilize any session information in responding to requests.  So an "open" vs. "closed"
domain is a concept that only applies at the client level.  The h5pyd file object
does use the close method to do some internal housekeeping however.  For example, closing
the http connection with the HSDS.  So invoking close on h5pyd file object is good best practice,
but not a critical as with h5py.

The close method will be invoked automatically when you leave the ``with h5py.File(...)`` block.

The close method does have an optional parameter not found in h5yd: ``flush``.
See See :ref:`file_flush` .


.. _file_delete:


Deleting Domains
----------------

With h5py and the HDF5 library you would normally delete HDF5 files using your systems file browser, or the "rm"
command.  Programmatically you could delete a HDF5 file using the standard Python Path.unlink method.
None of these options are possible with HSDS domains, but the ``hsrm`` (see: tbd) command is included with
h5pyd and works like the standard ``rm`` command with domain paths used instead of file paths.

Programmatically, you can delete domains using the del method of the folder object (see: tbd).

.. _file_summary:

Summary data
------------

Due to the way in which domains are stored, certain information about the domain would be unfeasible to 
determine on demand.  For example to compute the total amount of storage used would require summing the size
of each piece of object metadata and each dataset chunk, which for large domains could require fetching
attributes for millions of objects.  So for these properties, the server periodically runs asynchronous tasks 
to compile summary information about the domain.  

The impact of this is that some properties of the file object will only reflect the
domain state as of the last time HSDS ran this asynchronous task (typically a few seconds to a minute
after the last update to the domain).

Properties for which this applies are:

* num_objects
* num_datatypes
* num_groups
* num_datasets
* num_linked_chunks
* total_size
* metadata_bytes
* linked_bytes
* allocated_bytes
* md5_sum

The last_scan property returns the timestamp at which the scan was run.  You can use this property to determine when
HSDS has updated the summary data for a domain.  The following illustrates how to get summary data 
for a recent update:

.. code-block::

    time_stamp = f.last_scan  # get the last scan time
    f.create_group("g1")  # create a new group
    while f.last_scan == time_stamp:
       time.sleep(0.1)  # wait for summary data to be updated
    # print affected summary properties
    print("num_groups:", f.num_groups)
    print("num_objects:", f.num_objects)
    print("metadata_bytes:", f.metadata_bytes)
    print("total_size:", f.total_size)

..


.. _file_unsupported:

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
                    :class:`Group.id` to bind to an existing
                    domain identifier.
    :param mode:    Mode in which to open the domain; one of
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
    :param swmr:    If ``True`` open the domain in single-writer-multiple-reader.  Has the same 
                    effect as setting use_cache to False.
                    mode. Only used when mode="r".
    :param libver:  For compatibility with h5py - library version bounds.  Has no effect other
                    than returning given value as a property.
    :param owner:  For new domains, the owner username to be used for the domain.  Can only be
                   set if username is an HSDS admin user.  If owner is None, username will be 
                   assigned as owner.
    :param linked_domain: For new domain, use the root object of the linked_domain.
    :param logger:  Logger object to be used for logging.
    :param track_order:  Track dataset/group/attribute creation order under
                    root group if ``True``.  Default is
                    ``h5.get_config().track_order``.
    :param retries: Number of retries to use if an http request fails
                    (e.g. on a 503 Service Unavailable response).
    :param timeout: Number of seconds to wait on a http response before failing.

    

    .. method:: __bool__()

        Check that the file descriptor is valid and the domain is open:

            >>> f = h5pyd.File(domainpath)
            >>> f.close()
            >>> if f:
            ...     print("domain is open")
            ... else:
            ...     print("domain is closed")
            domain is closed

    .. method:: close(flush=False)

        Close this domain.  All open objects will become invalid.  If flush is True, will 
        invoke a flush operation before closing the domain.

    .. method:: flush()

        Request that HSDS persist any recent updates to permanent storage

    .. method:: getACLs()

        Return a list of ACLs associated with the domain.  See: tbd

    .. method:: getACL(username)

        Returns the ACL for the given user or group name.  Raises a ``401 - Not Found`` error
        if no ACL with that name exists

    .. method:: run_scan()

        Force a re-compilation of summary data (see tbd).  Requires write intent on the domain

    .. attribute:: id

        Low-level identifier (an instance of :class:`GroupID`).

    .. attribute:: filename

        Path to the domain, as a Unicode string.

    .. attribute:: mode

        String indicating if the domain is open readonly ("r") or read-write
        ("r+").  Will always be one of these two values, regardless of the
        mode used to open the domain.

    .. attribute:: swmr_mode

       True if the domain access is using :doc:`/swmr`. Use :attr:`mode` to
       distinguish SWMR read from write.

    .. attribute:: libver

        Compatibility place holder for HDF5 library version. 

    .. attribute:: driver

        Compatibility place holder for HDF5 file driver.  Returns: ``rest_driver``. 
    
    .. attribute:: serverver

        HSDS version string

    .. attribute:: userblock_size

        Compatibility place holder.  Always returns 0.

    .. attribute:: created

        Time (in seconds since epoch) that the domain was created.

    .. attribute:: modified

        Time (in seconds since epoch) the the domain was last modified

    .. attribute:: owner

        Name of user who created the domain

    .. attribute:: num_objects

        Number of objects (groups, datases, named datatypes) that are in the domain

    .. attribute:: num_datatypes

        Number of named datatypes in the domain

    .. attribute:: num_datasets

        Number of datasets in the domain
    
    .. attribute:: num_groups

        Number of groups in the domain

    .. attribute:: num_chunks

        Number of chunks (sum of number of chunks for each dataset) in the domain

    .. attribute:: num_linked_chunks

        Number of linked chunks (chunks that reference HDF5 file chunks) in the domain

    .. attribute:: allocated_bytes

        Number of bytes that have been allocated (i.e. the sum of the size of each chunk that has
        been created) for the domain

    .. attribute:: metadata_bytes

        Number of bytes that been used for metadata (object properties, links, attributes, etc.) in
        the domain

    .. attribute:: linked_bytes

        Number of bytes contained in chunks that links to HDF5 file chunks

    .. attribute:: total_size

        Total amount of storage used for metadata, chunk data, and linked chunks in the domain

    .. attribute:: md5_sum

        MD5 checksum for domain - a 32 character hexadecimal string.  Will change whenever any metadata
        or dataset data is modified

    .. attribute:: last_scan

        Time (in seconds since epoch) that the last domain scan was performed

    .. attribute:: limits

        Server defined limits.  Currently returns a dictionary with the keys
        ``min_chunk_size``, ``max_chunk_size``, and ``max_request_size``.

    .. attribute:: compressors

        Compression filters supported by HSDS.  See: tbd
    

