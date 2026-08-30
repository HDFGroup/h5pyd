h5pyd
=====

Python client library for HSDS


Introduction
------------
This repository contains library, test, and examples of h5pyd - a Python package for HSDS 
(Highly Scalable Data Service), or other HDF REST compatible web services.

The h5pyd library provides a high-level interface to the REST specification that is generally
easier to use than invoking http calls directly.

This package is based on the popular h5py package and aims to be source compatible with
the h5py high level interface.

Differences between h5py and h5pyd are described here:  [`docs/h5py_vs_h5pyd.md`](docs/h5py_vs_h5pyd.md),


What's New in v1.0.0
---------------------

This release is a major architectural update: h5pyd's high-level API now shares its
object-model, dtype-translation, and selection-handling code with
[h5json](https://github.com/HDFGroup/hdf5-json) - the same library used server-side by
HSDS - via a new pluggable storage backend (`HsdsPlugin`, in `h5pyd/hsds_plugin.py`).
This keeps client and server logic in sync and removes a lot of code that had
previously drifted apart between the two projects.

### New Features

- **Shared h5json object model**: `File`, `Group`, `Attribute`, and `Dataset` internals
  now delegate to h5json's `Hdf5db` for dtype translation, selections, and array
  (de)serialization, the same architecture HSDS itself uses server-side.
- **`track_order` support**: `create_group`/`create_dataset`/attribute creation, and
  `File`, now support creation-order (vs. alphanumeric) link/attribute iteration,
  matching h5py.
- **Consolidated domain metadata**: reading a domain can now pull a consolidated
  summary of all its objects in one request rather than one request per object.
- **`Dataset.query()` gains `update_value=`**: server-side conditional query can now
  also update matching elements in place, without pulling the whole dataset locally.
- **Region reference `.query()`**: `dset.regionref.query(...)` builds a
  point-selection region reference directly from a query expression.
- **`Table`/`Cursor` improvements**: `read_where()`/`update_where()`/
  `get_where_list()` now flush pending local state before/after querying so results
  stay consistent with the server.
- **Boolean mask selection**: `dset[bool_array]` is now supported directly.
- **Point-selection fixes**: `dset.points[...]` now correctly handles repeated and
  out-of-order coordinates.
- **Array (subarray) dtype support**: bare top-level `H5T_ARRAY` datasets (not just
  array-typed fields nested in a compound type) now read/write/resize correctly.
- **Compound field-selective read/write isolation**: writing `dset["a", "c"] = ...`
  no longer disturbs unselected fields, and field order in multi-field writes is now
  derived from the actual serialized byte order.
- **`blosclz`/`lz4hc` compression** support, alongside the previously-added `lz4`
  (h5py doesn't support any of these).
- **`getFilters()`/`File.compressors`**: introspect which compression filters the
  connected server supports.
- New `getACL`/`getACLs`/`putACL` methods on `File` and `Group`, matching the
  ACL support `Folder` already had, backed by a shared `ACLManager` helper.


### Notable Bug Fixes

- Fixed a bug where opening a nonexistent domain could raise a bare, message-less
  `FileNotFoundError` instead of one carrying the actual HTTP status/reason - this
  showed up as a blank "Unexpected error:" message in `hstouch`, `hsacl`, and other
  command line apps.
- `Folder`'s "not found" handling is now consistent with `File`'s (both raise
  `FileNotFoundError` for a missing domain/folder).
- Fixed `hsacl` crashing with `AttributeError` on file domains (ACL support
  previously only worked on folders).
- Fixed `hsls` crashing with an unhandled error when listing a nonexistent
  top-level domain.
- Fixed `hsinfo` failing when invoked from `testall.py`, due to a relative import
  that only worked when run as part of a package.
- Fixed several complex-number dtype issues in `create_dataset()`.
- Fixed scalar-dataset region reference handling.
- Fixed fixed-length UTF-8 string charset round-tripping.
- Fixed mixed integer/list advanced-indexing corruption after a server round-trip.
- Fixed dtype-mismatch issues in `MultiManager`/multi-dataset field-selection reads
  and writes.
- Numerous smaller fixes across `hsload`, `hsstat`, `hsdiff`, and dimension-scale
  compatibility with h5py.

### Other Changes

- Minimum supported Python version is now 3.11 (up from 3.10); depends on the new
  `h5json` package.
- Extensive new test coverage: compound-with-array-field types, field-selection
  write isolation, array-dtype edge cases, `blosclz` compression, `hstouch`/`hsacl`
  CLI regression tests, and dual-mode (h5py/h5pyd) test-suite fixes so `testall.py`
  can be run against real h5py for comparison.
 
### Known Issues

- `Dataset.astype(dtype)` doesn't apply the dtype conversion on read.
- `Dataset.__eq__`/`__ne__` are not symmetric against a bare numpy scalar/array
  (mirrors an old, since-fixed h5py issue).
- Field-restricted `query()` (combining a query string with a `fields=` selection)
  isn't supported.
- `zstd` is listed in `File.compressors`, but creating a dataset with it currently
  fails server-side.


Websites
--------

* Main website: http://www.hdfgroup.org
* Source code: https://github.com/HDFGroup/h5pyd
* Forum: https://forum.hdfgroup.org/c/hsds
* Documentation: TBD (but http://docs.h5py.org/en/latest/ should be helpful)

Related Projects
----------------

* HSDS: https://github.com/HDFGroup/hsds
* HDF5-JSON: https://github.com/HDFGroup/hdf5-json
* h5py: https://github.com/h5py/h5py
* REST API Documentation: https://github.com/HDFGroup/hdf-rest-api

Installing
-----------

Via pip::

   pip install h5pyd

From a release tarball or Git checkout::

   pip install .

Run `hsconfigure` to setup the connection info (endpoint, username, and password) to HSDS.  
If you don't have access to an HSDS instance, you can easily setup your own HSDS instance.
See  https://github.com/HDFGroup/hsds for instructions on installing and running HSDS
on locally or in the cloud.

Direct Mode
-----------
The h5pyd package can be used without an explicit HSDS connection.  Rather, the storage system will
be accessed directly.
  
To use in direct mode, set the HS_ENDPOINT to "local" (or "local[n]" where n is the number of desired
sub-processes).  

For direct mode, some additional environment
variables are needed to be defined:

* ``BUCKET_NAME`` - name of the S3 Bucket, Azure Container, or Posix top level folder

To use "local" mode with S3, define these variables:

* ``AWS_S3_GATEWAY`` - AWS S3 endpoint, e.g.: ``https://s3.us-west-2.amazonaws.com``
* ``AWS_REGION`` - Region where the Lambda function is installed, e.g.: ``us-west-2``
* ``AWS_SECRET_ACCESS_KEY`` - Your AWS secret access AWS_SECRET_ACCESS_KEY
* ``AWS_ACCESS_KEY_ID`` - Your AWS access key ID

To use "local" mode with Azure, defined these variables:

* ``AZURE_CONNECTION_STRING`` - The connection string for your Azure storage account

To use "local" with Posix storage, define these variables:

* ``ROOT_DIR`` - The top level directory used for storage (i.e. the parent directory of "buckets")


H5PYD Command Line Apps
-----------------------

Several utility applications are included with this package:

* ``hsconfigure`` - save endpoint, username, and password in config files
* ``hsacl`` - read/update ACL (access control list) for a given folder or domain
* ``hscopy`` - copy a domain
* ``hsrm`` - delete a domain or folder
* ``hsdiff`` - compare HDF5 file with HSDS domain
* ``hsget`` - create an HDF5 file from HSDS domain
* ``hsinfo`` - get server status or domain details
* ``hsload`` - upload an HDF5 file to an HSDS domain
* ``hsls`` - list contents of domain or folder
* ``hsmv`` - change the naame of a domain
* ``hstouch`` - create new domain or folder

Use the ``--help`` option to get usage information for each command.

Testing
-------

By default the test suite will attempt to connect to a local instance of HSDS with the 
`http://localhost:5101` endpoint.

Use the following environment variables as needed to modify the default configuration
for the test suite:

* ``HS_ENDPOINT`` - "http://127.0.0.1:5000" for HSDS installed locally or appropriate remote endpoint
* ``HS_USERNAME`` - "test_user1" or your preferred username
* ``HS_PASSWORD`` - password for above username - "test" for test_user1 with local HSDS install
* ``TEST2_USERNAME`` - "test_user2" or your preffered username
* ``TEST2_PASSWORD`` - password for above username - "test" for test_user2 with local HSDS install
* ``H5PYD_TEST_FOLDER`` - server folder that will be used for generated files.  Example: "/home/test_user1/h5pyd_test/". Use hstouch command to create the folder before running the test, e.g.: ``$ hstouch /home/test_user1/h5pyd_test/``

Run: ``$python testall.py`` to execute the test suite.

Uninstalling
-------------

Just remove the install directory and all contents to uninstall.


Reporting bugs (and general feedback)
-------------------------------------

Create new issues at http://github.com/HDFGroup/h5pyd/issues for any problems you find.

For general questions/feedback, please use the HSDS forum (https://forum.hdfgroup.org/c/hsds).
