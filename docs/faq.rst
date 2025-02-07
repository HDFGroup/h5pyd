.. _faq:

FAQ
===


What datatypes are supported?
-----------------------------

Below is a complete list of types for which h5pdy supports reading, writing and
creating datasets. Each type is mapped to a native NumPy type.

Fully supported types:

=========================           ============================================    ================================
Type                                Precisions                                      Notes
=========================           ============================================    ================================
Bitfield                            1, 2, 4 or 8 byte, BE/LE                        Read as unsigned integers
Integer                             1, 2, 4 or 8 byte, BE/LE, signed/unsigned
Float                               2, 4, 8, 12, 16 byte, BE/LE
Compound                            Arbitrary names and offsets
Strings (fixed-length)              Any length
Strings (variable-length)           Any length, ASCII or Unicode
Boolean                             NumPy 1-byte bool                               Stored as HDF5 enum
Array                               Any supported type
Enumeration                         Any NumPy integer type                          Read/write as integers
References                          object
Variable length array               Any supported type                              See :ref:`Special Types <vlen>`
=========================           ============================================    ================================

Other numpy dtypes, such as datetime64 and timedelta64, can optionally be
stored in HDF5 opaque data using :func:`opaque_dtype`.
h5pyd will read this data back with the same dtype, but other software probably
will not understand it.

Unsupported types:

=========================           ============================================
Type                                Status
=========================           ============================================
HDF5 "time" type                    Not planned
Complex                             Coming soon!
Opaque                              Coming soon!
Region References                   Coming soon!
Bitfield                            Not planned
NumPy "U" strings                   Not planned
NumPy generic "O"                   Not feasible for client/server architectures
=========================           ============================================


What compression/processing filters are supported?
--------------------------------------------------

=================================== =========================================== ============================
Filter                              Function                                    Availability
=================================== =========================================== ============================
DEFLATE/GZIP                        Standard HDF5 compression                   All platforms
SHUFFLE                             Increase compression ratio                  All platforms
FLETCHER32                          Error detection                             All platforms
Scale-offset                        Integer/float scaling and truncation        All platforms
SZIP                                Fast, patented compression for int/float    * UNIX: if supplied with HDF5.
                                                                                * Windows: read-only
`LZF <http://h5py.org/lzf>`_        Very fast compression, all types            Ships with h5py, C source
                                                                                available
=================================== =========================================== ============================


What file drivers are available?
--------------------------------

None -- file drivers are used by the HDF5 library to provide different modes to the 
filesystem, but this does not apply to HSDS.

HSDS does support different storage platforms: POSIX, AWS S3, Azure Blob Storage. 
Adding support for a new storage system can be done on the server side and shouldn't
require any h5pyd changes.
 
.. _h5py_h5pyd_cmp:

What's the difference between h5py and h5pyd?
---------------------------------------------

The h5py package was designed as a Pythonic interface to the HDF5 library, while
h5pyd was designed to be a client library for the HDF REST interface.  However one
of the goals of h5pyd to be as compatible as possible with h5py.  For applications
using the h5py high-level interface it's often possible to just change the h5py
import to: ``import h5pyd as h5py`` and have everything just work except now 
your code is sending and receiving requests to a server, rather than making calls 
to the HDF5 library.

With respect to the h5py low-level interface, there's no attempt to provide
compatibility in h5pyd.  The low-level methods in h5py are closely connected with
the HDF5 library interface and don't have a natural mapping the HDF REST API methods.

With respect to the the h5py high-level interface, one feature that h5pyd doesn't 
yet support is VDS (Virtual Datasets).  However, if you are using HDF5 file linking
in HSDS (see tbd), it's possible to achieve a similar effect by using the `H5D_CHUNK_REF_INDIRECT`
layout.  See: http://github.com/HDFGroup/hsds/docs/design/SingleObject.md. 

There are some operational differences between h5py and h5pyd to be aware of.

1. Since http requests are out of process and may involve access across a WAN, the time a 
given function takes can be much longer using h5pyd compared with h5py (where everything is
in process and typically accessing the local disk).  To mitigate this, h5pyd does some 
tricks with caching and deferred updates to minimize the impact of the increased latency.
To speed up dataset reading and writing, the MultiManager class (see tbd) might be helpful.
If the performance with h5pyd is not up to what you need, please contact the forum.  We may have
suggestions that will help.

2. In h5py, an object is deleted when the last link to it is removed.  This is not the case with
h5pyd, links and objects are managed separately.  To delete an object, use this syntax:
``del f[myobj.id]``.  Note if any links still point to the object, that will be "dangling".

3. In a related manner, in h5py anonymous objects (objects that have no link pointing to them),
will get removed when the file is closed.  By contrast, in h5pyd anonymous objects will only
get deleted when the are explicitly removed.

4. In h5py there's no way to absolutely determine if two object references are actually the
same object (i.e. obj1.id may be different than obj2.id even though obj1 and obj2 point to the
same object).  In h5pyd, an object's id uniquely identifies an object so an equality comparison
of two object ids will tell you if there are the same or not.

5. In h5py there's support for SWMR (Single Writer, Multiple Reader).  In h5pyd, the SWMR methods
are supported for compatibility, but in general there are no restrictions on multiple threads
are processes accessing the same domain (for reading or writing).  See: "Multi-prodessing and Multi-threading"
for more information (tbd).



.. _h5py_pytable_cmp:

What's the difference between h5py(d) and PyTables?
---------------------------------------------------

The two projects have different design goals. PyTables presents a database-like
approach to data storage, providing features like indexing and fast "in-kernel"
queries on dataset contents. It also has a custom system to represent data types.

In contrast, h5pyd (and h5py) is an attempt to map the HDF5 feature set to NumPy as closely
as possible. For example, the high-level type system uses NumPy dtype objects
exclusively, and method and attribute naming follows Python and NumPy
conventions for dictionary and array access (i.e. ".dtype" and ".shape"
attributes for datasets, ``group[name]`` indexing syntax for groups, etc).

In h5pyd (but not h5py), there is a ``Table`` class that provides some
PyTables like features (e.g. ability to use sql-like queries).  See: tbd
for more information.

There's also a PyTables perspective on this question at the
`PyTables FAQ <http://www.pytables.org/FAQ.html#how-does-pytables-compare-with-the-h5py-project>`_.


Does h5pyd support Parallel HDF5?
---------------------------------

There's no support in h5pyd for MPIO-enabled parallelism.  
Multiple processes can read and write to the same domain 
(see tbd), but if these processes need need to be closely 
synchronized, they'll need to implement their own IPC mechanism.

Variable-length (VLEN) data
---------------------------

All supported types can be stored in variable-length arrays.
See :ref:`Special Types <vlen>` for use details.  Unlike with 
h5py and the HDF5 library, datasets using variable-length types are
free to use compression.

Enumerated types
----------------
HDF5 enumerated types are supported. As NumPy has no native enum type, they
are treated on the Python side as integers with a small amount of metadata
attached to the dtype.

NumPy object types
------------------
Storage of generic objects (NumPy dtype "O") is not implemented and not
planned to be implemented, as in general these are client specific.  
However, objects picked to the "plain-text" protocol
(protocol 0) can be stored in HDF5 as strings.

Appending data to a dataset
---------------------------

For one-dimensional datasets, the ``Table`` class supports appending
operations much like with PyTables:

    >>> dt = np.dtype([('symbol', 'S4'), ('price', 'i4')])
    >>> table = mydomain.create_table("MyTable", dtype=dt)
    >>> table.append([('abc1', 68), ('xyz2', 98)])  # add two rows to the table

It's possible to have multiple processes appending to the same table and HSDS
will ensure no rows get over-written.

For mult-dimensional datasets, you can expand the shape of the dataset to fit your needs. For
example, if I have a series of time traces 1024 points long, I can create an
extendable dataset to store them:

    >>> dset = myfile.create_dataset("MyDataset", (10, 1024), maxshape=(None, 1024))
    >>> dset.shape
    (10,1024)

The keyword argument "maxshape" tells that the first dimension of the
dataset can be expanded to any size, while the second dimension is limited to a
maximum size of 1024. We create the dataset with room for an initial ensemble
of 10 time traces. If we later want to store 10 more time traces, the dataset
can be expanded along the first axis:

    >>> dset.resize(20, axis=0)   # or dset.resize((20,1024))
    >>> dset.shape
    (20, 1024)

Each axis can be resized up to the maximum values in "maxshape". Things to note:

* Unlike NumPy arrays, when you resize a dataset the indices of existing data
  do not change; each axis grows or shrinks independently
* The dataset rank (number of dimensions) is fixed when it is created
* If mulitple-processes will be changing the shape of a dataset, they
will need to coordinate to make sure they don't overrwite each other's data.

Unicode
-------
All domain names, link names, and attribute names are defined as unicode strings.
The h5pyd interface supports some operations that take byte strings, but these
will be decoded to unicode before being sent to the server.

However, HDF5 has no predefined datatype to represent fixed-width UTF-16 or
UTF-32 (NumPy format) strings. Therefore, the NumPy 'U' datatype is not supported.

Exceptions
----------

h5py tries to map the error codes from hdf5 to the corresponding
``Exception`` class on the Python side.  When an http request 
to HSDS fails, an IOError exception will be raised where the error
number is the http status code (e.g. `404 - Not Found`).

Development
-----------

Building from Git
~~~~~~~~~~~~~~~~~

Project code is on GitHub  (http://github.com/HDFGroup/h5pyd).

We use the following conventions for branches and tags:

* master: integration branch for the next minor (or major) version
* tags 2.0.0, 2.0.1, etc: Released bugfix versions

To build from a Git checkout:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone the project::

    $ git clone https://github.com/HDFGroup/h5pyd.git
    $ cd h5pyd

Build the project. If given, /path/to/hdf5 should point to a directory
containing a compiled, shared-library build of HDF5 (containing things like "include" and "lib")::

    $ python build -m 
    $ pip install -v .

If you will be using your own HSDS instance, setup the server and verify it's running:
``$ curl http://hsds_endpoint/about``

Set the following environment variables based on the server endpoint, which folder you will be using
to creat test data, and which credentials to use:

export HSDS_ENDPOINT=http://hsds.hdf.test:5101
export BUCKET_NAME=hsdstest
export ADMIN_USERNAME=admin
export ADMIN_PASSWORD=admin
export HS_USERNAME=test_user1
export HS_PASSWORD=test
export USER2_NAME=test_user2
export USER2_PASSWORD=test
export H5PYD_TEST_FOLDER=/home/test_user1/h5pyd_test/

Run the tests::

    $ python testall.py

Report any failing tests to the forum, or by filing a bug report at GitHub.
