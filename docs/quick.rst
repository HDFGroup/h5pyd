.. _quick:

Quick Start Guide
=================

Github Codespaces
-----------------

The quickest way to get started with h5pyd and HSDS is to use Github Codespaces.  You can launch A
codespace the incudes h5pyd and HSDS by clicking here: <https://codespaces.new/hdfgroup/h5pyd>.
Try out some of the included examples and Python notebooks to get familiar with the features
offered by the package.

Read on to run h5pyd on your laptop or desktop system...

Install
-------

You can install ``h5pyd`` via pip::

  pip install h5py

If you will be running your own HSDS instance, install the ``hsds`` package as well::

    pip install hsds

HSDS Setup
----------

If you will be using an existing HSDS instance, your sysadmin will provide you 
with the http endpoint, username, and password used to access HSDS.  You can
skip the rest of this section and go to h5pyd configuration below.

HSDS can be installed on different platforms such as Kubernetes, Docker, or DC/OS. For
these options see the relevant install guide in <https://github.com/HDFGroup/hsds/tree/master/docs>.

For now though, we will just run HSDS locally.  Follow these steps:

* Make a directory that will be used for data storage.  For example,: ``mkdir ~/hsds_data``
* Start the HSDS service: ``hsds --root_dir ~/hsds_data``
* Once you see the output: ``READY! use endpoint: http://localhost:5101``, you can open the HSDS status page at: <http://localhost:5101/about>

When you are ready to shut down HSDS, just hit Ctlr-C in the terminal window where you started it.

h5pyd configuration
-------------------

Typically, http requests to HSDS need to authenticated (likely you don't want just anyone messing around with your HDF data!).
HSDS supports several authentication protocols, but the simplest (if not the most secure) to use when getting started is what is 
known as HTTP Basic Authentication.  In this protocol, your HSDS username and password is encoded in the HTTP header of 
each request.  On receiving the request, HSDS will decode the autentication header and then compare with a list of valid 
usernames/passwords stored in a text file.  The h5pyd package will do this automatically, using your credentials stored 
in the file ``~/.hscfg`` to add the authentication header to each HSDS request.  

You can edit hscfg file by hand, or use the hsconfigure tool included with h5pyd as follows:

* Start the app by running: ``hsconfigure`` in a terminal
* You'll be prompted for a server endpoint.  If running HSDS locally, enter: ``http://localhost:5101`` 
* Next you'll be asked for your username.  Enter your system username if running HSDS locally, or the name given by your sysadmin otherwise
* Next you'll be asked for your password.  Again use your system username, or the password provided by your sysadmin
* For API Key, just hit enter
* Access to HSDS and your credentials will be verified, and if ok, you will see ``connection ok``
* Type ``Y`` to save your information to the .hscfg file

At anytime, your can verify access to HSDS by running ``hsabout``.  This utility will used your saved credentials to fetch
status information from the server and display it.


Core concepts
-------------

While the HDF5 library works with files on a POSIX filesystem (typically a local disk or network mount), 
with h5pyd all access to data storage is mediated by HSDS.  For example, HSDS may be configured to use 
AWS storage that you don't have permissions to view directly. 

To make keeping track of everything  easier, HSDS manages storage using three levels of organization:

* Buckets are collections of Folders and Domains
* Folders live in buckets are work much like directories in a POSIX file system
* Domains are the equivalent to HDF5 files

Buckets are the top-level of storage used by HSDS and will correspond to AWS S3 Buckets, Azure Blob Containers, or POSIX directories.
Buckets can not be created using the h5pyd package (these need to be setup by the HSDS administrator), 
but the h5pyd File and Folder object have an optional bucket parameter to specify which
bucket to access.  Typically HSDS will be setup with a default bucket that will be used if no bucket name is given explicitly. 

Folders can be created using h5pyd (or the hstouch CLI tool).  To use hstouch to create a folder, run hstouch followed by
the path to the desired folder.  E.g. ``hstouch /home/$USER/myfolder/``.  In h5oyd, there no 'current folder' concept,
so the path must always start with ``/`` (or if desired, ``hdf5://`` to distinguish from a POSIX path).  Also, to create
folder, the path must end in a slash.  To view the contents of a folder, use the ``hsls`` tool.  E.g.:
``hsls /home/$USER/myfolder/``. 

Folder can contain sub-folders, but also domains (equivalent to an HDF5 file).  As with HDF5 files, 
HSDS domains are containers for two kinds of objects: `datasets`, which are
array-like collections of data, and `groups`, which are folder-like containers
that hold datasets and other groups. The most fundamental thing to remember
when using h5pyd (as with h5py) is:

    **Groups work like dictionaries, and datasets work like NumPy arrays**

Domains can be created programmatically, or using the CLI tools.  E.g. ``hstouch /home/$USER/myfolder/mytestfile.h5``.
To convert an HDF5 file to an HSDS domain, you can use the hscp command: ``hscp mytestfile.h5 /home/$USER/myfolder/``.

A quick note on domain permissions:  When you create a new domain, it will only be accessible using your 
credentials.  You can enable who else can access the domain using the hsacl tool.  For example, to enable 
other users to read a domain (but not modify it) use: ``hsacl /home/$USER/myfolder/mytest.h5 +r default``.  
For details of using hsacl, see: tbd.

To programmatically access a domain for reading for reading, use the h5pyd.File object::

    >>> import h5pyd as h5py
    >>> f = h5py.File('/home/test_user1/mytestfile.h5', 'r')  # replace test_user1 with your user name

The :ref:`File object <file>` is your starting point. If you are familiar with h5py, the rest of this section will be 
exactly the same as to what you'd expect opening an HDF5 file.  In you are not familiar with h5py, keep reading, but
keep in mind that this will apply to both h5py and h5pyd.
   

What is stored in the domain? Remember :py:class:`h5pyd.File` 
acts like a Python dictionary, thus we can check the keys,

    >>> list(f.keys())
    ['mydataset']

Based on our observation, there is one data set, :code:`mydataset` in the file.
Let us examine the data set as a :ref:`Dataset <dataset>` object

    >>> dset = f['mydataset']

The object we obtained isn't an array, but :ref:`an HDF5 dataset <dataset>`.
Like NumPy arrays, datasets have both a shape and a data type:

    >>> dset.shape
    (100,)
    >>> dset.dtype
    dtype('int32')

They also support array-style slicing.  This is how you read and write data
from a dataset in the file::

    >>> dset[...] = np.arange(100)
    >>> dset[0]
    0
    >>> dset[10]
    10
    >>> dset[0:100:10]
    array([ 0, 10, 20, 30, 40, 50, 60, 70, 80, 90])

For more, see :ref:`file` and :ref:`dataset`.

Creating a domain programmatically
++++++++++++++++++++++++++++++++++

You can create a domain by setting the :code:`mode` to :code:`w` when
the File object is initialized. Some other modes are :code:`a`
(for read/write/create access), and
:code:`r+` (for read/write access).
A full list of file access modes and their meanings is at :ref:`file`. ::

    >>> import h5py
    >>> import numpy as np
    >>> f = h5py.File("/home/test_user1/myfolder/mytestfile.hdf5", "w")

The :ref:`File object <file>` has a couple of methods which look interesting. One of them is ``create_dataset``, which
as the name suggests, creates a data set of given shape and dtype ::

    >>> dset = f.create_dataset("mydataset", (100,), dtype='i')

The File object is a context manager; so the following code works too ::

    >>> import h5py
    >>> import numpy as np
    >>> with h5py.File("mytestfile.hdf5", "w") as f:
    >>>     dset = f.create_dataset("mydataset", (100,), dtype='i')


Groups and hierarchical organization
------------------------------------

"HDF" stands for "Hierarchical Data Format".  Every object in an HDF5 file
has a name, and they're arranged in a POSIX-style hierarchy with
``/``-separators::

    >>> dset.name
    '/mydataset'

The "folders" in this system are called :ref:`groups <group>`.  The ``File`` object we
created is itself a group, in this case the `root group`, named ``/``:

    >>> f.name
    '/'

Creating a subgroup is accomplished via the aptly-named ``create_group``. But we need to open the file in the "append" mode first (Read/write if exists, create otherwise) ::

    >>> f = h5py.File('/home/test_user1/myfolder/mydataset.h5', 'a')
    >>> grp = f.create_group("subgroup")

All ``Group`` objects also have the ``create_*`` methods like File::

    >>> dset2 = grp.create_dataset("another_dataset", (50,), dtype='f')
    >>> dset2.name
    '/subgroup/another_dataset'

By the way, you don't have to create all the intermediate groups manually.
Specifying a full path works just fine::

    >>> dset3 = f.create_dataset('subgroup2/dataset_three', (10,), dtype='i')
    >>> dset3.name
    '/subgroup2/dataset_three'

Groups support most of the Python dictionary-style interface.
You retrieve objects in the file using the item-retrieval syntax::

    >>> dataset_three = f['subgroup2/dataset_three']

Iterating over a group provides the names of its members::

    >>> for name in f:
    ...     print(name)
    mydataset
    subgroup
    subgroup2

Membership testing also uses names::

    >>> "mydataset" in f
    True
    >>> "somethingelse" in f
    False

You can even use full path names::

    >>> "subgroup/another_dataset" in f
    True

There are also the familiar ``keys()``, ``values()``, ``items()`` and
``iter()`` methods, as well as ``get()``.

Since iterating over a group only yields its directly-attached members,
iterating over an entire file is accomplished with the ``Group`` methods
``visit()`` and ``visititems()``, which take a callable::

    >>> def printname(name):
    ...     print(name)
    >>> f.visit(printname)
    mydataset
    subgroup
    subgroup/another_dataset
    subgroup2
    subgroup2/dataset_three

For more, see :ref:`group`.

Attributes
----------

One of the best features of HDF5 is that you can store metadata right next
to the data it describes.  All groups and datasets support attached named
bits of data called `attributes`.

Attributes are accessed through the ``attrs`` proxy object, which again
implements the dictionary interface::

    >>> dset.attrs['temperature'] = 99.5
    >>> dset.attrs['temperature']
    99.5
    >>> 'temperature' in dset.attrs
    True

For more, see :ref:`attributes`.
