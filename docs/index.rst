HDF5 for the Cloud
==================

The h5pyd package is a Python interface for accessing the Highly Scalable Data Service (HSDS)
(https://www.hdfgroup.org/solutions/highly-scalable-data-service-hsds/).  Whereas the HDF5 library enables
complex scientific data to be stored in files, HSDS uses a cloud-native storage model that works well with 
object storage systems such as AWS S3 and Azure Blob Storage (regular POSIX stores are also supported).  As 
a service, HSDS can be running on the same system as the application using h5pyd, or it can be running on a server 
(e.g. running in an AWS data center co-located with S3 storage) while clients send requests over the network.

The native interface for HSDS is the HDF REST API (https://github.com/HDFGroup/hdf-rest-api), but the h5pyd package 
provides a convenient mode of access using the same API as the popular h5py package (https://docs.h5py.org/en/stable/index.html) 
that provides a Pythonic interface to the HDF5 library.  In fact, many applications that use h5py can be 
converted to using h5py, just by adding the statement ``import h5pyd as h5py``.

However, not every h5py feature is supported in h5pyd (at least not yet!).  For example Virtual Datasets are not supported.
For a complete list see the: :ref:`FAQ <faq>`.  In addition, there are some features that are supported, like multi-threading,
but work somewhat differently from the HDF5 library.  And finally, there are features of h5pyd, that don't have any 
correspondence to h5py, such as Folders.

The h5pyd package also includes a set of command line tools, for doing common tasks such as uploading HDF5 files to HSDS.
See: tbd for a description of the CLI tools. 

Where to start
--------------

* :ref:`Quick-start guide <quick>`


Other resources
---------------

* `Python and HDF5 O'Reilly book <https://shop.oreilly.com/product/0636920030249.do>`_
* `Ask questions on the HDF forum <https://forum.hdfgroup.org/c/hsds/6>`_
* `GitHub project for h5pyd <https://github.com/HDFGroup/h5pyd>`_
* `GitHub project for HSDS <https://github.com/HDFGroup/hsds>`_


Introductory info
-----------------

.. toctree::
    :maxdepth: 1

    quick


High-level API reference
------------------------

.. toctree::
    :maxdepth: 1

    high/file
    high/group
    high/dataset
    high/attr
    high/dims
    high/lowlevel


Advanced topics
---------------

.. toctree::
    :maxdepth: 1

    config
    special
    strings
    refs
    mpi
    swmr
    vds
    related_projects


Meta-info about the h5pyd project
---------------------------------

.. toctree::
    :maxdepth: 1

    whatsnew/index
    contributing
    release_guide
    faq
    licenses
