h5pyd
=====

.. image:: https://travis-ci.org/HDFGroup/h5pyd.svg?branch=master
    :target: https://travis-ci.org/HDFGroup/h5pyd

Python client library for HDF5 REST interface


Introduction
------------
This repository contains library, test, and examples of h5pyd - a Python package for the
HDF REST interface.

The library is provides a high-level interface to the REST specification that is generally
easier to use than invoking http calls directly.

The package is based on the popular h5py package and aims to be source compatible with
the h5py high level interface.


Websites
--------

* Main website: http://www.hdfgroup.org
* Source code: https://github.com/HDFGroup/h5pyd
* Forum: https://forum.hdfgroup.org/c/hsds
* Documentation: TBD (but http://docs.h5py.org/en/latest/ should be helpful)

Related Projects
----------------

* HSDS: https://github.com/HDFGroup/hsds
* HDF Server: https://github.com/HDFGroup/h5serv
* HDF5-JSON: https://github.com/HDFGroup/hdf5-json
* h5py: https://github.com/h5py/h5py
* REST API Documentation: https://github.com/HDFGroup/hdf-rest-api

Installing
-----------

Via pip::

   pip install h5pyd

From a release tarball or Git checkout::

   python setup.py install

By default the examples look for a local instance of HSDS.  See the  https://github.com/HDFGroup/hsds
for instructions on installing and running HSDS.

These tests are also to designed to work with HSDS (see https://github.com/HDFGroup/hsds).  Install HSDS locally, or set environment variables (see next section)
to point to an existing HSDS instance.

h5pyd can all be run in serverless mode with either AWS Lambda or direct mode (storage system accessed directly).

To use with AWS Lambda, set the HS_ENDPOINT to: "http+lambda://hslambda" where "hslambda" is the name
of the lambda function.  When using AWS Lambda some additional environment variables need to be set:

* ``AWS_LAMBDA_GATEWAY`` - AWS Lambda endpoint, e.g.: ``https://lambda.us-west-2.amazonaws.com``
* ``AWS_REGION`` - Region where the Lambda function is installed, e.g.: ``us-west-2``
* ``AWS_SECRET_ACCESS_KEY`` - Your AWS secret access AWS_SECRET_ACCESS_KEY
* ``AWS_ACCESS_KEY_ID`` - Your AWS access key ID


To use in direct mode, set the HS_ENDPOINT to "local".  For direct mode, some additional environment
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

Serveral utility applications are included with this package:

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
Setup the following environment variables that inform h5pyd which endpoint and username to use:

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
