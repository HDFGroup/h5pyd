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
* Mailing list: hdf-forum@lists.hdfgroup.org <hdf-forum@lists.hdfgroup.org>
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

By default the examples look for a local instance of h5serv.  See the  https://github.com/HDFGroup/h5serv
for instructions on installing and running h5serv.

These tests are also to designed to work with HSDS (see https://github.com/HDFGroup/hsds).  Install HSDS locally, or set environment variables (see next section)
to point to an existing HSDS instance.

Testing
-------
Setup the following environment variables that inform h5pyd which endpoint and username to use:

* ``HS_ENDPOINT`` - "http://127.0.0.1:5000" for h5serv installed locally or appropriate remote endpoint
* ``HS_USERNAME`` - "test_user1" or your preferred username
* ``HS_PASSWORD`` - password for above username - "test" for test_user1 with local h5serv install
* ``TEST2_USERNAME`` - "test_user2" or your preffered username
* ``TEST2_PASSWORD`` - password for above username - "test" for test_user2 with local h5serv install
* ``H5PYD_TEST_FOLDER`` - server folder that will be used for generated files.  Use: "h5pyd_test.hdfgroup.org" for local h5serv install.  For HSDS, posix-style paths are also supported, e.g.: /home/bob/h5pyd_test.  For HSDS use hstouch command to create the folder before running the test, e.g.: ``$ hstouch /home/bob/h5pyd_test/``

Run: ``$python testall.py`` to execute the test suite.

Uninstalling
-------------

Just remove the install directory and all contents to uninstall.


Reporting bugs (and general feedback)
-------------------------------------

Create new issues at http://github.com/HDFGroup/h5pyd/issues for any problems you find.

For general questions/feedback, please use the Kita forum (https://forum.hdfgroup.org/c/kita).
