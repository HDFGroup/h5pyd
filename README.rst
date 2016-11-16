h5pyd
=====

.. image:: https://travis-ci.org/HDFGroup/h5pyd.svg?branch=develop
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
* HDF Server: https://github.com/HDFGroup/h5serv
* HDF5-JSON: https://github.com/HDFGroup/h5-json
* h5py: https://github.com/h5py/h5py 

Installing
-----------

Via pip::

   pip install h5pyd
   
From a release tarball or Git checkout::

   python setup.py install
   
By default the examples look for a local instance of h5serv.  See the  https://github.com/HDFGroup/h5serv
for instructions on installing and running h5serv. 

 
Uninstalling
-------------

Just remove the install directory and all contents to uninstall.

    
Reporting bugs (and general feedback)
-------------------------------------

Create new issues at http://github.com/HDFGroup/h5pyd/issues for any problems you find. 

For general questions/feedback, please use the hdf list (hdf-forum@lists.hdfgroup.org).

