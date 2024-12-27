##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

from __future__ import absolute_import

from packaging.version import Version, parse
import sys
import numpy

version = "0.20.0"

hdf5_version = "REST"

_exp = parse(version)

version_tuple = _exp._version + (
    ("".join(str(x) for x in _exp.pre),)
    if _exp.is_prerelease
    else ("",)
)

api_version_tuple = (0, 20, 0)
api_version = "0.20.0"

__doc__ = f"""\
This is h5pyd **{version}**

"""

info = f"""\
Summary of the h5pyd configuration
---------------------------------

h5pyd    {version}
Python  {sys.version}
sys.platform    {sys.platform}
sys.maxsize     {sys.maxsize}
numpy   {numpy.__version__}
"""
