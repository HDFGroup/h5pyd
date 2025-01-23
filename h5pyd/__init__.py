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

from . import version
from ._hl.base import Empty
from .h5type import special_dtype, Reference, RegionReference
from .h5type import vlen_dtype, string_dtype, enum_dtype
from .h5type import check_vlen_dtype, check_string_dtype, check_enum_dtype
from .h5type import check_opaque_dtype, check_ref_dtype, check_dtype
from ._hl.files import File, H5Image, is_hdf5
from ._hl.folders import Folder
from ._hl.group import Group, SoftLink, ExternalLink, UserDefinedLink, HardLink
from ._hl.dataset import Dataset, MultiManager
from ._hl.table import Table
from ._hl.datatype import Datatype
from ._hl.attrs import AttributeManager
from .serverinfo import getServerInfo
from . import h5ds


from .config import get_config

__version__ = version.version


__doc__ = \
    f"""
    This is the h5pyd package, a Python interface to the HDF REST Server.

    Version {version.version}

    """


def enable_ipython_completer():
    import sys
    if 'IPython' in sys.modules:
        ip_running = False
        try:
            from IPython.core.interactiveshell import InteractiveShell
            ip_running = InteractiveShell.initialized()
        except ImportError:
            # support <ipython-0.11
            from IPython import ipapi as _ipapi
            ip_running = _ipapi.get() is not None
        except Exception:
            pass
        if ip_running:
            from . import ipy_completer
            return ipy_completer.load_ipython_extension()

    raise RuntimeError('completer must be enabled in active ipython session')
