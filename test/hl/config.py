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
import os

cfg = {
    'use_h5py': False
}


def get(x):
    """
    Get the value from the environment.

    Args:
        x: (int): write your description
    """
    # see if there is a command-line override
    config_value = None

    # see if there are an environment variable override
    if x.upper() in os.environ:
        config_value = os.environ[x.upper()]
    # no command line override, just return the cfg value
    if config_value is None:
        config_value = cfg[x]

    # convert string to boolean if true or false
    if type(config_value) is str:
        if config_value.upper() in ('T', 'TRUE'):
            config_value = True
        elif config_value.upper() in ('F', 'FALSE'):
            config_value = False
    return config_value



