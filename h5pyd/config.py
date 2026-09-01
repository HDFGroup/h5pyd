##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import os
import json


class Config:
    """
    User Config state
    """
    _cfg = {}  # global state

    def __init__(self, config_file=None, **kwargs):
        if Config._cfg:
            return  # already initialized
        if config_file:
            self._config_file = config_file
        elif os.path.isfile(".hscfg"):
            self._config_file = ".hscfg"
        else:
            self._config_file = os.path.expanduser("~/.hscfg")
        # process config file if found
        if os.path.isfile(self._config_file):
            line_number = 0
            with open(self._config_file) as f:
                for line in f:
                    line_number += 1
                    s = line.strip()
                    if not s:
                        continue
                    if s[0] == '#':
                        # comment line
                        continue
                    fields = s.split('=')
                    if len(fields) < 2:
                        print(f"config file: {self._config_file} line: {line_number} is not valid")
                        continue
                    k = fields[0].strip()
                    v = fields[1].strip()
                    if k == "complex_names":
                        self.complex_names = v
                    elif k == "bool_names":
                        self.bool_names = v
                    elif k == "track_order":
                        self.track_order = v
                    else:
                        Config._cfg[k] = v

        # add standard keys if not already picked up
        for k in ("hs_endpoint", "hs_username", "hs_password", "hs_api_key"):
            if k not in Config._cfg:
                Config._cfg[k] = ""

        # override any config values with environment variable if found
        for k in Config._cfg.keys():
            if k.upper() in os.environ:
                Config._cfg[k] = os.environ[k.upper()]

        # update any values that are passed in to the constructor
        for k in kwargs.keys():
            Config._cfg[k] = kwargs[k]

        # finally, set defaults for any expected keys that are not already set
        for k in ("hs_endpoint", "hs_username", "hs_endpoint"):
            if k not in Config._cfg:
                Config._cfg[k] = None
        if "bool_names" not in Config._cfg:
            Config._cfg["bool_names"] = (b"FALSE", b"TRUE")
        if "complex_names" not in Config._cfg:
            Config._cfg["complex_names"] = ("r", "i")
        if "track_order" not in Config._cfg:
            Config._cfg["track_order"] = False

    def __getitem__(self, name):
        """ Get a config item  """
        if name not in Config._cfg:
            if name.upper() in os.environ:
                Config._cfg[name] = os.environ[name.upper()]
            else:
                return None
        return Config._cfg[name]

    def get(self, name, default):
        """ return option for name if found, otherwise default """
        val = self.__getitem__(name)
        if val is None:
            val = default
        return val

    def __setitem__(self, name, obj):
        """ set config item """
        Config._cfg[name] = obj

    def __delitem__(self, name):
        """ Delete option. """
        del Config._cfg[name]

    def __len__(self):
        return len(Config._cfg)

    def __iter__(self):
        """ Iterate over config names """
        keys = Config._cfg.keys()
        for key in keys:
            yield key

    def __contains__(self, name):
        return name in Config._cfg

    def __repr__(self):
        return json.dumps(Config._cfg)

    def keys(self):
        return Config._cfg.keys()

    @property
    def hs_endpoint(self):
        return Config._cfg.get("hs_endpoint")

    @property
    def hs_username(self):
        return Config._cfg.get("hs_username")

    @property
    def hs_password(self):
        return Config._cfg.get("hs_password")

    @property
    def hs_api_key(self):
        return Config._cfg.get("hs_api_key")

    @property
    def bool_names(self):
        if "bool_names" in Config._cfg:
            names = Config._cfg["bool_names"]
        else:
            names = (b"FALSE", b"TRUE")
        return names

    @bool_names.setter
    def bool_names(self, value):
        if isinstance(value, str):
            names = value.split(())
            if len(names) < 2:
                raise ValueError("bool_names must have two items")
            elif len(names) == 2:
                pass
            else:
                names = names[:2]  # just use the first two items
        elif len(value) != 2:
            raise ValueError("expected two-element list for bool_names")
        else:
            names = value
        Config._cfg["bool_names"] = tuple(names)

    @property
    def complex_names(self):
        if "complex_names" in Config._cfg:
            names = Config._cfg["complex_names"]
        else:
            names = ("r", "i")
        return names

    @complex_names.setter
    def complex_names(self, value):
        if isinstance(value, str):
            names = value.split()
            if len(names) < 2:
                raise ValueError("complex_names must have two items")
            elif len(names) == 2:
                pass
            else:
                names = names[:2]  # just use the first two items
        elif len(value) != 2:
            raise ValueError("complex_names must have two values")
        else:
            names = value

        Config._cfg["complex_names"] = tuple(names)

    @property
    def track_order(self):
        if "track_order" in Config._cfg:
            track = Config._cfg["track_order"]
        else:
            track = None
        return track

    @track_order.setter
    def track_order(self, value):
        if isinstance(value, str):
            tokens = value.split()
            if len(tokens) == 0:
                track = None
            else:
                track = bool(tokens[0])  # strip any comments
        else:
            track = bool(value)
        Config._cfg["track_order"] = track


def get_config(config_file=None, **kwargs):
    return Config(config_file=config_file, **kwargs)
