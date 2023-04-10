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
import sys
import json
import logging

default_cfg = {
    "hs_endpoint": {
        "default": None,
        "flags": ["-e", "--endpoint"],
        "help": "server endpoint, e.g. http://hsdshdflab.hdfgroup.org",
        "choices": ["ENDPOINT",]
    },
    "hs_username": {
        "default": None,
        "flags": ["-u", "--user"],
        "help": "user name credential",
        "choices": ["USERNAME",]
    },
    "hs_password": {
        "default": None,
        "flags": ["-p", "--password"],
        "help": "password credential",
        "choices": ["PASSWORD",]
    },
    "hs_api_key": {
        "default": None,
        "flags": ["--api_key",],
        "help": "user api key",
        "choices": ["API_KEY"]
    }, 
    "hs_bucket": {
        "default": None,
        "flags": ["--bucket",],
        "help": "storage Bucket to use (S3 Bucket, Azure Container, or top-level directory)",
        "choices": ["BUCKET",]
    },

    "loglevel": {
        "default": "error",
        "flags": ["--loglevel",],
        "help": "logging verbosity",
        "choices": ["debug", "info", "warning", "error"],
    },
    "logfile": {
        "default": None,
        "flags": ["--logfile",],
        "help": "file to send logout to (otherwise stdout)",
        "choices": ["FILENAME",]
    },
    "verbose": {
        "default": False,
        "flags": ["--verbose", "-v"],
        "help": "verbose output",
    },
    "ignore": {
        "default": False,
        "flags": ["--ignore",],
        "help": "don't exit on error"
    } 
}

hscmds = ("hsinfo", "hsconfigure", "hsls", "hstouch", "hsload", "hsget", "hsacl", "hsrm", "hsdiff")

class Config:
    """
    User Config state
    """
    def __init__(self, config_file=None, custom_entries=[], **kwargs):
        self._names = []
        self._values = {}
        self._flags = {}
        self._help = {}
        self._choices = {}
        self._flag_map = {}

        # set default entries
        for defaults in (default_cfg, custom_entries):
            for name in defaults:
                if name in self._names:
                    raise ValueError(f"config {name} already set")
                entry = defaults[name]
                self._names.append(name)
                if "default" in entry:
                    self._values[name] = entry["default"]
                if "flags" in entry:
                    self._flags[name] = entry["flags"]
                    for flag in entry["flags"]:
                        self._flag_map[flag] = name
                if "help" in entry:
                    self._help[name] = entry["help"]
                if "choices" in entry:
                    self._choices[name] = entry["choices"]

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
                    if k not in self._names:
                        raise ValueError(f"undefined option: {name}")
                    if k in self._choices:
                        choices = self._choices[k]
                        if len(choices) > 1 and v not in self._choices:
                            raise ValueError(f"option {k} must be one of {choices}")
                    self._values[k] = v
        # override any config values with environment variable if found
        for k in self._names:
            if k.upper() in os.environ:
                v = os.environ[k.upper()]
                if name in self._choices:
                    choices = self._choices[name]
                    if len(choices) > 1 and v not in self._choices:
                        raise ValueError(f"option {name} must be one of {choices}")
                self._values[k] = v

        # finally update any values that are passed in to the constructor
        for name in kwargs.keys():
            if name in self._names:
                v = kwargs[name]
                if name in self._choices:
                    choices = self._choices[name]
                    if len(choices) > 1 and v not in self._choices:
                        raise ValueError(f"option {name} must be one of {choices}")
                self._values[name] = kwargs[name]

    def __getitem__(self, name):
        """ Get a config item  """
        if name not in self._names:
            return None
        return self._values[name]

    def setitem(self, name, value, flags=None, choices=None, help=None):
        """ Set a config item """
        if name not in self._names:
            self._names.append(name)
        self._values[name] = value 
        if flags is not None:
            self._flags[name] = flags
            for flag in flags:
                self._flag_map[flag] = name
        if choices is not None:
            self._choices[name] = choices
        if help is not None:
            self._help[name] = help

    def __setitem__(self, name, value):
        self.setitem(name, value)
     
    def __len__(self):
        return len(self._names)

    def __iter__(self):
        """ Iterate over config names """
        for name in self._names:
            yield name

    def __contains__(self, name):
        return name in self._names

    def __repr__(self):
        return json.dumps(self._values)

    def keys(self):
        return self._names

    def get_flags(self, name):
        if name in self._flags:
            return self._flags[name]
        else:
            return None

    def get_help(self, name):
        if name in self._help:
            return self._help[name]
        else:
            return None

    def get_see_also(self, this_cmd):
        msg = "See also the commands: "
        for cmd in hscmds:
            if cmd != this_cmd:
                msg += f"{cmd}, "
        msg = msg[:-2]  # remove trailing comma
        return msg
        

    def get_help_message(self, name):
        help_text= self.get_help(name)
        flags = self.get_flags(name)
        choices = self.get_choices(name)
        if not help_text or len(flags) == 0:
            return None
        
        msg = flags[0]
        for i in range(1, len(flags)):
            msg += f", {flags[i]}"
        if choices:
            if len(choices) == 1:
                msg += f" {choices[0]}"
            else:
                msg += " {" 
                for choice in choices:
                    msg += f"{choice}|"
                msg = msg[:-1]
                msg += "}"
        if len(msg) < 40:
            pad = " "*(40 - len(msg))
            msg += pad
        
        msg += f" {help_text}"
        
        return msg
        

    def get_nargs(self, name):
        choices = self._choices.get(name)
        if not choices:
            return 0
        else:
            return 1

    def get_choices(self, name):
        if name in self._choices:
            return self._choices[name]
        else:
            return 0

    def get_names(self):
        return self._names

    def set_cmd_flags(self, args, allow_post_flags=False):
        """ process any command line options
            return any place argument as a list 
        """
        options = []
        argn = 0
        while argn < len(args):
            arg = args[argn]
            val = None
            if len(args) > argn + 1:
                val = args[argn+1]
            if not arg.startswith("-"):
                options.append(arg)
                argn += 1
            elif options:
                if allow_post_flags:
                    options.append(arg)
                    argn += 1
                else:
                    raise ValueError("flags must be set before positional arguments")
            else:
                name = self._flag_map.get(arg)
                if arg in ("-h", "--help"):
                    raise ValueError()  # trigger print usage
                if name not in self._names:
                    raise ValueError("option not found")
                if not self.get_nargs(name):
                    # set flag
                    self._values[name] = True
                    argn += 1
                else:
                    if not val:
                        raise ValueError("option value missing")
                    if self._choices.get(name):
                        choices = self._choices.get(name)

                        if choices and len(choices) > 1 and val not in self._choices.get(name):
                            raise ValueError(f"option value must be one of {self._choices.get(name)}")
                    self._values[name] = val
                    argn += 2
        return options
    
    def get_loglevel(self):
        val = self._values["loglevel"]
        val = val.upper()
        choices = ("DEBUG", "INFO", "WARNING", "ERROR")
        if val == "DEBUG":
            loglevel = logging.DEBUG
        elif val == "INFO":
            loglevel = logging.INFO
        elif val in ("WARN", "WARNING"):
            loglevel = logging.WARNING
        elif val == "ERROR":
            loglevel = logging.ERROR
        else:
            raise ValueError(f"loglevel must be one of {choices}")
        return loglevel

    def get_cmd(self):
        """ return command argument used to invoke"""
        cmd = sys.argv[0].split('/')[-1]
        if cmd.endswith(".py"):
            cmd = "python " + cmd
        return cmd

    def print(self, msg):
        if self._values.get("logfile"):
            # write msg to logfile as info
            logging.info(msg)
        if self._values.get("verbose"):
            print(msg)
         
    








