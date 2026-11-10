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
"""
Helpers for handling credentials: files that hold secrets, warnings about where credentials
are sent, and redacting secrets from log messages.

Every check here only warns.  None of them changes which credentials are sent or refuses a
configuration that works today.
"""

import ipaddress
import logging
import os
import re
import stat
import warnings
from urllib.parse import urlsplit

# config keys whose values are secrets
SECRET_KEYS = ("hs_password", "hs_api_key", "hs_ad_client_secret", "hs_keycloak_client_secret")

# environment variable that turns off the warning about credentials over plain http
ALLOW_HTTP_AUTH_ENV = "H5PYD_ALLOW_HTTP_AUTH"

log = logging.getLogger("h5pyd")

_warned = set()  # (kind, key) pairs already warned about in this process


def _warn_once(kind, key, msg):
    if (kind, key) in _warned:
        return
    _warned.add((kind, key))
    warnings.warn(msg, UserWarning, stacklevel=3)


def _reset_warnings():
    """ forget which warnings were issued - for tests """
    _warned.clear()


# ----------------------------------------------------------------------------
# files that hold secrets
# ----------------------------------------------------------------------------

def write_private_file(path, text):
    """ Write text to path, readable and writable only by the owner (on POSIX).

    A new file is created with mode 0600, and an existing file is set to 0600 before it is
    written, so the secret is never in a file that others can read. """
    if os.name != "posix":
        with open(path, "w") as f:
            f.write(text)
        return
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)
    except OSError as e:
        log.warning(f"unable to set mode 0600 on {path}: {e}")
    with os.fdopen(fd, "w") as f:
        f.write(text)


def _secret_keys_in(path, keys):
    """ return the keys in keys that have a non-empty value in the config file at path """
    found = set()
    try:
        with open(path) as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = k.strip()
                v = v.strip()
                if k in keys and v and not v.startswith("#") and v.upper() != "NONE":
                    found.add(k)
    except (OSError, UnicodeDecodeError):
        pass
    return found


def check_config_file_mode(path, keys=SECRET_KEYS):
    """ Warn (once per path) when a config file that holds a secret can be read by other
    users.  POSIX only. """
    if os.name != "posix":
        return
    try:
        st = os.stat(path)
    except OSError:
        return
    if not st.st_mode & (stat.S_IRGRP | stat.S_IROTH):
        return
    found = _secret_keys_in(path, keys)
    if not found:
        return
    mode = stat.S_IMODE(st.st_mode)
    msg = f"{path} holds {', '.join(sorted(found))} and is readable by other users "
    msg += f"(mode {mode:04o}); run: chmod 600 {path}"
    _warn_once("config_mode", os.path.realpath(path), msg)


def check_token_cache_mode(path):
    """ Warn (once per path) when the OpenID token cache can be read or changed by other
    users.  POSIX only. """
    if os.name != "posix":
        return
    try:
        st = os.stat(path)
    except OSError:
        return
    problems = []
    if st.st_mode & (stat.S_IRGRP | stat.S_IROTH):
        problems.append("read")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        problems.append("changed")
    if not problems:
        return
    mode = stat.S_IMODE(st.st_mode)
    msg = f"token cache {path} can be {' and '.join(problems)} by other users "
    msg += f"(mode {mode:04o}); run: chmod 600 {path}"
    _warn_once("token_mode", os.path.realpath(path), msg)


def check_cwd_config(path):
    """ Warn (once per path) when a .hscfg in the current directory, which replaces
    ~/.hscfg, isn't owned by the current user or can be changed by other users. """
    log.info(f"using config file: {os.path.abspath(path)}")
    if os.name != "posix":
        return
    try:
        st = os.stat(path)
    except OSError:
        return
    problems = []
    if st.st_uid != os.getuid():
        problems.append("is owned by another user")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        problems.append("can be changed by other users")
    if not problems:
        return
    msg = f"using config file {os.path.abspath(path)} from the current directory, which "
    msg += " and ".join(problems) + "; it replaces ~/.hscfg"
    _warn_once("cwd_config", os.path.realpath(path), msg)


# ----------------------------------------------------------------------------
# where credentials are sent
# ----------------------------------------------------------------------------

def _is_loopback(host):
    if not host:
        return False
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def warn_plaintext_credentials(url, what="credentials"):
    """ Warn (once per host) when credentials are about to be sent over plain http to a host
    that isn't this machine.  Set H5PYD_ALLOW_HTTP_AUTH=1 to turn the warning off. """
    if not url:
        return
    parts = urlsplit(url)
    if parts.scheme.lower() != "http":
        return  # https, http+unix, ...
    if _is_loopback(parts.hostname):
        return
    if os.environ.get(ALLOW_HTTP_AUTH_ENV, "").strip().lower() in ("1", "true", "yes"):
        return
    origin = normalize_endpoint(url)
    msg = f"sending {what} to {redact(url)} over unencrypted http; use https, or set "
    msg += f"{ALLOW_HTTP_AUTH_ENV}=1 if the network is trusted"
    _warn_once("http_auth", origin, msg)


def normalize_endpoint(endpoint):
    """ return (scheme, host, port) for an http(s) endpoint, or the endpoint itself """
    if not endpoint:
        return None
    parts = urlsplit(endpoint)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        return endpoint.rstrip("/")
    try:
        port = parts.port
    except ValueError:
        return endpoint.rstrip("/")
    if port is None:
        port = 443 if scheme == "https" else 80
    return (scheme, (parts.hostname or "").lower(), port)


def warn_endpoint_mismatch(endpoint, configured_endpoint):
    """ Warn (once per endpoint) when credentials from the config file or environment are
    about to be sent to an endpoint other than the configured hs_endpoint. """
    if not endpoint or not configured_endpoint:
        return
    if normalize_endpoint(endpoint) == normalize_endpoint(configured_endpoint):
        return
    msg = f"sending the credentials configured for {redact(configured_endpoint)} to "
    msg += f"{redact(endpoint)}; pass username and password (or api_key) for that endpoint"
    _warn_once("endpoint_mismatch", normalize_endpoint(endpoint), msg)


# ----------------------------------------------------------------------------
# log redaction
# ----------------------------------------------------------------------------

_API_KEY_RE = re.compile(r"(api_key=)[^&\s'\"]*")
_USERINFO_RE = re.compile(r"(\w[\w+.-]*://)[^/@\s'\"]+@")


def redact(text):
    """ return text with api_key query values and URL userinfo replaced by *** """
    text = str(text)
    text = _API_KEY_RE.sub(r"\1***", text)
    text = _USERINFO_RE.sub(r"\1***@", text)
    return text
