import os
import json
import sys
import getpass

import h5pyd
from h5pyd._credentials import write_private_file
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


#
# input function that works with Python 2 or 3
#
def get_input(prompt):
    result = input(prompt)
    return result


def get_secret(prompt):
    """ read a secret without echoing it when stdin is a terminal """
    try:
        interactive = sys.stdin.isatty()
    except (AttributeError, ValueError):
        interactive = False
    if interactive:
        return getpass.getpass(prompt)
    return get_input(prompt)


def masked(value):
    """ how a secret is shown as a default """
    if not value or value == "None":
        return "None"
    return "****"


#
# Save configuration file
#
def saveConfig(username, password, endpoint, api_key):

    filepath = os.path.expanduser('~/.hscfg')
    print(f"Saving config file to: {filepath}")
    lines = ["# HDFCloud configuration file"]
    lines.append(f"hs_endpoint = {endpoint if endpoint else ''}")
    lines.append(f"hs_username = {username if username else ''}")
    lines.append(f"hs_password = {password if password else ''}")
    lines.append(f"hs_api_key = {api_key if api_key else ''}")
    # the file holds the password - create it readable by the owner only
    write_private_file(filepath, "\n".join(lines) + "\n")


#
# Check to see if we can get a response from the server
#
def pingServer(username, password, endpoint, api_key):
    if not endpoint.startswith("http"):
        print("endpoint must start with 'http...'")
        return False

    try:
        info = h5pyd.getServerInfo(username=username, password=password, endpoint=endpoint, api_key=api_key)
        if 'state' not in info:
            print("unexpected response from server")
            return False
        state = info["state"]
        if state != "READY":
            print("Server is not ready, please try later")
            return False
    except IOError as ioe:
        if ioe.errno == 401:
            print("Unauthorized (username/password or api key not valid)")
            return False
        elif ioe.errno == 403:
            print("forbidden (account not setup?)")
            return False
        elif ioe.errno:
            eprint(f"Unexpected error: {ioe.errno}")
            return False
        else:
            print("Couldn't connect to server")
            return False
    except json.decoder.JSONDecodeError:
        print("Unexpected response from server")
        return False
    return True


#
# Main
#
def main():
    cfg = Config()
    if cfg.config_file:
        print(f"Using config file: {os.path.abspath(cfg.config_file)}")

    hs_endpoint = cfg["hs_endpoint"]
    if not hs_endpoint:
        hs_endpoint = "None"
    hs_username = cfg["hs_username"]
    if not hs_username:
        hs_username = "None"
    hs_password = cfg["hs_password"]
    if not hs_password:
        hs_password = "None"
    hs_api_key = cfg["hs_api_key"]
    if not hs_api_key:
        hs_api_key = "None"

    done = False
    dirty = False

    while not done:
        print("Enter new values or accept defaults in brackets with Enter.")
        print("")
        new_endpoint = get_input(f"Server endpoint [{hs_endpoint}]: ")
        if new_endpoint:
            print(f"Updated endpoint [{new_endpoint}]:")
            hs_endpoint = new_endpoint
            dirty = True

        new_username = get_input(f"Username [{hs_username}]: ")
        if new_username:
            print(f"Updated username: [{new_username}]")
            hs_username = new_username
            dirty = True

        new_password = get_secret(f"Password [{masked(hs_password)}]: ")
        if new_password:
            print("Updated password")
            hs_password = new_password
            dirty = True

        new_api_key = get_secret(f"API Key [{masked(hs_api_key)}]: ")
        if new_api_key:
            print("Updated api key")
            hs_api_key = new_api_key
            dirty = True
        if hs_api_key and hs_api_key.lower() == "none":
            hs_api_key = None

        print("Testing connection...")
        ok = pingServer(hs_username, hs_password, hs_endpoint, hs_api_key)
        if ok:
            print("connection ok")
        if dirty:
            update = get_input("Save changes? (Y/N)")
            if update in ("Y", "y"):
                saveConfig(hs_username, hs_password, hs_endpoint, hs_api_key)
                break

        quit = get_input("Quit? (Y/N)")
        if quit in ("Y", "y"):
            break


if __name__ == "__main__":
    main()
