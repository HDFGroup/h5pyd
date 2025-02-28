import os
import json
import sys

import h5pyd
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


#
# Save configuration file
#
def saveConfig(username, password, endpoint, api_key):

    filepath = os.path.expanduser('~/.hscfg')
    print(f"Saving config file to: {filepath}")
    with open(filepath, 'w') as file:
        file.write("# HDFCloud configuration file\n")
        if endpoint:
            file.write(f"hs_endpoint = {endpoint}\n")
        else:
            file.write("hs_endpoint = \n")
        if username:
            file.write(f"hs_username = {username}\n")
        else:
            file.write("hs_username = \n")
        if password:
            file.write(f"hs_password = {password}\n")
        else:
            file.write("hs_password = \n")
        if api_key:
            file.write(f"hs_api_key = {api_key}\n")
        else:
            file.write("hs_api_key = \n")


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

        new_password = get_input(f"Password [{hs_password}]: ")
        if new_password:
            print(f"Updated password: [{new_password}]")
            hs_password = new_password
            dirty = True

        new_api_key = get_input(f"API Key [{hs_api_key}]: ")
        if new_api_key:
            print(f"Updated api key: [{new_api_key}]")
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
