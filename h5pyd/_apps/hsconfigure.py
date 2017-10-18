import sys
import os
import json
import h5pyd
if __name__ == "__main__":
    from config import Config
else:
    from .config import Config

#
# Save configuration file
#
def saveConfig(username, password, endpoint):
    filepath = os.path.expanduser('~/.hscfg')
    print("Saving config file to: {}".format(filepath))
    with open(filepath, 'w') as file:
        file.write("# HDFCloud configuration file\n")
        file.write("hs_endpoint = {}\n".format(endpoint))
        file.write("hs_username = {}\n".format(username))
        file.write("hs_password = {}\n".format(password))
   
#
# Check to see if we can get a response from the server
#
def pingServer(username, password, endpoint):
    try:
        info = h5pyd.getServerInfo(username=username, password=password, endpoint=endpoint)
        if 'state' not in info:
            print("unexpected response from server")
            return False
        state = info["state"]
        if state != "READY":
            print("Server is not ready, please try later")
            return False  
    except IOError as ioe:
        if ioe.errno == 401:
            print("username/password not valid")
            return False
        elif ioe.errno:
            print("Unexpected error: {}".format(ioe.errno))
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
    hs_username = cfg["hs_username"]
    hs_password = cfg["hs_password"]

    done = False
    dirty = False

    while not done:
        print("Enter new values or accept defaults in brackets with Enter.")
        print("")
    
        new_endpoint = input("Server endpoint [{}]: ".format(hs_endpoint))
        if new_endpoint:
            print("Updated endpoint [{}]:".format(new_endpoint))
            hs_endpoint = new_endpoint
            dirty = True
         
    
        new_username = input("Username [{}]: ".format(hs_username))
        if new_username:
            print("Updated username: [{}]".format(new_username))
            hs_username = new_username
            dirty = True
        
        new_password = input("Password [{}]: ".format(hs_password))
        if new_password:
            print("updated password: [{}]".format(new_password))
            hs_password = new_password
            dirty = True
        
        print("Testing connection...")
        ok = pingServer(hs_username, hs_password, hs_endpoint)
        if ok:
            print("connection ok")
        if dirty:
            update = input("Save changes? (Y/N)")
            if update in ("Y", "y"):
                saveConfig(hs_username, hs_password, hs_endpoint)
                dirty = False
        quit = input("Quit? (Y/N)")
        if quit in ("Y", "y"):
            break

if __name__ == "__main__":
    main()