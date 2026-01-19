import os
from pywebpush import Vapid
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_public_key
import hashlib
import secrets
import shutil
import base64
import traceback
import requests
import re
import time

def generate_keys():
    """ Generates a pair (pub/priv) of vapid keys for webpush notification, prints them out, and saves them to the ./secrets/vapid-keys.txt dir """
    os.makedirs("secrets", exist_ok=True) # we will write here, make sure it exsists
    v = Vapid()
    v.generate_keys()
    public_key_o = load_pem_public_key(v.public_pem())
    pub_bytes = public_key_o.public_bytes(encoding=serialization.Encoding.X962, format=serialization.PublicFormat.UncompressedPoint) # this is apparently right
    pub_b64 = base64.urlsafe_b64encode(pub_bytes).rstrip(b'=').decode('utf-8') # the public key needs to be in B64URL
    private = v.private_pem().decode('utf-8').strip().replace("\n", "").removeprefix("-----BEGIN PRIVATE KEY-----").removesuffix("-----END PRIVATE KEY-----")
    print(f"Public Vapid Key: {pub_b64}")
    print(f"Private Vapid Key: {private}")
    with open("./secrets/vapid-keys.txt", 'w') as w:
        w.writelines([pub_b64 + "\n", private])
    return (pub_b64, private)

def generate_admin():
    """ Generates admin credentials for the inputted username and password, as well as a random flask secret key, and saves them to secrets/admin.txt """
    os.makedirs("secrets", exist_ok=True)
    un = input("Enter username: ")
    pwd = line_str_hash(input("Enter password: "))
    sec = secrets.token_hex(32)
    with open("./secrets/admin.txt", 'w') as f:
        f.write(un + '\n' + pwd + '\n' + sec)
    return (un, pwd, sec)

def generate_default_admin():
    """ Generates default admin credentials so as not to rely on input() """
    os.makedirs("secrets", exist_ok=True)
    pwd = line_str_hash("admin")
    sec = secrets.token_hex(32)
    with open("./secrets/admin.txt", 'w') as f:
        f.write("admin" + '\n' + pwd + '\n' + sec)
    return ("admin", pwd, sec)
    

def safer_replace(src, dest):
    """ os.replace dies sometimes """
    with open(src, 'rb') as fsrc, open(dest, 'wb') as fdest:
        shutil.copyfileobj(fsrc, fdest)
        fdest.flush()
        os.fsync(fdest.fileno())

    os.remove(src)

def test_tba_key(key: str):
    if key == None or key.strip() == "": # dont bother testing an empty key
        return False
    # use time.time to force a refresh of the server and prevent caches from accepting junk keys
    response = requests.get(f"https://www.thebluealliance.com/api/v3/event/2025iri?_={int(time.time()*1_000)}", headers={
        "X-TBA-Auth-Key": key,
        'Cache-Control': 'no-store, no-cache, max-age=0',
        'Pragma': 'no-cache'
    })
    if response.status_code == 401:
        return False
    elif response.status_code == 200 or response.status_code == 304:
        return True
    raise Exception(f"Error testing tba key: unexpected reseponse {response.status_code}: {response.text}")

def get_tba_ranks(event_key, api_key, teams):
    if api_key == None or api_key.strip() == "":
        return None

    ranks = requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event_key}/rankings",
            headers={"X-TBA-Auth-Key": api_key},
    ).json()
    
    return dict(map(lambda x: (x, [(t["rank"], t["sort_orders"][0]) for t in ranks["rankings"] if t["team_key"] == f"frc{x}"][0]), teams))

def load_tba_data(event_key, api_key):
    """ Loads up the teams and schedule for `event_key` and returns a tuple (teams, schedule) """

    if api_key == None or api_key.strip() == "":
        return (None, None, None) # the propogation

    teams = [
        x["team_number"]
        for x in requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event_key}/teams",
            headers={"X-TBA-Auth-Key": api_key},
        ).json()
    ]

    def sched_sorter(match): # sorting function
        key = match["k"].removeprefix(event_key + "_")
        order = {"qm": 0, "sf": 1, "f": 2}

        if key.startswith("qm"):
            x = int(key[2:])
            return (order["qm"], x, 0)
        else:
            m = re.match(r"(sf|f)(\d+)m(\d+)", key) # match sf<x>m<y>
            if m:
                prefix, round, idx = m.groups()
                return (order[prefix], int(round), int(idx))
            else:
                return (99, 0, 0)

    schedule = sorted([
        {
            "k": x["key"],
            "r": x["alliances"]["red"]["team_keys"],
            "b": x["alliances"]["blue"]["team_keys"],
        }
        for x in requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event_key}/matches",
            headers={"X-TBA-Auth-Key": api_key},
        ).json()
    ], key=sched_sorter)

    return (teams, schedule, get_tba_ranks(event_key, api_key, teams))

def read_secrets():
    """ Reads the different secrets of the repo: vapid keys, admin creds, flask secret key, and tba auth key in that order """
    vapid_keys = {}
    admin_login = {}
    if (os.path.exists('./secrets/vapid-keys.txt')):
        with open("./secrets/vapid-keys.txt", 'r') as f:
            vapid_keys["public"] = f.readline().strip()
            vapid_keys["private"] = f.readline().strip()
    else:
        vapid_keys["public"], vapid_keys["private"] = generate_keys()

    if (os.path.exists('./secrets/admin.txt')):
        with open("./secrets/admin.txt", 'r') as r:
            admin_login["un"] = r.readline().strip()
            admin_login["pwd"] = r.readline().strip()
            key = r.readline().strip()
    else:
        admin_login["un"], admin_login["pwd"], key = generate_default_admin()

    if (os.path.exists("./secrets/key.txt")):
        with open("./secrets/key.txt", 'r') as f:
            auth_key = f.read().strip()
    else:
        auth_key = ""

    return (vapid_keys, admin_login, key, auth_key)

def set_auth_key(key: str):
    with open("./secrets/key.txt", 'w') as w:
        w.write(key.strip())

def data_in_exists(app):
    return os.path.exists(f"./{app.config["UPLOAD_DIR"]}/{app.config["INPUT_FILENAME"]}")

def change_un_pwd(current_secret_key: str, newun: str, newpwd: str):
    os.makedirs("./secrets", exist_ok=True)
    with open("./secrets/admin.txt", 'w') as f:
        f.write('\n'.join([newun.strip(), newpwd.strip(), current_secret_key.strip()]))

def get_input_headers(file):
    if (os.path.exists(file)):
        with open(file, encoding='utf-8', mode='r') as r:
            return r.readline().strip().split(',')
    return []

def line_str_hash(row: str):
        """ Hashes a line of text with sha256 """
        return hashlib.sha256(row.encode("utf-8")).hexdigest()

def stream(file):
        """ Return a stream which reads a file in chunks; used for downloading in case files get big """
        with open(file, 'rb') as r:
            while chunk := r.read(8192):
                yield chunk

def exception_format(e: Exception): # bruh
        """Gets the stack frame where the exception ACTUALLY occured (deepest frame not in a dependecy)"""
        tb = traceback.extract_tb(e.__traceback__)
        for i in range(len(tb)):
            if not ".venv" in tb[len(tb) - i - 1].filename: # make all the junk go away
                tb = tb[len(tb) - i - 1]
                break
        return f"Error in {tb.filename if "filename" in tb else ""}, line {tb.lineno if "lineno" in tb else ""}, in {tb.name if "name" in tb else ""}\n" + traceback.format_exception_only(e)[0]