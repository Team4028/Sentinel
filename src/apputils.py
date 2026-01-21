import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_public_key
import hashlib
import secrets
import shutil
import base64
import traceback
import requests
import re
import json
import time

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

def add_jsons_to_cache(js: dict):
    if os.path.exists('config/tba-cache.json'):
        with open('config/tba-cache.json', 'r') as r:
            js_tmp = json.load(r)
    else: js_tmp = {}
    for key in js.keys():
        js_tmp[key] = js[key]
    with open('config/tba-cache.json', 'w') as w:
        json.dump(js_tmp, w, indent=4)

def has_internet():
    try:
        res = requests.get("https://8.8.8.8", timeout=10)
    except Exception:
        return False
    return res.ok

def tba_health():
    try:
        res = requests.get("https://www.thebluealliance.com", timeout=10)
    except Exception:
        return False
    return res.ok

def test_tba_key(key: str):
    if key == None or key.strip() == "": # dont bother testing an empty key
        return False
    if not tba_health():
        raise Exception("Error testing tba key: no wifi")
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
    if tba_health() and not (api_key == None or api_key.strip() == ""):
        ranks = requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{event_key}/rankings",
                headers={"X-TBA-Auth-Key": api_key},
        ).json()
        add_jsons_to_cache({"ranks": ranks})
    elif os.path.exists("config/tba-cache.json"):
        with open('config/tba-cache.json', 'r') as r:
            ranks = json.load(r)['ranks']
    else:
        raise Exception("Error: no wifi or tba cache or invalid api key")
    
    return dict(map(lambda x: (x, [(t["rank"], t["sort_orders"][0]) for t in ranks["rankings"] if t["team_key"] == f"frc{x}"][0]), teams))

def load_tba_data(event_key, api_key):
    """ Loads up the teams and schedule for `event_key` and returns a tuple (teams, schedule) """

    if not tba_health() or (api_key == None or api_key.strip() == ""):
        if os.path.exists('config/tba-cache.json'):
            with open('config/tba-cache.json', 'r') as r:
                js = json.load(r)
                teams = [x["team_number"] for x in js["teams"]]
                schedJson = js["matches"]
        else:
            raise Exception("Error: no wifi or tba cache or invalid api key")
    else:
        teamJSON = requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{event_key}/teams",
                headers={"X-TBA-Auth-Key": api_key},
            ).json()
        teams = [
            x["team_number"]
            for x in teamJSON
        ]
        schedJson = requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event_key}/matches",
            headers={"X-TBA-Auth-Key": api_key},
        ).json()
        add_jsons_to_cache({"teams": teamJSON, "matches": schedJson})

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
        for x in schedJson
    ], key=sched_sorter)

    return (teams, schedule, get_tba_ranks(event_key, api_key, teams))

def read_secrets():
    """ Reads the different secrets of the repo: admin creds, flask secret key, and tba auth key in that order """
    admin_login = {}

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

    return (admin_login, key, auth_key)

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