import os
import hashlib
import secrets
import shutil
import traceback
from typing import Any, Generator
from pandas import api
import requests
from datetime import date
import re
import json
import time
import logging
from jsonschema import Draft7Validator
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta, UTC


logger = logging.getLogger(__name__)


def generate_admin() -> tuple[str, str, str]:
    """Generates admin credentials for the inputted username and password, as well as a random flask secret key, and saves them to secrets/admin.txt"""
    os.makedirs("secrets", exist_ok=True)  # ensure secrets directory exists
    un = input("Enter username: ")
    pwd = line_str_hash(input("Enter password: "))  # hash pw into sha256
    sec = secrets.token_hex(32)  # generate random code for secret
    with open("./secrets/admin.txt", "w") as f:
        f.write(un + "\n" + pwd + "\n" + sec)  # save
    return (un, pwd, sec)


def generate_default_admin() -> tuple[str, str, str]:
    """Generates default admin credentials so as not to rely on input()"""
    os.makedirs("secrets", exist_ok=True)
    pwd = line_str_hash("admin")
    sec = secrets.token_hex(32)
    with open("./secrets/admin.txt", "w") as f:
        f.write("admin" + "\n" + pwd + "\n" + sec)
    return ("admin", pwd, sec)

def generate_ssl_sign():
    domains = ["sentinel.beaksquad.dev", "localhost"]
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Ohio"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Local"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"FRC 4028 The Beak Squad"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, u"Robotics"),
        x509.NameAttribute(NameOID.COMMON_NAME, domains[0]),
    ])

    san = x509.SubjectAlternativeName([x509.DNSName(d) for d in domains])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC))
        .not_valid_after(datetime.now(UTC) + timedelta(days=365))
        .add_extension(san, critical=False)
        .sign(key, hashes.SHA256())
    )

    with open("./secrets/sentinel-key.pem", 'wb') as w:
        w.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
        )
    
    with open('./secrets/certinel.pem', 'wb') as w:
        w.write(
            cert.public_bytes(serialization.Encoding.PEM)
        )


def safer_replace(src, dest) -> None:
    """os.replace dies sometimes"""
    with open(src, "rb") as fsrc, open(dest, "wb") as fdest:
        shutil.copyfileobj(fsrc, fdest)
        fdest.flush()
        os.fsync(fdest.fileno())

    os.remove(src)


def add_jsons_to_cache(js: dict) -> None:
    """adds each key in js to the tba cache"""
    if os.path.exists("config/tba-cache.json"):
        with open("config/tba-cache.json", "r") as r:
            js_tmp = json.load(r)
    else:
        js_tmp = {}
    for key in js:
        if key in js_tmp and isinstance(
            js_tmp[key], dict
        ):  # append to dicts instead of fully overwriting
            js_tmp[key] |= js[key]
        else:
            js_tmp[key] = js[key]
    with open("config/tba-cache.json", "w") as w:
        json.dump(js_tmp, w, indent=4)


def has_internet() -> bool:
    """pings google for 10 seconds and returns whether it's okay"""
    try:
        res = requests.get("https://8.8.8.8", timeout=10)  # ping google
    except Exception:
        return False
    return res.ok


def tba_health() -> bool:
    """pings tba for 10 seconds and returns whether it's okay"""
    try:
        res = requests.get("https://www.thebluealliance.com", timeout=10)  # ping tba
    except Exception:
        return False
    return res.ok


def yaml_check_schema_raise_errors(yamldata):
    with open("./config/schema.json", "r") as f:
        schema = json.load(f)
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(yamldata), key=lambda e: e.path)
    if errors:
        messages = [f"{list(e.path)}: {e.message}" for e in errors]
        raise Exception(f"Schema validation failed:\n{'\n'.join(messages)}")


def test_tba_key(key: str) -> bool:
    """pings tba/api/v3/status with the key given to see if the key is good"""
    if key == None or key.strip() == "":  # dont bother testing an empty key
        return False
    if not tba_health():
        raise Exception("Error testing tba key: no wifi")
    # use time.time to force a refresh of the server and prevent caches from accepting junk keys
    response = requests.get(
        f"https://www.thebluealliance.com/api/v3/status?_={int(time.time()*1_000)}",
        headers={
            "X-TBA-Auth-Key": key,
            "Cache-Control": "no-store, no-cache, max-age=0",
            "Pragma": "no-cache",
        },
    )  # ping tba api
    if response.status_code == 401:
        return False
    elif response.status_code == 200 or response.status_code == 304:
        return True
    raise Exception(
        f"Error testing tba key: unexpected reseponse {response.status_code}: {response.text}"
    )


def clear_tba_cache() -> None:
    """clears the cache of all tba fetches"""
    if os.path.exists("config/tba-cache.json"):
        os.remove("config/tba-cache.json")


def get_event_team_oprs(event_key, api_key) -> dict[Any, Any] | Any:
    """ """
    oprs = {}
    try:
        if tba_health() and not (api_key == None or api_key.strip() == ""):
            logger.info(
                f"Fetch https://www.thebluealliance.com/api/v3/event/{event_key}/oprs"
            )
            fetch_oprs = requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{event_key}/oprs",
                {"X-TBA-Auth-Key": api_key},
            ).json()
            for x, y in fetch_oprs["oprs"].items():
                oprs |= {x.removeprefix("frc"): round(float(y), 3)}
            add_jsons_to_cache({"curr_oprs": oprs})
        else:
            if os.path.exists("config/tba-cache.json"):
                with open("config/tba-cache.json") as r:
                    js = json.load(r)
                    if "curr_oprs" in js:
                        oprs = js["curr_oprs"]
                    else:
                        logger.error("Error: no wifi or tba cache or invalid api key")
                        return {}
            else:
                logger.error("Error: no wifi or tba cache or invalid api key")
                return {}
        return oprs
    except Exception as e:
        logger.error(exception_format(e))
        return {}
    
def invert_jeson(jeson):
    result = {}
    for k, k2 in jeson.items():
        for k21, k22 in k2.items():
            result.setdefault(k21, {})[k] = k22
    return result
    
def get_tba_coprs(event_key, api_key):
    coprs = {}
    try:
        if tba_health() and not (api_key == None or api_key.strip() == ""):
            logger.info(
                f"Fetch https://www.thebluealliance.com/api/v3/event/{event_key}/coprs"
            )
            coprs = requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{event_key}/coprs",
                {'X-TBA-Auth-Key': api_key},
            ).json()
            coprs = invert_jeson(coprs)
            for team in list(coprs.keys()):
                coprs[team.removeprefix('frc')] = coprs.pop(team)
            add_jsons_to_cache({"coprs": coprs})
        else:
            if os.path.exists("config/tba-cache.json"):
                with open('config/tba-cache.json') as r:
                    js = json.load(r)
                    if "coprs" in js:
                        coprs = js["coprs"]
                    else:
                        logger.error("Error: no wifi or tba cache or invalid api key")
                        return {}
            else:
                logger.error("Error: no wifi or tba cache or invalid api key")
                return {}
        return coprs
    except Exception as e:
        logger.error(exception_format(e))
        return {}

def get_tba_opr(event_key, api_key, year, teams):
    """returns a dictionary of each team to their cooresponding opr at their last competition"""
    oprs = {}
    try:
        didnt_read = True
        if os.path.exists("config/tba-cache.json"):  # use cache first because lots of data
            with open("config/tba-cache.json", "r") as r:
                js = json.load(r)
                if "oprs" in js:
                    oprs = js["oprs"]
                    didnt_read = False
        if didnt_read and tba_health() and not (api_key == None or api_key.strip() == ""):
            for team in teams:
                opr = 0.0
                logger.info(
                    f"Fetch: https://www.thebluealliance.com/api/v3/team/frc{team}/events/{year}"
                )
                events = requests.get(
                    f"https://www.thebluealliance.com/api/v3/team/frc{team}/events/{year}",
                    headers={"X-TBA-Auth-Key": api_key},
                ).json()  # get events that team was in
                curr_date = date.today().strftime("%Y-%m-%d")
                latest_not_over = "0000-00-00"
                latest_no_event = None
                for event in events:
                    if (
                        event["start_date"] < curr_date
                        and event["key"] != event_key
                        and event["start_date"] > latest_not_over
                        and not (event["event_type"] in [4, 99])
                    ):  # 99 => offseason, 4 => einstein
                        latest_not_over = event["start_date"]
                        latest_no_event = event
                if latest_no_event:
                    logger.info(
                        f"Fetch: https://www.thebluealliance.com/api/v3/event/{latest_no_event["key"]}/oprs"
                    )
                    opr = float(
                        requests.get(
                            f"https://www.thebluealliance.com/api/v3/event/{latest_no_event["key"]}/oprs",
                            headers={"X-TBA-Auth-Key": api_key},
                        ).json()["oprs"][f"frc{team}"]
                    )  # get the teams opr from that event
                oprs |= {team: round(opr, 3)}
            add_jsons_to_cache({"oprs": oprs})
        elif didnt_read:
            logger.error("Error: no wifi or tba cache or invalid api key")
            return {}
        return oprs
    except Exception as e:
        logger.error(exception_format(e))
        return {}


def get_tba_ranks(event_key, api_key, teams):
    """returns a dictionary mapping each team to a tuple of their rank and rps"""
    try:
        if tba_health() and not (
            api_key == None or api_key.strip() == ""
        ):  # prioritize live fetch for ranks because they update quickly
            logger.info(
                f"Fetch: https://www.thebluealliance.com/api/v3/event/{event_key}/rankings"
            )
            ranks = requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{event_key}/rankings",
                headers={"X-TBA-Auth-Key": api_key},
            ).json()
            add_jsons_to_cache({"ranks": ranks})
        else:
            if os.path.exists("config/tba-cache.json"):
                with open("config/tba-cache.json", "r") as r:
                    js = json.load(r)
                    if "ranks" in js:
                        ranks = js["ranks"]
                    else:
                        logger.error("Error: no wifi or tba cache or invalid api key")
                        return {}
            else:
                logger.error("Error: no wifi or tba cache or invalid api key")
                return {}

        return dict(
            map(
                lambda x: (
                    x,
                    [
                        (t["rank"], t["sort_orders"][0])
                        for t in ranks["rankings"]
                        if t["team_key"] == f"frc{x}"
                    ][0],
                ),
                teams,
            )
        )
    except Exception as e:
        logger.error(exception_format(e))
        return {}


def load_tba_data(event_key, api_key, year):
    """Loads up the teams and schedule for `event_key` and returns a tuple (teams, schedule)"""
    didnt_read = True
    if os.path.exists("config/tba-cache.json"):
        with open("config/tba-cache.json", "r") as r:
            js = json.load(r)
            if "teams" in js and "matches" in js:
                teams = [x["team_number"] for x in js["teams"]]
                schedJson = js["matches"]
                didnt_read = False
    elif didnt_read:
        if (not tba_health()) or (api_key == None or api_key.strip() == ""):
            raise Exception("Error: no wifi or tba cache or invalid api key")
        logger.info(
            f"Fetch: https://www.thebluealliance.com/api/v3/event/{event_key}/teams"
        )
        teamJSON = requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event_key}/teams",
            headers={"X-TBA-Auth-Key": api_key},
        ).json()
        teams = [x["team_number"] for x in teamJSON]
        logger.info(
            f"Fetch: https://www.thebluealliance.com/api/v3/event/{event_key}/matches"
        )
        schedJson = requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event_key}/matches",
            headers={"X-TBA-Auth-Key": api_key},
        ).json()
        add_jsons_to_cache({"teams": teamJSON, "matches": schedJson})

    def sched_sorter(match):  # sorting function
        key = match["k"].removeprefix(event_key + "_")
        order = {"qm": 0, "sf": 1, "f": 2}

        if key.startswith("qm"):
            x = int(key[2:])
            return (order["qm"], x, 0)
        else:
            m = re.match(r"(sf|f)(\d+)m(\d+)", key)  # match (s)f<x>m<y>
            if m:
                prefix, round, idx = m.groups()
                return (order[prefix], int(round), int(idx))
            else:
                return (99, 0, 0)

    schedule = sorted(
        [
            {
                "k": x["key"],
                "r": x["alliances"]["red"]["team_keys"],
                "b": x["alliances"]["blue"]["team_keys"],
            }
            for x in schedJson
        ],
        key=sched_sorter,
    )

    return (
        teams,
        schedule,
        get_tba_ranks(event_key, api_key, teams),
        get_tba_opr(event_key, api_key, year, teams),
        get_tba_coprs(event_key, api_key),
        get_event_team_oprs(event_key, api_key),
    )


def read_secrets():
    """Reads the different secrets of the repo: admin creds, flask secret key, and tba auth key in that order"""
    admin_login = {}

    if os.path.exists("./secrets/admin.txt"):
        with open("./secrets/admin.txt", "r") as r:
            admin_login["un"] = r.readline().strip()
            admin_login["pwd"] = r.readline().strip()
            key = r.readline().strip()
    else:
        admin_login["un"], admin_login["pwd"], key = generate_default_admin()

    if os.path.exists("./secrets/key.txt"):
        with open("./secrets/key.txt", "r") as f:
            auth_key = f.read().strip()
    else:
        auth_key = ""

    return (admin_login, key, auth_key)


def set_auth_key(key: str) -> None:
    """sets the tba key to `key`"""
    with open("./secrets/key.txt", "w") as w:
        w.write(key.strip())


def data_in_exists(app) -> bool:
    """checks whether the data_in file exists for `app` based on its config"""
    return os.path.exists(
        f"./{app.config["UPLOAD_DIR"]}/{app.config["INPUT_FILENAME"]}"
    )


def change_un_pwd(current_secret_key: str, newun: str, newpwd: str) -> None:
    """updates the username and password"""
    os.makedirs("./secrets", exist_ok=True)
    with open("./secrets/admin.txt", "w") as f:
        f.write("\n".join([newun.strip(), newpwd.strip(), current_secret_key.strip()]))


def line_str_hash(row: str) -> str:
    """Hashes a line of text with sha256"""
    return hashlib.sha256(row.encode("utf-8")).hexdigest()


def stream(file) -> Generator[bytes, Any, None]:
    """Return a stream which reads a file in chunks; used for downloading in case files get big"""
    with open(file, "rb") as r:
        while chunk := r.read(8192):
            yield chunk


def exception_format(e: Exception) -> str:  # bruh
    """Gets the stack frame where the exception ACTUALLY occured (deepest frame not in a dependecy)"""
    tb = traceback.extract_tb(e.__traceback__)
    _err = []
    for f in tb:
        if ".venv" not in f:
            _err.append(tb.format_frame_summary(f))
    return f"\n{"".join(_err)}\n{type(e).__name__}: {e}"
