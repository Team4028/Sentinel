import os
import hashlib
from pathlib import Path
import secrets
import shutil
import traceback
from typing import Any, Generator
import base64
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
import asyncio


logger = logging.getLogger(__name__)

def can_cast(x: Any, _type: type) -> bool:
    try:
        _type(x)
        return True
    except:
        return False

def generate_ssl_sign():
    """useless, just use reverse-proxy with nginx for https"""
    domains = ["sentinel.beaksquad.dev", "localhost"]
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Ohio"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Local"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "FRC 4028 The Beak Squad"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Robotics"),
            x509.NameAttribute(NameOID.COMMON_NAME, domains[0]),
        ]
    )

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

    with open(os.path.join("secrets", "sentinel-key.pem"), "wb") as w:
        w.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    with open(os.path.join("secrets", "certinel.pem"), "wb") as w:
        w.write(cert.public_bytes(serialization.Encoding.PEM))


def safer_replace(src, dest) -> None:
    """os.replace dies sometimes"""
    with open(src, "rb") as fsrc, open(dest, "wb") as fdest:
        shutil.copyfileobj(fsrc, fdest)
        fdest.flush()
        os.fsync(fdest.fileno())

    os.remove(src)


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
    with open(os.path.join("config", "schema.json"), "r") as f:
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
        timeout=20,
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

def get_tasks_snapshot(loop=None):
    """
    Returns a snapshot of all asyncio tasks without blocking.
    Can be called from synchronous code.
    """
    loop = loop or asyncio.get_running_loop()
    tasks_info = {}

    for task in asyncio.all_tasks(loop=loop):
        task_info = {
            "done": task.done(),
            "stack": [line.strip() for stack in task.get_stack(limit=1) for line in traceback.format_stack(stack)]
        }
        tasks_info[task.get_name()] = task_info

    return tasks_info


def clear_pictures() -> None:
    shutil.rmtree("photos")
    os.makedirs("photos", exist_ok=True)


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
                timeout=20,
                headers={"X-TBA-Auth-Key": api_key},
            ).json()
            if "oprs" in fetch_oprs:
                for x, y in fetch_oprs["oprs"].items():
                    oprs |= {int(x.removeprefix("frc")): round(float(y), 1)}
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


def get_tba_coprs(event_key, api_key, config_data):
    coprs = {}
    try:
        if tba_health() and not (api_key == None or api_key.strip() == ""):
            logger.info(
                f"Fetch https://www.thebluealliance.com/api/v3/event/{event_key}/coprs"
            )
            coprs = requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{event_key}/coprs",
                timeout=20,
                headers={"X-TBA-Auth-Key": api_key},
            ).json()
            coprs = invert_jeson(coprs)
            for team in list(coprs.keys()):
                coprs[int(team.removeprefix("frc"))] = coprs.pop(team)
            for team in list(coprs.keys()):
                for cop in list(coprs[team].keys()):
                    if cop not in config_data["copr"]:
                        del coprs[team][cop]
                    else:
                        coprs[team][cop] = round(coprs[team][cop], 1)
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
    
        if (
            tba_health()
            and not (api_key == None or api_key.strip() == "")
        ):
            for team in teams:
                opr = 0.0
                logger.info(
                    f"Fetch: https://www.thebluealliance.com/api/v3/team/frc{team}/events/{year}"
                )
                events = requests.get(
                    f"https://www.thebluealliance.com/api/v3/team/frc{team}/events/{year}",
                    timeout=20,
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
                    try:
                        opr = float(
                            requests.get(
                                f"https://www.thebluealliance.com/api/v3/event/{latest_no_event["key"]}/oprs",
                                timeout=20,
                                headers={"X-TBA-Auth-Key": api_key},
                            ).json()["oprs"][f"frc{team}"]
                        )  # get the teams opr from that event
                    except KeyError:
                        opr = 0.0
                oprs |= {int(team): round(opr, 1)}
        else:
            logger.error("Error: no wifi or tba cache or invalid api key")
            return {}
        return oprs
    except Exception as e:
        logger.error(exception_format(e))
        return {}
    
def get_tba_images(api_key, year, photo_dir, teams):
    for team in teams:
        logger.info(
            f"Fetch: https://www.thebluealliance.com/api/v3/team/frc{team}/media/{year}"
        )
        pics = requests.get(
            f"https://www.thebluealliance.com/api/v3/team/frc{team}/media/{year}",
            timeout=20,
            headers={"X-TBA-Auth-Key": api_key},
        ).json()
        for i, pic in enumerate(pics):
            output_image_name = os.path.join(photo_dir, f"{team}-tba-{i}")
            if pic["type"] in ["avatar", "instagram-image"] or os.path.exists(f"{output_image_name}.png") or os.path.exists(f"{output_image_name}.jpeg"): continue
            if "details" in pic and "image_url" in pic["details"]:
                img_src = pic["details"]["image_url"]
                output_image_name += os.path.splitext(img_src)[1]
                try:
                    logger.info(f"Fetch: {img_src}")
                    response = requests.get(img_src, timeout=5, headers={
                        "User-Agent": "curl/7.88.1", # pretend to be curl to avoid 429
                        "Accept": "*/*"
                    }, allow_redirects=False)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    logger.info(f"Error downloading image: {e}")
                    continue
                with open(output_image_name, "wb") as w:
                    for chunk in response.iter_content(chunk_size=8192):
                        w.write(chunk)
                logger.info(f"Downloaded image {output_image_name} from {img_src}")
            elif pic["direct_url"].strip():
                img_src = pic["direct_url"]
                if pic["type"] == "onshape":
                    output_image_name += ".png"
                else:
                    output_image_name += os.path.splitext(img_src)[1]
                try:
                    logger.info(f"Fetch: {img_src}")
                    response = requests.get(img_src, timeout=5, headers={
                        "User-Agent": "curl/7.88.1", # pretend to be curl to avoid 429
                        "Accept": "*/*"
                    }, allow_redirects=False)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    logger.info(f"Error downloading image: {e}")
                    continue
                with open(output_image_name, "wb") as w:
                    for chunk in response.iter_content(chunk_size=8192):
                        w.write(chunk)
                logger.info(f"Downloaded image {output_image_name} from {img_src}")
            elif "details" in pic and "base64Image" in pic["details"]:
                img_src = pic["details"]["base64Image"]
                img_data = base64.b64decode(img_src)
                if "PNG" in img_data.decode(errors="replace"):
                    output_image_name += ".png"
                else:
                    output_image_name += ".jpeg"
                with open(output_image_name, "wb") as w:
                    w.write(img_data)
                logger.info(f"Saved image {output_image_name} from b64 {img_src}")

def get_num_team_pics(team, photo_dir):
    return len(list(
        Path(photo_dir).glob(
            f"{team}*.*"
        )
    ))


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
                timeout=20,
                headers={"X-TBA-Auth-Key": api_key},
            ).json()
        else:
            logger.error("Error: no wifi or tba cache or invalid api key")
            return {}

        return dict(
            map(
                lambda x: (
                    x,
                    next(iter([
                        (t["rank"], t["sort_orders"][0])
                        for t in ranks["rankings"]
                        if t["team_key"] == f"frc{x}"
                    ]), (0, 0.0)),
                ),
                teams,
            )
        )
    except Exception as e:
        logger.error(exception_format(e))
        return {}
    
def get_tba_events(key, year, team):
    logger.info(f"Fetch: https://www.thebluealliance.com/api/v3/team/frc{team}/events/{year}")
    json = requests.get(f"https://www.thebluealliance.com/api/v3/team/frc{team}/events/{year}", timeout=20, headers={
        "X-TBA-Auth-Key": key
    }).json()
    events = []
    if json:
        for event in json:
            events.append({
                "name": event["name"],
                "key": event["key"],
                "city": event["city"],
                "state": event["state_prov"],
                "start": event["start_date"],
                "end": event["end_date"],
                "short": event["short_name"],
                "week": event["week"]
            })
    return events

class TBADataStatic:
    def __init__(
        self,
        teams=[],
        team_info = {},
        schedule={},
        opr={},
    ):
        self.teams = teams
        self.team_info = team_info
        self.schedule = schedule
        self.oprs = opr

class TBADataDynamic:
    def __init__(
        self,
        ranks={},
        copr={},
        curr_oprs={},
    ):
        self.ranks = ranks
        self.oprs = curr_oprs
        self.copr = copr


def load_tba_data_static(event_key, api_key, year, last_opr_disabled) -> TBADataStatic:
    """Loads up the teams and schedule for `event_key` and returns a tuple (teams, schedule)"""
    if (not tba_health()) or (api_key == None or api_key.strip() == ""):
        raise Exception("Error: no wifi or tba cache or invalid api key")
    logger.info(
        f"Fetch: https://www.thebluealliance.com/api/v3/event/{event_key}/teams"
    )
    teamJSON = requests.get(
        f"https://www.thebluealliance.com/api/v3/event/{event_key}/teams",
        timeout=20,
        headers={"X-TBA-Auth-Key": api_key},
    ).json()
    teams = [x["team_number"] for x in teamJSON]
    team_info = {
        x["team_number"]: {
            "Country": x["country"],
            "State": x["state_prov"],
            "City": x["city"],
            "Name": x["nickname"], # x["name"] is used for sponsors
            "School": x["school_name"],
            "RookieYear": x["rookie_year"],
            "PostalCode": x["postal_code"], # ???
            "Website": x["website"]
        } for x in teamJSON
    }
    logger.info(
        f"Fetch: https://www.thebluealliance.com/api/v3/event/{event_key}/matches"
    )
    schedJson = requests.get(
        f"https://www.thebluealliance.com/api/v3/event/{event_key}/matches",
        timeout=20,
        headers={"X-TBA-Auth-Key": api_key},
    ).json()

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
                "r": list(
                    map(
                        lambda team: team.removeprefix("frc"),
                        x["alliances"]["red"]["team_keys"],
                    )
                ),
                "b": list(
                    map(
                        lambda team: team.removeprefix("frc"),
                        x["alliances"]["blue"]["team_keys"],
                    )
                ),
            }
            for x in schedJson
        ],
        key=sched_sorter,
    )

    return TBADataStatic(
        teams,
        team_info,
        schedule,
        get_tba_opr(event_key, api_key, year, teams) if not last_opr_disabled else {},
    )

def load_tba_data_dynamic(event_key, api_key, config_data, teams_list) -> TBADataDynamic:
    return TBADataDynamic(
        get_tba_ranks(event_key, api_key, teams_list),
        get_tba_coprs(event_key, api_key, config_data),
        get_event_team_oprs(event_key, api_key),
    )

def is_iterable(x):
        try:
            iter(x)
            return True
        except TypeError:
            return False


def read_secrets():
    """Reads the different secrets of the repo: admin creds, flask secret key, and tba auth key in that order"""

    if os.path.exists(os.path.join("secrets", "admin.txt")):
        with open(os.path.join("secrets", "admin.txt"), "r") as r:
            key = r.readline().strip()
    else:
        key = secrets.token_hex(32)

    if os.path.exists(os.path.join("secrets", "tba.txt")):
        with open(os.path.join("secrets", "tba.txt"), "r") as f:
            auth_key = f.readline().strip()
            tba_hmac = f.readline().strip()
    else:
        auth_key, tba_hmac = "", ""

    return (key, auth_key, tba_hmac)


def set_auth_key(key: str) -> None:
    """sets the tba key to `key`"""
    hmac_old = ""
    if os.path.exists(os.path.join("secrets", "tba.txt")):
        with open(os.path.join("secrets", "tba.txt"), 'r') as r:
            lines = r.readlines()
            if len(lines) > 1:
                hmac_old = lines[1].strip()
    with open(os.path.join("secrets", "tba.txt"), "w") as w:
        w.write(f"{key}\n{hmac_old}")

def set_tba_whook_key(hmac: str) -> None:
    key_old = ""
    if os.path.exists(os.path.join("secrets", "tba.txt")):
        with open(os.path.join("secrets", "tba.txt"), 'r') as r:
            lines = r.readlines()
            if len(lines) > 0:
                key_old = lines[0].strip()
    with open(os.path.join("secrets", "tba.txt"), 'w') as w:
        w.write(f"{key_old}\n{hmac}")


def data_in_exists() -> bool:
    """checks whether the data_in file exists for `app` based on its config"""
    return os.path.exists(
        os.path.join("datain", "data_in.csv")
    )

def change_un_pwd_admin(current_secret_key: str, newun: str, newpwd: str) -> None:
    """updates the username and password"""
    os.makedirs("secrets", exist_ok=True)
    with open(os.path.join("secrets", "admin.txt"), "w") as f:
        f.write("\n".join([newun.strip(), newpwd.strip(), current_secret_key.strip()]))


def change_un_pwd_viewer(current_secret_key: str, newun: str, newpwd: str) -> None:
    """updates the username and password"""
    os.makedirs("secrets", exist_ok=True)
    with open(os.path.join("secrets", "viewer.txt"), "w") as f:
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
