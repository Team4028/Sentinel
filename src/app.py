from flask import Flask, request, render_template, jsonify, Response, send_file, url_for, abort, redirect
from flask_login import current_user, UserMixin, LoginManager, login_user, login_required
from flask_cors import cross_origin
from flask_wtf import CSRFProtect, FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired
try:
    from lib.data_main import Processor
    import lib.mesh as mesh
    from lib.data_config import lex_config
except ModuleNotFoundError:
    from src.lib.data_main import Processor
    import src.lib.mesh as mesh
    from src.lib.data_config import lex_config
import os
import secrets
import requests
import json
import re
from threading import Thread
import html
import yaml
import traceback
from pywebpush import webpush, WebPushException
from pywebpush import Vapid as Vap
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_public_key
import base64
import sys
import hashlib
import tempfile
import sqlite3
from functools import wraps

def generate_keys():
    """ Generates a pair (pub/priv) of vapid keys for webpush notification, prints them out, and saves them to the ./secrets/vapid-keys.txt dir """
    os.makedirs("secrets", exist_ok=True) # we will write here, make sure it exsists
    v = Vap()
    v.generate_keys()
    public_key_o = load_pem_public_key(v.public_pem())
    pub_bytes = public_key_o.public_bytes(encoding=serialization.Encoding.X962, format=serialization.PublicFormat.UncompressedPoint) # this is apparently right
    pub_b64 = base64.urlsafe_b64encode(pub_bytes).rstrip(b'=').decode('utf-8') # the public key needs to be in B64URL
    private = v.private_pem().decode('utf-8').strip().replace("\n", "").removeprefix("-----BEGIN PRIVATE KEY-----").removesuffix("-----END PRIVATE KEY-----")
    print(f"Public Vapid Key: {pub_b64}")
    print(f"Private Vapid Key: {private}")
    with open("./secrets/vapid-keys.txt", 'w') as w:
        w.writelines([pub_b64 + "\n", private])

def generate_admin():
    os.makedirs("secrets", exist_ok=True)
    un = input("Enter username: ")
    pwd = hashlib.sha256(input("Enter password: ").encode("utf-8"))
    sec = secrets.token_hex(32)
    with open("./secrets/admin.txt", 'w') as f:
        f.write(un + '\n' + pwd + '\n' + sec)


def create_app(): # cursed but whatever
    """ Wraps the flask app in an exportable context so you can load it into the project root dir to make gunicorn happy """
    app = Flask(__name__)
    csrf = CSRFProtect(app)
    # load app configs from json file
    app.config.from_file("config/app-config.json", load=json.load)
    vapid_keys = {}
    admin_login = {}
    CONFIG_FILE = os.path.join("config", "field-config.yaml")

# =======================================================
# Ensure directories on gitignore are present
# =======================================================
    def ensure_configurable_dirs():
        """ Ensures that the upload and output directories exist.<br>
            It is wrapped in a function because these directories can change at runtime. """
        os.makedirs(app.config["UPLOAD_DIR"], exist_ok=True)
        os.makedirs(app.config["OUT_DIR"], exist_ok=True)
    ensure_configurable_dirs()
    os.makedirs("secrets", exist_ok=True) # make sure secrets exists because we will soon open some files

# =======================================================
# Load Auth Keys
# =======================================================

    if (os.path.exists('./secrets/vapid-keys.txt')):
        with open("./secrets/vapid-keys.txt", 'r') as f:
            vapid_keys["public"] = f.readline().strip()
            vapid_keys["private"] = f.readline().strip()
    else: raise Exception("Error: Missing vapid-keys.txt file in secrets") # hmm yes very safe

    if (os.path.exists('./secrets/admin.txt')):
        with open("./secrets/admin.txt", 'r') as r:
            admin_login["un"] = r.readline().strip()
            admin_login["pwd"] = r.readline().strip()
            app.config["SECRET_KEY"] = r.readline().strip()
    else: raise Exception("Error: Missing admin.txt file in secrets")

    if (os.path.exists("./secrets/key.txt")):
        with open("./secrets/key.txt", 'r') as f:
            auth_key = f.read().strip()
    else:
        auth_key = input("Enter TBA Auth key: ")
        if auth_key: # save the one they enter
            with open("./secrets/key.txt", 'w') as w:
                w.write(auth_key)

# =======================================================
# Load TBA Schedule from event key for match predictor
# =======================================================

    teams = [
        x["team_number"]
        for x in requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{app.config["EVENT_KEY"]}/teams",
            headers={"X-TBA-Auth-Key": auth_key},
        ).json()
    ]

    def sched_sorter(match): # sorting function
        key = match["k"].removeprefix(app.config["EVENT_KEY"] + "_")
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
            f"https://www.thebluealliance.com/api/v3/event/{app.config["EVENT_KEY"]}/matches",
            headers={"X-TBA-Auth-Key": auth_key},
        ).json()
    ], key=sched_sorter)

# =======================================================
# Parse field config and setup processor
# =======================================================

    config_data = lex_config()
    processor = Processor(app.config["OUT_DIR"], app.config["CHUNK_SIZE"], teams, schedule, config_data)
    infile = os.path.join(app.config["UPLOAD_DIR"], app.config["INPUT_FILENAME"])
    js = None

    process_queue = []

# =======================================================
# Helper functions
# =======================================================

    def init_db():
        """ Initialize a new sqlite database to store notification subscriptions through runtimes """
        os.makedirs(os.path.dirname(app.config["NOTIFY_SUB_STORAGE"]), exist_ok=True)
        conn = sqlite3.connect(app.config["NOTIFY_SUB_STORAGE"])
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS subs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT UNIQUE,
            p256dh TEXT,
            auth TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        conn.close()

    init_db()

    class BigBrother(UserMixin):
        id = "admin"
        is_admin = True

    class LoginForm(FlaskForm):
        username = StringField("Username", validators=[DataRequired()])
        password = PasswordField("Password", validators=[DataRequired()])
        submit = SubmitField("Log in")

    login_manager = LoginManager()
    login_manager.login_view = "login"
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        if user_id == "admin":
            return BigBrother()
        return None

    def require_admin(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401) # unauthorized
            if not current_user.is_admin:
                abort(403) # forbidden
            return f(*args, **kwargs)
        return decorated

    def exception_format(e: Exception): # bruh
        """Gets the stack frame where the exception ACTUALLY occured (deepest frame not in a dependecy)"""
        tb = traceback.extract_tb(e.__traceback__)
        for i in range(len(tb)):
            if not ".venv" in tb[len(tb) - i - 1].filename: # make all the junk go away
                tb = tb[len(tb) - i - 1]
                break
        return f"Error in {tb.filename}, line {tb.lineno}, in {tb.name}\n" + traceback.format_exception_only(e)[0]

    def stream(file):
        """ Return a stream which reads a file in chunks; used for downloading in case files get big """
        with open(file, 'rb') as r:
            while chunk := r.read(8192):
                yield chunk

    def reload_js():
        """ Updates the copy of the 'other-metrics.json' file in memory (used for '/percent' endpoint) to use the newest file """
        nonlocal js
        if not os.path.exists(os.path.join(app.config["OUT_DIR"], app.config["METRIC_OUTPUT_FILENAME"])):
            js = None
            return
        with open(os.path.join(app.config["OUT_DIR"], app.config["METRIC_OUTPUT_FILENAME"]), "r") as r:
            js = json.load(r) if os.path.exists(os.path.join(app.config["OUT_DIR"], app.config["METRIC_OUTPUT_FILENAME"])) else None
    
    reload_js()

    def line_str_hash(row: str):
        """ Hashes a line of text with sha256 """
        return hashlib.sha256(row.encode("utf-8")).hexdigest()

    def send_change_notification(lines: str | None = None):
        """ Sends a notification to all subscribers. <br>
            If 'lines' is None, this will send a notification letting the user know that
                new changes are avaliable (restrict append level is 2) with a link to /changes.<br>
            If 'lines' is not None, restrict append level is assumed to be 1 and a new notification that's body has 'lines'
                and a link to remove 'lines' from the input data will be sent."""
        data = {
            "title": "Sentinel" if lines == None else "New Data",
            "body": "New changes avaliable to apply." if lines == None else "\n".join(lines),
            "icon": url_for('static', filename='favicon.ico', _external=True),
        }
        data["actions"] = [ # makes a button that invokes the 'goto-changes' action in the service worker
            {
                "action": "goto-changes",
                "title": "View" # label for button
            }
        ] if lines == None else [ # makes a button that invokes the 'goto-changes' action in the service worker
            {
                "action": "remove-change",
                "title": "Remove" # label for button
            }
        ]
        if lines != None:
            data["data"] = {
                "line-hashes": json.dumps([line_str_hash(x) for x in lines])
            }
        con = sqlite3.connect(app.config["NOTIFY_SUB_STORAGE"])
        c = con.cursor()
        c.execute("SELECT id, endpoint, p256dh, auth FROM subs")
        rows = c.fetchall()
        expired_ids = []
        for row in rows:
            sub_id, endp, p256, auth = row # 
            try:
                webpush(subscription_info={"endpoint": endp, "keys": { "p256dh": p256, "auth": auth }},
                        data=json.dumps(data),
                        vapid_private_key=vapid_keys["private"], # PEM form
                        vapid_claims={"sub": "https://beaksquad.dev"}) # <- who sent it
            except WebPushException as we:
                print(we)
                if we.response and we.response.status_code in (404, 410): # sw expired
                    print(f"Subscription {sub_id} expired.")
                    expired_ids.append(sub_id)
                elif we.response and we.response.json():
                    extra = we.response.json()
                    print("Remote replied with a {}:{}, {}",
                        extra.code,
                        extra.errno,
                        extra.message)
        if expired_ids:
            c.executemany("DELETE FROM subs WHERE id = ?", [(i,) for i in expired_ids])
            print(f"Removed {len(expired_ids)} expired subscriptions")
        con.commit()
        con.close()

    def rm_row_hash(hashes):
        """ Deletes rows of data from the input csv by matching their hashes with the ones provided and then reprocesses the data """
        speedy_hashes = set(hashes) # set is O(1) trust
        temp_fd, temp_path = tempfile.mkstemp()
        firstLine = True
        os.close(temp_fd)
        with open(infile, 'r', newline="", encoding="utf-8") as inf, \
             open(temp_path, 'w', newline='', encoding='utf-8') as outf:
            
            for line in inf:
                if line_str_hash(line.strip()) not in speedy_hashes:
                    outf.write(("" if firstLine else "\n") + line.strip())
                    firstLine = False
        os.replace(temp_path, infile)
        processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
        reload_js()

    def append_lines_nofile(lines_to_write: list[str]):
        """ Appends `lines_to_write` to the input csv and reprocesses the data. <br>
            This will emit a notification if restrict append level is 1 """
        exists = os.path.exists(infile)
        with open(infile, 'a' if exists else 'w', encoding='utf-8') as append:
            lines_to_write[-1] = lines_to_write[-1].strip() # yeet tailing newline
            if len(lines_to_write) > 0:
                if exists: append.write("\n")
                append.writelines(lines_to_write)
                if app.config["RESTRICT_APPEND_LEVEL"] == 1:
                    send_change_notification(lines_to_write)
        processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
        reload_js()
    
    def handle_mesh_line(message):
        """ This function is a consumer for a meshtastic message (line of csv) and will append it to the csv much like /append """
        message = message.strip()
        if app.config["RESTRICT_APPEND_LEVEL"] == 2:
            process_queue.append(message)
            send_change_notification()
        else:
            lines = [message]
            append_lines_nofile(lines)
            reload_js()

    def mesh_listen():
        """ Binds `handle_mesh_line` to the meshtastic port """
        mesh.main(handle_mesh_line)

# =======================================================
# Endpoints
# =======================================================

    @app.route("/")
    @login_required
    @require_admin
    def main():
        """ Renders homepage html template, passes the public key to the client to bind the service worker  """
        return render_template("home.html", pubkey=vapid_keys["public"])
    
    @app.route("/login", methods=["GET", "POST"])
    def login():
        form = LoginForm()
        if form.validate_on_submit():
            if form.username.data == admin_login["un"] \
                and hashlib.sha256(form.password.data.encode("utf-8")).hexdigest() == admin_login["pwd"]:
                login_user(BigBrother())
                next_site = request.args.get('next') or '/'
                print(next_site)
                return redirect(next_site)
            else:
                return "Invalid Credentials", 401
        # if request.method == "POST":
        #     if (
        #         request.form["un"] == admin_login["un"]
        #         and hashlib.sha256(request.form["pwd"]) == admin_login["pwd"]
        #     ):
        #         login_user(BigBrother())
        #         return redirect(request.args.get("next") or '/')
        #     abort(401)
        return render_template("login.html", form=form)
    
    @app.get('/service_worker.js')
    def send_sw():
        """ Passthrough for the service worker so that the client can fetch it """
        return send_file("service_worker.js")
    
    @app.route("/changes")
    @login_required
    @require_admin
    def changes():
        """ Renders page listing changes to apply for restrict append level 2 """
        return render_template("change.html", append_queue=process_queue)
    
    @app.post('/subscribe')
    def push_sub():
        """ Consumes subscription data from a service worker and stores it in the db """
        json_data = request.get_json() # comes from the service worker
        sub = json.loads(json_data["subscription_json"])
        endp = sub["endpoint"]
        keys = sub.get("keys", {})
        p256dh = keys.get("p256dh")
        auth = keys.get("auth")
        con = sqlite3.connect(app.config["NOTIFY_SUB_STORAGE"])
        c = con.cursor()
        c.execute("""
            INSERT OR IGNORE INTO subs (endpoint, p256dh, auth)
            VALUES (?, ?, ?)
        """, (endp, p256dh, auth))
        con.commit()
        con.close()
        return "", 200
    
    @app.route("/health")
    @cross_origin(origins="*") # average cors experience
    def health():
        """ Health check, primarily so that QRScout can know its url is correct """
        return "Sentinel is watching", 200

    @app.get("/percent")
    def percent(): # need because grafana infinity can't do local JSON
        """ Rest passthrough for other-metrics.json, named percent because its current data is the percentage of teams scouted. """
        return jsonify(js) if js else ""

    @app.post("/upload")
    @login_required
    @require_admin
    def upload_file():
        """ Overrides the current input data with an uploaded file from the frontend and reprocesses the data """
        try:
            if "data" in request.files:
                d_file = request.files["data"]
                if d_file.filename != "":
                    d_file.save(infile) # saves the file
                    processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
                    reload_js()
            return "", 200
        except Exception as e:
            return exception_format(e), 500
    
    @app.get("/schema.json")
    def send_schema():
        """ Passthrough for the field-config.yaml schema, because I want to pretend it works with monaco """
        f = os.path.join("config", "schema.json")
        return send_file(f)

    @app.route("/reproc")
    @login_required
    @require_admin
    def reprocess():
        """ Reprocesses the data with no additional inputs; for testing or manual csv changes """
        try: 
            processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
            reload_js()
            return "Data reloaded."
        except Exception as e:
            return exception_format(e), 500

    @app.route("/team-meta")
    def tmeta():
        """ Returns the field names (headers) contained in the teams output csv """
        if (os.path.exists(os.path.join(app.config["OUT_DIR"], f"{app.config["BASE_OUTPUT_FILENAME"]}-teams.csv"))):
            with open(os.path.join(app.config["OUT_DIR"], f"{app.config["BASE_OUTPUT_FILENAME"]}-teams.csv")) as r:
                headers = r.readline().strip().split(",") # decode csv into list[str]
                headers.remove("Team")
                return jsonify(headers)
        return ""

    @app.route("/match-meta")
    def mmeta():
        """ Returns the field names (headers) contained in the matches output csv """
        if (os.path.exists(os.path.join(app.config["OUT_DIR"], f"{app.config["BASE_OUTPUT_FILENAME"]}-matches.csv"))):
            with open(os.path.join(app.config["OUT_DIR"], f"{app.config["BASE_OUTPUT_FILENAME"]}-matches.csv")) as r:
                headers = r.readline().strip().split(",")
                headers.remove("Match")
                headers.remove("Team")
                return jsonify(headers)
        return ""

    @app.get("/next-3")
    def n3():
        """ Returns the next app.config["NEXT_N_MATCHES_NUMBER"] (currently 3, hence the name) matches after ?`mkey` that contain ?`team`<br>
            where ?`x` is the url parameter named `x` """
        if not request.args["mkey"]: return "", 400 # bad request
        team = request.args["team"] or -1 # just gets the next 3 matches if team == None
        curr_match = request.args["mkey"]
        foundit = False # whether it has found the curr_match yet
        next_3 = []
        for m in schedule:
            if foundit:
                if len(next_3) >= app.config["NEXT_N_MATCHES_NUMBER"]: # app is compatible for >3 next, but keep endpoint name because i don't want to redo dash
                    break # stop looking once you find all of them
                if team == -1 or (("frc" + team) in m['b']) or (("frc" + team) in m['r']):
                    next_3.append(m['k'])
            elif m['k'] == curr_match:
                foundit = True
        return jsonify(next_3)

    @app.route("/append", methods=["POST"])
    @login_required
    @require_admin
    def append_lines():
        """ Appends a series of csv lines to the input data based off of the file sent via request.files.<br>
            Complies with the restrict append level. """
        try:
            exists = os.path.exists(infile)
            if app.config["RESTRICT_APPEND_LEVEL"] == 2 and "data" in request.files:
                d_file = request.files["data"]
                # \/ readlines returns a buffer (so use decode) and also use "MN" (which doesn't change from year to year bc theres always matches), to filter out in case its a header
                lines_to_write = [l.decode("utf-8").strip() + "\n" for l in d_file.readlines() if l and ((not exists) or (not "MN" in l.decode("utf-8")))]
                for line in lines_to_write:
                    process_queue.append(line)
                    send_change_notification()
            elif "data" in request.files:
                d_file = request.files["data"]
                exists = os.path.exists(infile)
                lines_to_write = [l.decode("utf-8").strip() + "\n" for l in d_file.readlines() if l and ((not exists) or (not "MN" in l.decode("utf-8")))] # dodge header if exists
                append_lines_nofile(lines_to_write)
            return "", 200
        except Exception as e:
            return exception_format(e), 500
        
    @app.post("/apply-change/<int:idx>")
    @login_required
    @require_admin
    def apply_change(idx):
        """ For restrict append level 2: applies the `idx`'th queued append """
        if idx != None:
            item = process_queue.pop(idx) # delete it from the array and return it
            try:
                append_lines_nofile([item]) # pass in as a single-element list
                return "", 200
            except Exception as e:
                return exception_format(e), 500
        return "Invalid request", 400
    
    @app.post("/delete-change/<int:idx>")
    @login_required
    @require_admin
    def delete_change(idx):
        """ For restrict append level 2: drops the `idx`'th queued append """
        if idx != None:
            process_queue.pop(idx) # rm it from the queue
            return "", 200
        return "Invalid request", 400
    
    @app.post("/delete-lines")
    @login_required
    @require_admin
    def delete_lines():
        """ For restrict append level 1: deletes the already applied append cooresponding to the input json's `lines` field using `rm_row_hash` """
        if request.json and request.json["lines"]:
            rm_row_hash(request.json["lines"])
            return "", 200
        return "Invalid request", 400
    
    @app.get("/edit")
    @login_required
    @require_admin
    def edit_yaml():
        """ Renders the editing template for web editing the field-config.yaml file """
        with open(CONFIG_FILE, "r") as r: # load CONFIG_FILE into mem to pass into jinja2
            content = r.read()
        return render_template("edit.html", yaml_content=content)
    
    @app.post("/save")
    @login_required
    @require_admin
    def save_yaml():
        """ A file consumer that saves the field-config.yaml file during web editing """
        data = html.unescape(request.json.get("code", ""))
        try:
            yaml.safe_load(data)
            with open(CONFIG_FILE, 'w') as f: # save
                f.write(data)
            processor.config_data = lex_config() # reload config data
            processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"]) # reprocess
            reload_js()
            return jsonify({"ok": True, "message": "Saved"})
        except yaml.YAMLError as e:
            return jsonify({"ok": False, "message": str(e)}), 400
        
    @app.get("/edit-app-conf")
    @login_required
    @require_admin
    def edit_app_conf_page():
        """ Returns a template for editing the app configuration """
        return render_template("appconfig.html")

    @app.get("/get-config")
    @login_required
    @require_admin
    def get_app_config():
        """ Returns the app configuration for the editor at /edit-app-conf to read """
        with open(os.path.join(app.root_path, "config", "app-config.json"), 'r') as r:
            return jsonify(json.load(r))
        return "", 500
    
    @app.post("/save-app-config")
    @login_required
    @require_admin
    def save_app_config():
        """ Consumes an app configuration json, saves it, and applies it """
        if request and request.json:
            try:
                with open(os.path.join(app.root_path, "config", "app-config.json"), 'w') as w:
                    json.dump(request.json, w, indent=4) # indent=4 auto-formats the json with \t = 4 spaces
                app.config.from_file("config/app-config.json", load=json.load)
                ensure_configurable_dirs() # if either of the the upl/out dirs were changed, they may no longer exist
                return "", 200
            except Exception as e:
                return exception_format(e), 500
        return "Invalid Request", 400

    @app.get("/download/<file>")
    @login_required
    @require_admin
    def dload(file):
        """ Sends a stream using the `stream` helper for the requested file to download it """
        if not file: return "File not found.", 403
        file = os.path.basename(file) # no .. touchy
        file = os.path.join(app.config["OUT_DIR"] if (app.config["BASE_OUTPUT_FILENAME"] in file or
                                                    file == app.config["METRIC_OUTPUT_FILENAME"]) else app.config["UPLOAD_DIR"], file) # get dir based on name
        if not os.path.exists(file): return "File not found.", 403
        return Response(stream(file), mimetype=('text/json' if ".json" in file else 'text/csv'), headers={ # send over a file stream to be handled by the client XHR
            'Content-Disposition': f"attachment; filename={os.path.basename(file)}"
        })
    
    @app.get("/test-mesh")
    @login_required
    @require_admin
    def test_mesh():
        """ Debug endpoint to test the meshtastic listener without a mesh radio """
        m = request.args.get('m') # (m is the message)
        if m:
            mesh.send_mesh_test(m) # debug without a meshtastic
            return "Message sent"
        else:
            return "Use the 'm' url param to specify a test message"

# =======================================================
# Initialize Meshtastic Listener
# =======================================================

    Thread(target=mesh_listen, daemon=True).start() # peak multithreading

    return app

# =======================================================
# Handle running from this file for debug and keygen
# =======================================================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "gen": # python src/app.py gen (argv[0] is src/app.py so check argv[1])
        generate_keys()
    else:
        create_app().run(port=5001, use_reloader=False) # debug run python