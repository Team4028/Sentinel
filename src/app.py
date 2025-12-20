from flask import Flask, request, render_template, jsonify, Response, send_file, url_for, redirect
from flask_login import login_user, login_required, logout_user
from flask_cors import cross_origin
from flask_wtf import CSRFProtect
try:
    from lib.data_main import Processor
    import lib.mesh as mesh
    from lib.data_config import lex_config
except ModuleNotFoundError:
    from src.lib.data_main import Processor
    import src.lib.mesh as mesh
    from src.lib.data_config import lex_config
import os
import json
from threading import Thread
import html
import yaml
from pywebpush import webpush, WebPushException
import sys
import tempfile
import sqlite3
import apputils
from auth import BigBrother, LoginForm, require_admin, init_loginm_app

def create_app(): # cursed but whatever
    """ Wraps the flask app in an exportable context so you can load it into the project root dir to make gunicorn happy """
    app = Flask(__name__)
    csrf = CSRFProtect(app)
    # load app configs from json file
    app.config.from_file("config/app-config.json", load=json.load)
    vapid_keys = {}
    admin_login = {}
    CONFIG_FILE = os.path.join("config", "field-config.yaml")

    def render_template_pass_vapids(template, **context):
        return render_template(template, pubkey=vapid_keys["public"], **context)

# =======================================================
# Initialize sqlite database for storing notification subscriptions
# =======================================================
    
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

    vapid_keys, admin_login, app.config["SECRET_KEY"], auth_key = apputils.read_secrets()

# =======================================================
# Load TBA Schedule from event key for match predictor
# =======================================================

# =======================================================
# Parse field config and setup processor
# =======================================================

    config_data = lex_config()
    processor = Processor(app.config["OUT_DIR"], app.config["CHUNK_SIZE"], *apputils.load_tba_data(app.config["EVENT_KEY"], auth_key), config_data) # what's wrong with my copy of python why are their pointers (its just the unpack operator)
    infile = os.path.join(app.config["UPLOAD_DIR"], app.config["INPUT_FILENAME"])
    js = None # load the json file into mem so we don't have to read it every time its requested

    process_queue = []

# =======================================================
# Helper functions
# =======================================================

    init_loginm_app(app)

    def reload_js():
        """ Updates the copy of the 'other-metrics.json' file in memory (used for '/percent' endpoint) to use the newest file """
        nonlocal js
        if not os.path.exists(os.path.join(app.config["OUT_DIR"], app.config["METRIC_OUTPUT_FILENAME"])):
            js = None
            return
        with open(os.path.join(app.config["OUT_DIR"], app.config["METRIC_OUTPUT_FILENAME"]), "r") as r:
            js = json.load(r) if os.path.exists(os.path.join(app.config["OUT_DIR"], app.config["METRIC_OUTPUT_FILENAME"])) else None
    
    reload_js()

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
                "line-hashes": json.dumps([apputils.line_str_hash(x) for x in lines])
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
                if apputils.line_str_hash(line.strip()) not in speedy_hashes:
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

    # RESTRICTED (can download and edit things and such, though not directly so ig it could be open)
    @app.route("/")
    @login_required
    @require_admin
    def main():
        """ Renders homepage html template, passes the public key to the client to bind the service worker  """
        return render_template_pass_vapids("home.html")
    
    # OPEN (need to log in before you can be logged in)
    @app.route("/login", methods=["GET", "POST"])
    def login():
        form = LoginForm()
        if form.validate_on_submit():
            if form.username.data == admin_login["un"] \
                and form.password.data.strip() == admin_login["pwd"]:
                login_user(BigBrother())
                return "Login Successful", 200
            else:
                return "Invalid Credentials", 401
        return render_template("login.html", form=form)
    
    # PARTIALLY OPEN (just need to be logged in so basically closed, but technically no admin is necessary)
    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        return redirect(url_for('login'))
    
    # OPEN (sends immutable copy, nothing to hide bc open source)
    @app.get('/service_worker.js')
    def send_sw():
        """ Passthrough for the service worker so that the client can fetch it """
        return send_file("service_worker.js")
    
    # RESTRICTED (can edit things and delete data = restrict)
    @app.route("/changes")
    @login_required
    @require_admin
    def changes():
        """ Renders page listing changes to apply for restrict append level 2 """
        return render_template_pass_vapids("change.html", append_queue=process_queue)
    
    # RESTRICTED (edits db = bad)
    @app.post('/subscribe')
    @login_required
    @require_admin
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
    
    # OPEN + CORS OPEN (just health, literally returns a string)
    @app.route("/health")
    @cross_origin(origins="*") # average cors experience
    def health():
        """ Health check, primarily so that QRScout can know its url is correct """
        return "Sentinel is watching", 200

    # OPEN (immutable passthrough for other-metrics = fine)
    @app.get("/percent")
    def percent(): # need because grafana infinity can't do local JSON
        """ Rest passthrough for other-metrics.json, named percent because its current data is the percentage of teams scouted. """
        return jsonify(js) if js else ""

    # RESTRICTED (completely wipes out input data = bad)
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
            return apputils.exception_format(e), 500
    
    # OPEN (immutable passthrough, literally a yaml schema)
    @app.get("/schema.json")
    def send_schema():
        """ Passthrough for the field-config.yaml schema, because I want to pretend it works with monaco """
        f = os.path.join("config", "schema.json")
        return send_file(f)

    # RESTRICTED (not as bad, but still requires decent processing power)
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
            return apputils.exception_format(e), 500

    # OPEN (grafana need this)
    @app.route("/team-meta")
    def tmeta():
        """ Returns the field names (headers) contained in the teams output csv """
        if (os.path.exists(os.path.join(app.config["OUT_DIR"], f"{app.config["BASE_OUTPUT_FILENAME"]}-teams.csv"))):
            with open(os.path.join(app.config["OUT_DIR"], f"{app.config["BASE_OUTPUT_FILENAME"]}-teams.csv")) as r:
                headers = r.readline().strip().split(",") # decode csv into list[str]
                headers.remove("Team")
                return jsonify(headers)
        return ""

    # OPEN (grafana needs this)
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

    # OPEN (grafana needs this)
    @app.get("/next-3")
    def n3():
        """ Returns the next app.config["NEXT_N_MATCHES_NUMBER"] (currently 3, hence the name) matches after ?`mkey` that contain ?`team`<br>
            where ?`x` is the url parameter named `x` """
        if not request.args["mkey"]: return "", 400 # bad request
        team = request.args["team"] or -1 # just gets the next 3 matches if team == None
        curr_match = request.args["mkey"]
        foundit = False # whether it has found the curr_match yet
        next_3 = []
        for m in processor._sched:
            if foundit:
                if len(next_3) >= app.config["NEXT_N_MATCHES_NUMBER"]: # app is compatible for >3 next, but keep endpoint name because i don't want to redo dash
                    break # stop looking once you find all of them
                if team == -1 or (("frc" + team) in m['b']) or (("frc" + team) in m['r']):
                    next_3.append(m['k'])
            elif m['k'] == curr_match:
                foundit = True
        return jsonify(next_3)

    # RESTRICTED (edits input data = bad)
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
            return apputils.exception_format(e), 500
        
    # RESTRICTED (edits input data = bad)
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
                return apputils.exception_format(e), 500
        return "Invalid request", 400
    
    # RESTRICTED (can delete queued data = bad)
    @app.post("/delete-change/<int:idx>")
    @login_required
    @require_admin
    def delete_change(idx):
        """ For restrict append level 2: drops the `idx`'th queued append """
        if idx != None:
            process_queue.pop(idx) # rm it from the queue
            return "", 200
        return "Invalid request", 400
    
    # RESTRICTED (can delete input data = bad)
    @app.post("/delete-lines")
    @login_required
    @require_admin
    def delete_lines():
        """ For restrict append level 1: deletes the already applied append cooresponding to the input json's `lines` field using `rm_row_hash` """
        if request.json and request.json["lines"]:
            rm_row_hash(request.json["lines"])
            return "", 200
        return "Invalid request", 400
    
    # RESTRICTED (can edit field-config = bad (technically not but still))
    @app.get("/edit")
    @login_required
    @require_admin
    def edit_yaml():
        """ Renders the editing template for web editing the field-config.yaml file """
        with open(CONFIG_FILE, "r") as r: # load CONFIG_FILE into mem to pass into jinja2
            content = r.read()
        return render_template_pass_vapids("edit.html", yaml_content=content)
    
    # RESTRICTED (overwrites field-config = bad)
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
        
    # RESTRICTED (technically doesn't write to app config but still bad)
    @app.get("/edit-app-conf")
    @login_required
    @require_admin
    def edit_app_conf_page():
        """ Returns a template for editing the app configuration """
        return render_template_pass_vapids("appconfig.html")

    # RESTRICTED (don't want to share app config because it has secrets)
    @app.get("/get-config")
    @login_required
    @require_admin
    def get_app_config():
        """ Returns the app configuration for the editor at /edit-app-conf to read """
        with open(os.path.join(app.root_path, "config", "app-config.json"), 'r') as r:
            return jsonify(json.load(r))
        return "", 500

    # RESTRICTED (overwrites un/pwd = bad)
    @app.post("/set-admin-creds")
    def set_admin_creds():
        nonlocal admin_login
        if request and request.json:
            try:
                un = request.json.get('un', admin_login["un"])
                pwd = apputils.line_str_hash(request.json["pwd"]) if "pwd" in request.json else admin_login["pwd"]
                apputils.change_un_pwd(app.config["SECRET_KEY"], un, pwd)
                admin_login = {
                    "un": un,
                    "pwd": pwd
                }
                return "Password change successful", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid Request", 400
    
    # RESTRICTED (overwrites app config = bad)
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
                processor._teamsAt, processor._sched = apputils.load_tba_data(app.config["EVENT_KEY"], auth_key) # event key may have changed
                processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"]) # upgated processor, so this
                ensure_configurable_dirs() # if either of the the upl/out dirs were changed, they may no longer exist
                return "", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid Request", 400

    # RESTRICTED (save bandwidth)
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
        return Response(apputils.stream(file), mimetype=('text/json' if ".json" in file else 'text/csv'), headers={ # send over a file stream to be handled by the client XHR
            'Content-Disposition': f"attachment; filename={os.path.basename(file)}"
        })
    
    # RESTRICTED (edits input data = bad)
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
        apputils.generate_keys()
    elif len(sys.argv) > 1 and sys.argv[1] == "pwd":
        apputils.generate_admin()
    else:
        create_app().run(port=5001, use_reloader=False) # debug run python