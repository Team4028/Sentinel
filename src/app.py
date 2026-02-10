import time
from flask import Flask, request, render_template, jsonify, Response, send_file, url_for, redirect
from flask_login import login_user, login_required, logout_user, current_user
from flask_wtf import CSRFProtect
from flask_cors import CORS
from flask_wtf.csrf import generate_csrf
try: # janky import stuff: running src/app.py and running scouting_app.py via gunicorn lead to different import paths
    from lib.data_main import Processor
    import lib.mesh as mesh
    from lib.data_config import lex_config
    import apputils
    from auth import BigBrother, LoginForm, require_admin, init_loginm_app
except ModuleNotFoundError:
    from src.lib.data_main import Processor
    import src.lib.mesh as mesh
    from src.lib.data_config import lex_config
    import src.apputils as apputils
    from src.auth import BigBrother, LoginForm, require_admin, init_loginm_app
import os
import json
from threading import Thread
import html
import yaml
import sys
import tempfile
import logging
from pathlib import Path
import argparse
import shlex
import shutil
from jinja2 import Environment, FileSystemLoader

def create_app(): # cursed but whatever
    """ Wraps the flask app in an exportable context so you can load it into the project root dir to make gunicorn happy """
    app = Flask(__name__)
    # setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    # set up cross site request forgery protection because it's one line
    csrf = CSRFProtect(app)
    CORS(app, supports_credentials=True, origins=["https://team4028.github.io", "http://localhost:5173"])
    # load app configs from json file
    app.config.from_file("config/app-config.json", load=json.load)
    POSS_YEARS = [f.stem.split('-', 2)[-1] for f in Path("./config").glob("field-config-*.yaml")]
    admin_login = {}
    config_file = os.path.join("config", f"field-config-{app.config["YEAR"]}.yaml")
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_SECURE'] = False

    notification_queue = []

    # check docker
    is_docker = False
    if os.path.exists("/.dockerenv"):
        is_docker = True
    try:
        with open("/proc/1/cgroup", 'rt') as f:
            for line in f:
                if 'docker' in line:
                    is_docker = True
    except Exception:
        pass
    if os.getenv("container", None) is not None:
        is_docker = True

    GRAFANA_BASE_URL = "http://localhost:3005/d/" if is_docker else "http://localhost:3000/d/" # local testing

    def render_template_style(template, **context):
        """ Renders the input template with the given context and also the accent and text colors of the app """
        return render_template(template, accent=app.config["ACCENT_COLOR"], text=app.config["TEXT_COLOR"], **context)

# =======================================================
# Ensure directories on gitignore are present
# =======================================================
    def ensure_configurable_dirs():
        """ Ensures that the upload and output directories exist.<br>
            It is wrapped in a function because these directories can change at runtime. """
        os.makedirs(app.config["UPLOAD_DIR"], exist_ok=True)
        os.makedirs(app.config["OUT_DIR"], exist_ok=True)
        os.makedirs("secrets", exist_ok=True) # make sure secrets exists because we will soon open some files
    ensure_configurable_dirs()

# =======================================================
# Load Auth Keys
# =======================================================

    admin_login, app.config["SECRET_KEY"], auth_key = apputils.read_secrets()

# =======================================================
# Parse field config and setup processor
# =======================================================
    try:
        processor = Processor(app.config["OUT_DIR"], app.config["CHUNK_SIZE"], *apputils.load_tba_data(app.config["EVENT_KEY"], auth_key, app.config["YEAR"]), lex_config(app.config["YEAR"])) # what's wrong with my copy of python why are their pointers (it's just the unpack operator)
    except Exception as e:
        app.logger.error(f"Error collecting tba data: {apputils.exception_format(e)}") # assume that the load_tba_data is what failed because lex_config has seperate error handling
        processor = Processor(app.config["OUT_DIR"], app.config["CHUNK_SIZE"], None, None, None, None, None, lex_config(app.config["YEAR"]))

    infile = os.path.join(app.config["UPLOAD_DIR"], app.config["INPUT_FILENAME"])
    js = None # load the json file into mem so we don't have to read it every time its requested

    process_queue = []

# =======================================================
# Helper functions
# =======================================================

    # set up the login manager
    init_loginm_app(app)

    def compile_scouting_dashboard():
        """ Uses jinja templating to create dashboard jsons for provisioning that cast all of the number fields to numbers """
        env = Environment(loader=FileSystemLoader("."))
        for path in Path("./src/templates").glob("*.ji"):
            tmpl = env.get_template(path.relative_to('.').as_posix())
            fname = path.name[:-3] # remove .ji
            abbrs = [(title, "".join(w[0].lower() for w in title.split())) for title in processor.config_data["dash-panel"][fname].keys()]
            template_vars = {}
            for abbr in abbrs:
                template_vars |= {abbr[1] + "_headers": processor.config_data["dash-panel"][fname][abbr[0]]}
            os.makedirs("./grafana-dashboard", exist_ok=True)
            out_path = "./grafana-dashboard/" + path.relative_to('.').as_posix().rsplit(".", 2)[0].rsplit("/")[-1] + ".json"
            Path(out_path).write_text(tmpl.render(template_vars))
            if os.name == "posix":
                shutil.copy(out_path, "/var/lib/grafana/dashboards/")

    compile_scouting_dashboard()

    try:
        processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
        app.logger.info("inital processing success")
    except Exception as e:
        app.logger.warning(f"initial processing failed: {apputils.exception_format(e)}")
    
    DASHBOARD_UIDS = {}
    for dash in ["ScoutingDashboard.json", "TeamView.json"]:
        with open(f"/var/lib/grafana/dashboards/{dash}" if is_docker else f"./grafana-dashboard/{dash}", 'r') as r:
            DASHBOARD_UIDS |= {dash: json.load(r)["uid"]}

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
        """ Sends a notification. <br>
            If 'lines' is None, this will send a notification letting the user know that
                new changes are avaliable (restrict append level is 2) with a link to /changes.<br>
            If 'lines' is not None, restrict append level is assumed to be 1 and a new notification that's body has 'lines'
                and a link to remove 'lines' from the input data will be sent."""
        data = {
            "title": "Sentinel" if lines == None else "New Data",
            "body": "New changes avaliable to apply." if lines == None else "\n".join(lines),
            "icon": "/static/favicon.ico",
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
        notification_queue.append((data, time.time() + 300, [])) # gone is the toilsome webpush shenanigens (ik i spelled that wrong)

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
        apputils.safer_replace(temp_path, infile)
        processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
        app.logger.info("Finished processing data.")
        reload_js()

    def append_lines_nofile(lines_to_write: list[str], sending: bool = False):
        """ Appends `lines_to_write` to the input csv and reprocesses the data. <br>
            This will emit a notification if restrict append level is 1 """
        exists = os.path.exists(infile)
        with open(infile, 'a' if exists else 'w', encoding='utf-8') as append:
            lines_to_write[-1] = lines_to_write[-1].strip() # yeet trailing newline
            if len(lines_to_write) > 0:
                if exists: append.write("\n")
                else:
                    append.write(",".join(processor.config_data["headers"]) + "\n")
                append.writelines(lines_to_write)
                if sending:
                    for l in lines_to_write: mesh.send_message(l)
                elif app.config["RESTRICT_APPEND_LEVEL"] == 1:
                    send_change_notification(lines_to_write)
        if not sending:
            processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"])
            app.logger.info("Finished processing data.")
            reload_js()

    def save_photo(filestorage, team: str):
        """ saves the given filestorage to PHOTO_STORAGE/{team}.ext where ext is the extension of filestorage """
        os.makedirs(app.config["PHOTO_STORAGE"], exist_ok=True)
        ext = os.path.splitext(filestorage.filename)[1].lower() # . + type (ex. .png)
        file_save = os.path.join(app.config["PHOTO_STORAGE"], team + ext)
        filestorage.save(file_save)
    
    def handle_mesh_line(message: str):
        """ This function is a consumer for a meshtastic message (line of csv) and will append it to the csv much like /append """
        message = message.strip()
        if message.startswith("@app.cmd"):
            # execute command
            message = message.removeprefix("@app.cmd").strip()
            parser = argparse.ArgumentParser(prog="@app.cmd")
            parser.add_argument("--pwd", type=str, default="")
            parser.add_argument("expr", nargs=argparse.REMAINDER)
            argv = shlex.split(message)
            args = parser.parse_args(argv)
            if args.pwd == admin_login["pwd"]:
                cmd, *rest = args.expr
                rest = ''.join(rest)
                match (cmd):
                    case "rm":
                        Processor.delete_match_team(infile, *rest.split(','))
                # room for more

        elif app.config["RESTRICT_APPEND_LEVEL"] == 2:
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

    @app.before_request
    def log_request():
        """ logs the method and path of the request """
        if not "/notifyq" in request.path: # that spam is annoying
            app.logger.info(f"{request.method} {request.path}")
        else:
            app.logger.info(f"Service worker polled queue")

    @app.errorhandler(Exception)
    def handle_exception(e):
        """ Dampens the app's explosion """
        app.logger.exception(f"Unhandled Exception: {apputils.exception_format(e)}")
        return "Internal server error", 500
    
    @app.errorhandler(404)
    def handle_404(e):
        """ Custom 404 handler """
        app.logger.warning(f"Tried to access nonexistant page: {e}")
        return render_template_style("404.html")
    
    # RESTRICTED (can pop the entire queue by spamming the endpoint)
    @app.route("/notifyq")
    @login_required
    @require_admin
    def notify_q():
        """ pop the queue of notifications for frontend to consume and notify the user """
        if not ("X-Cid" in request.headers) or request.headers.get('X-Cid') == None:
            cid = None
        else: cid = request.headers.get("X-Cid")
        if cid == "null": cid = None
        for n in reversed(notification_queue):
            if n[1] <= time.time():
                notification_queue.remove(n)
            elif not (cid in n[2]) and cid != None:
                n[2].append(cid)
                return jsonify(n[0])
        if cid == None: return "Error, invalid cid", 400
        return "No notifications in queue", 204 # 204 => no content


    # RESTRICTED (can download and edit things and such, though not directly so ig it could be open)
    @app.route("/")
    @login_required
    @require_admin
    def main():
        """ Renders homepage html template, passes the public key to the client to bind the service worker  """
        csv_data = []
        if os.path.exists(infile):
            with open(infile, "r") as r:
                for line in r.readlines():
                    if "MN" not in line:
                        csv_data.append(line.replace("\n", ""))
        return render_template_style("home.html", headers=json.dumps(processor.config_data["headers"]).replace("\uffef", ""), inp_data=json.dumps(csv_data).replace("\\", "\\\\"), graf_url=GRAFANA_BASE_URL.removesuffix("/d/"))
    
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
        return render_template_style("login.html", form=form)
    
    @app.get("/我是谁")
    def whoami():
        if current_user.is_authenticated:
            return jsonify({"logged_in": True, "username": current_user.id})
        return jsonify({"logged_in": False})

    @app.get("/csrf")
    def gen_csrf():
        return jsonify({ "csrf": generate_csrf() })
    
    @app.get("/run_thing")
    def run_thing():
        return app.config["YEAR"]
    
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
        return render_template_style("change.html", append_queue=process_queue)
    
    # OPEN + CORS OPEN (just health, literally returns a string)
    @app.route("/health")
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
                    app.logger.info("Finished processing data.")
                    reload_js()
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500
        
    @app.post("/upload-photo")
    @login_required
    @require_admin
    def upload_photo():
        """ Save a photo for the cooresponding team """
        try:
            if ("photo" in request.files) and (team := request.headers.get("team")) != None:
                photo = request.files["photo"]
                if photo.filename != "":
                    save_photo(photo, team)
                    return "", 200
            return "Error: invalid request", 400
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
            app.logger.info("Finished processing data.")
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
    
    # OPEN (grafana required)
    @app.get("/team-photo")
    def get_team_pics():
        """ Return the uploaded picture cooresponding to the team given in the ?team urlparam """
        if "team" in request.args:
            try:
                files_match = list(Path(app.config["PHOTO_STORAGE"]).glob(f"{request.args.get("team", 0).strip()}.*"))
                files_match = list(map(lambda p: p.absolute().as_posix(), files_match))
                return send_file(files_match[0]) if files_match and len(files_match) > 0 else ("Error, team picture not found", 400)
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Error: invalid request", 400
    
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
    @app.post("/append")
    @login_required
    @require_admin
    def append_lines():
        """ Appends a series of csv lines to the input data based off of the file sent via request.files.<br>
            Complies with the restrict append level. """
        try:
            if request.headers.get("sending", "false") == "false" and app.config["RESTRICT_APPEND_LEVEL"] == 2 and "data" in request.files:
                d_file = request.files["data"]
                # \/ readlines returns a buffer (so use decode) and also use "MN" (which doesn't change from year to year bc theres always matches), to filter out in case its a header
                lines_to_write = [l.decode("utf-8").strip() + "\n" for l in d_file.readlines() if l and (not "MN" in l.decode("utf-8"))]
                for line in lines_to_write:
                    process_queue.append(line)
                    send_change_notification()
            elif "data" in request.files:
                d_file = request.files["data"]
                lines_to_write = [l.decode("utf-8").strip() + "\n" for l in d_file.readlines() if l and (not "MN" in l.decode("utf-8"))] # dodge header if exists
                append_lines_nofile(lines_to_write, request.headers.get("sending", "false") == "true")
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
            process_queue.pop(idx) # rm it from the queue/
            return "", 200
        return "Invalid request", 400
    
    # RESTRICTED (can delete input data = bad)
    @app.post("/delete-lines")
    @login_required
    @require_admin
    def delete_lines():
        """ For restrict append level 1: deletes the already applied append cooresponding to the input json's `lines` field using `rm_row_hash` """
        if "sending" in request.headers and request.headers.get("sending", "false") == "true" and "tn" in request.json and "mn" in request.json:
            mesh.send_command(f"rm {request.json["mn"]},{request.json["tn"]}", admin_login["pwd"])
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
        with open(config_file, "r") as r: # load CONFIG_FILE into mem to pass into jinja2
            content = r.read()
        with open(os.path.join("config", "schema.json"), 'r') as r:
            schema = r.read()
        return render_template_style("edit.html", yaml_content=content, schema=schema)
    
    # RESTRICTED (overwrites field-config = bad)
    @app.post("/save")
    @login_required
    @require_admin
    def save_yaml():
        """ A file consumer that saves the field-config.yaml file during web editing """
        data = html.unescape(request.json.get("code", ""))
        try:
            yaml.safe_load(data)
            with open(config_file, 'w') as f: # save
                f.write(data)
            processor.config_data = lex_config(app.config["YEAR"]) # reload config data
            compile_scouting_dashboard()
            processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"]) # reprocess
            app.logger.info("Finished processing data.")
            reload_js()
            return jsonify({"ok": True, "message": "Saved"})
        except yaml.YAMLError as e:
            return jsonify({"ok": False, "message": str(e)})
        
    # RESTRICTED (technically doesn't write to app config but still bad)
    @app.get("/edit-app-conf")
    @login_required
    @require_admin
    def edit_app_conf_page():
        """ Returns a template for editing the app configuration """
        return render_template_style("appconfig.html", years=POSS_YEARS)

    # RESTRICTED (don't want to share app config because it has secrets)
    @app.get("/get-config")
    @login_required
    @require_admin
    def get_app_config():
        """ Returns the app configuration for the editor at /edit-app-conf to read """
        with open(os.path.join(app.root_path, "config", "app-config.json"), 'r') as r:
            return jsonify(json.load(r))
        return "", 500
    
    # RESTRICTED (obviously don't want to share this)
    @app.get("/tba-key")
    @login_required
    @require_admin
    def get_tba_key():
        """ returns the current TBA api key and whether or not it is good """
        return jsonify({"key": auth_key, "good": apputils.test_tba_key(auth_key)})
    
    @app.get("/test-notification")
    @login_required
    @require_admin
    def test_notification():
        notification_queue.append(({
            "title": "Test",
            "body": "this is a test notification",
            "icon": "/static/favicon.ico"
        }, time.time() + 10, []))
        return "Sent", 200

    # RESTRICTED (may as well, not used by grafana)
    @app.post("/test-tba-key/", defaults={"key": ""})
    @app.post("/test-tba-key/<path:key>")
    @login_required
    @require_admin
    def test_tba_key(key): # necessary because fetching client-side runs into sad caching server-side
        """ tests whether or not the given TBA api key is valid, returning a string 'true' for good and 'false' for bad """
        if apputils.test_tba_key(key):
            return "true", 200
        return "false", 200
    
    @app.post("/set-tba-key")
    @login_required
    @require_admin
    def set_tba_key():
        """ verifies the inputted api key sent via json["key"], applies it and writes it to key.txt, and reprocesses data """
        nonlocal auth_key
        try:
            if request.json and request.json["key"]:
                auth_key = request.json["key"].strip()
                if not apputils.test_tba_key(auth_key): # health check
                    return "Bad TBA key", 400
                apputils.set_auth_key(auth_key)
                processor._teamsAt, processor._sched, processor._ranks, processor._oprs, processor._curr_oprs = apputils.load_tba_data(app.config["EVENT_KEY"], auth_key, app.config["YEAR"])
                return "", 200
            else:
                return "Invalid Request", 400
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (overwrites un/pwd = bad)
    @app.post("/set-admin-creds")
    @login_required
    @require_admin
    def set_admin_creds():
        """ resets the admin username and password to json["un"] and json["pwd"] """
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
    
    @app.get("/calc-team-score")
    @login_required
    @require_admin
    def calc_team_score():
        teams = ["4028"]
        if "pick1" in request.args:
            teams.append(request.args.get('pick1'))
        if "pick2" in request.args:
            teams.append(request.args.get('pick2'))
        if "pick3" in request.args:
            teams.append(request.args.get('pick3'))
        sum = 0
        for team in teams:
            if int(team) in processor._teams:
                sum += processor._teams[int(team)] \
                    .output_dict(
                        processor.config_data,
                        processor._oprs[team],
                        processor._curr_oprs[team]
                    )[
                        processor.config_data["p-metric"]["source"]
                    ]
        return jsonify({"score": round(sum)})

    @app.get("/picklist")
    def picklist():
        return render_template_style("picklist.html", teams=filter(lambda x: x != 4028, sorted([int(x) for x in processor._teamsAt])), dashes=json.dumps(DASHBOARD_UIDS), grafana_base=GRAFANA_BASE_URL)
    
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
                last_year, last_id = app.config["YEAR"], app.config["EVENT_KEY"]
                app.config.from_file("config/app-config.json", load=json.load)
                processor.config_data = lex_config(app.config["YEAR"])
                compile_scouting_dashboard()
                if (app.config["YEAR"] != last_year or app.config["EVENT_KEY"] != last_id):
                    apputils.clear_tba_cache()
                processor._teamsAt, processor._sched, processor._ranks, processor._oprs, processor._curr_oprs = apputils.load_tba_data(app.config["EVENT_KEY"], auth_key, app.config["YEAR"]) # event key may have changed
                if apputils.data_in_exists(app):
                    try:
                        processor.proccess_data(infile, app.config["BASE_OUTPUT_FILENAME"]) # updated processor, so this
                        app.logger.info("Finished processing data.")
                        reload_js()
                    except:
                        os.remove(infile) # remove data_in if it's not playing nice (ie. 2025 data in, switches to 2026)
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