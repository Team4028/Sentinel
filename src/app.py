import time
from flask import (
    Flask,
    request,
    render_template,
    jsonify,
    Response,
    send_file,
    url_for,
    redirect,
)
from flask_login import login_user, login_required, logout_user, current_user
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS

try:  # janky import stuff: running src/app.py and running scouting_app.py via gunicorn lead to different import paths
    from lib.data_main import Processor, Event
    import lib.mesh as mesh
    from lib.data_config import lex_config
    import apputils
    from auth import BigBrother, Winston, LoginForm, require_admin, init_loginm_app, require_json
except ModuleNotFoundError:
    from src.lib.data_main import Processor, Event
    import src.lib.mesh as mesh
    from src.lib.data_config import lex_config
    import src.apputils as apputils
    from src.auth import BigBrother, Winston, LoginForm, require_admin, init_loginm_app, require_json
import csv
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
import asyncio


def create_app(inital_process = True, skip_last_opr_fetching_for_testing_because_its_slow = False):  # cursed but whatever
    """Wraps the flask app in an exportable context so you can load it into the project root dir to make gunicorn happy"""

    loop = asyncio.new_event_loop()

    SUPERUSER_CODE = os.getenv("SU_CODE")

    if not SUPERUSER_CODE or SUPERUSER_CODE == "":
        print("Error, su code not specified.")
        sys.exit(1)

    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    # setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    CORS(
        app,
        supports_credentials=True,
        origins=["https://team4028.github.io", "http://localhost:5173"],  # testing
    )
    # load app configs from json file
    app.config.from_file("config/app-config.json", load=json.load)
    app.logger.info(f"Key: {SUPERUSER_CODE}")
    # match the x in field-config-x.yaml to get the different configs
    POSS_YEARS = [
        f.stem.split("-", 2)[-1] for f in Path("./config").glob("field-config-*.yaml")
    ]
    admin_login = {}
    viewer_login = {}
    config_file = os.path.join("config", f"field-config-{app.config["YEAR"]}.yaml")
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = False

    notification_queue = []

    def start_loop(loop: asyncio.AbstractEventLoop):
        app.logger.info("Starting app async thread")
        asyncio.set_event_loop(loop)
        loop.run_forever()
        loop.close()

    def run_async_task(coro):
        app.logger.info(f"Async thread add coroutine: {coro.__name__}")
        return asyncio.run_coroutine_threadsafe(coro, loop)
    
    # check docker
    is_docker = False
    if os.path.exists("/.dockerenv"):
        is_docker = True
    try:
        with open("/proc/1/cgroup", "rt") as f:
            for line in f:
                if "docker" in line:
                    is_docker = True
    except Exception:
        pass
    if os.getenv("container", None) is not None:
        is_docker = True

    def render_template_style(template, **context):
        """Renders the input template with the given context and also the accent and text colors of the app"""
        return render_template(
            template,
            accent=app.config["ACCENT_COLOR"],
            text=app.config["TEXT_COLOR"],
            **context,
        )

    async def do_data_processing():
        app.logger.info("Starting data proceessing...")
        if not processor.has_sched_data:
            app.logger.warning("Processing aborted: processor does not have valid data")
            return
        await processor.proccess_data(infile)
        app.logger.info("Finished processing data.")
        reload_js()
        return
        

    # =======================================================
    # Ensure directories on gitignore are present
    # =======================================================
    def ensure_untracked_dirs():
        """Ensures that the upload and output directories exist.<br>
        It is wrapped in a function because these directories can change at runtime."""
        os.makedirs("datain", exist_ok=True)
        os.makedirs("dataout", exist_ok=True)
        os.makedirs("photos", exist_ok=True)
        os.makedirs("autos", exist_ok=True)
        os.makedirs(
            "secrets", exist_ok=True
        )  # make sure secrets exists because we will soon open some files

    ensure_untracked_dirs()

    # =======================================================
    # Load Auth Keys
    # =======================================================

    admin_login, viewer_login, app.config["SECRET_KEY"], auth_key, tba_hmac = apputils.read_secrets()

    # =======================================================
    # Parse field config and setup processor
    # =======================================================
    processor = Processor(
        skip_last_opr_fetching_for_testing_because_its_slow,
        app.config["TBA_FETCH_PERIOD_MIN"],
        auth_key,
        app.config["YEAR"].split("_")[0] if "_" in app.config["YEAR"] else app.config["YEAR"],
        lex_config(app.config["YEAR"]),
    )  # what's wrong with my copy of python why are their pointers (it's just the unpack operator)

    infile = os.path.join("datain", "data_in.csv")
    js = None  # load the json file into mem so we don't have to read it every time its requested

    process_queue = []

    # =======================================================
    # Helper functions
    # =======================================================

    # set up the login manager
    init_loginm_app(app)

    def compile_scouting_dashboard(url: str):
        """Uses jinja templating to create dashboard jsons for provisioning that cast all of the number fields to numbers"""
        app.logger.info("Compiling dashboards...")
        env = Environment(loader=FileSystemLoader("."))
        for path in Path("./src/templates").glob("*.ji"):
            app.logger.info(f"Compiling {path}...")
            tmpl = env.get_template(path.relative_to(".").as_posix())
            fname = path.name[:-3]  # remove .ji
            # make acronym from title
            abbrs = [
                (title, "".join(w[0].lower() for w in title.split()))
                for title in processor.config_data["dash-panel"][fname].keys()
            ]
            template_vars = {
                "sentinel_url": url,
                "grafana_url": app.config["GRAFANA_URL"],
                "event_prefix": processor.event_key + "_",
            }
            for abbr in abbrs:
                template_vars |= {
                    abbr[1]
                    + "_headers": processor.config_data["dash-panel"][fname][abbr[0]]
                }
            os.makedirs("./grafana-dashboard", exist_ok=True)
            out_path = (
                "./grafana-dashboard/"
                + path.relative_to(".").as_posix().rsplit(".", 2)[0].rsplit("/")[-1]
                + ".json"
            )
            Path(out_path).write_text(tmpl.render(template_vars))
            if os.name == "posix":
                # inject the new dash to the provisioning dir
                shutil.copy(out_path, "/var/lib/grafana/dashboards/")

    def get_fa_icon(name: str):
        ext = name.lower().split(".")[-1]

        return {
            "csv": "fa-file-excel-o",
            "png": "fa-file-image-o",
            "jpg": "fa-file-image-o",
            "jpeg": "fa-file-image-o",
            "ico": "fa-file-image-o",
            "py": "fa-code",
            "js": "fa-code",
            "html": "fa-code",
            "css": "fa-code",
            "json": "fa-code",
            "ji": "fa-code",
            "css": "fa-code",
            "env": "fa-gear",
            "txt": "fa-file-text-o",
            "md": "fa-file-text-o",
            "zip": "fa-file-archive-o",
            "gitattributes": "fa-git",
            "gitignore": "fa-git",
            "bat": "fa-windows",
            "sh": "fa-dollar",
        }.get(ext, "fa-file-o")
    
    app.jinja_env.globals.update(get_fa_icon=get_fa_icon)

    if inital_process:
        try:
            run_async_task(do_data_processing()).add_done_callback(lambda _: app.logger.info("initial processing finished."))
        except Exception as e:
            app.logger.warning(f"initial processing failed: {apputils.exception_format(e)}")

    DASHBOARD_UIDS = {}
    for dash in ["Prematch.json", "Full Team Data.json", "Statbotics Viz.json", "Team View.json", "Pit Scouting View.json"]: # TODO: make this a grep
        if not os.path.exists(
            f"/var/lib/grafana/dashboards/{dash}"
                if is_docker
                else f"./grafana-dashboard/{dash}"
        ):
            compile_scouting_dashboard("http://localhost:5000")
        with open(
            (
                f"/var/lib/grafana/dashboards/{dash}"
                if is_docker
                else f"./grafana-dashboard/{dash}"
            ),
            "r",
        ) as r:
            DASHBOARD_UIDS |= {dash: json.load(r)["uid"]}

    def reload_js():
        """Updates the copy of the 'other-metrics.json' file in memory (used for '/percent' endpoint) to use the newest file"""
        nonlocal js
        if not os.path.exists(
            os.path.join("dataout", "other-metrics.json")
        ):
            js = None
            return
        with open(
            os.path.join("dataout", "other-metrics.json"),
            "r",
        ) as r:
            js = (
                json.load(r)
                if os.path.exists(
                    os.path.join(
                        "dataout", "other-metrics.json"
                    )
                )
                else None
            )

    reload_js()

    def send_generic_notification(data: dict):
        notification_queue.append((data | { "icon": '/static/favicon.ico' }, time.time() + 300, []))

    def send_change_notification(lines: str | None = None):
        """Sends a notification. <br>
        If 'lines' is None, this will send a notification letting the user know that
            new changes are avaliable (restrict append level is 2) with a link to /changes.<br>
        If 'lines' is not None, restrict append level is assumed to be 1 and a new notification that's body has 'lines'
            and a link to remove 'lines' from the input data will be sent."""
        data = {
            "title": "Sentinel" if lines == None else "New Data",
            "body": (
                "New changes avaliable to apply." if lines == None else "\n".join(lines)
            ),
            "icon": '/static/favicon.ico',
        }
        data["actions"] = (
            [  # makes a button that invokes the 'goto-changes' action in the service worker (active mod)
                {"action": "goto-changes", "title": "View"}  # label for button
            ]
            if lines == None
            else [  # makes a button that invokes the 'remove-change' action in the service worker (passive mod)
                {"action": "remove-change", "title": "Remove"}  # label for button
            ]
        )
        if lines != None:
            data["data"] = {
                "line-hashes": json.dumps([apputils.line_str_hash(x) for x in lines])
            }
        notification_queue.append(
            (data, time.time() + 300, [])
        )  # gone is the toilsome webpush shenanigens (ik i spelled that wrong)


    def handle_tba_webhook(notification_json):
        if not "message_type" in notification_json or not "message_data" in notification_json:
            return
        match (notification_json["message_type"]):
            case "match_score":
                if processor.event_key and notification_json["message_data"]["event_key"] == processor.event_key:
                    run_async_task(processor.perform_periodic_calls())
                return
            case "schedule_updated":
                if processor.event_key and notification_json["message_data"]["event_key"] == processor.event_key:
                    event = processor.event_key
                    run_async_task(processor.clear_database())
                    processor.event_key = event
                    with open("last_loaded_event_key.txt", 'w') as w:
                        w.write(event)
                    run_async_task(processor.load_event_data())
                    run_async_task(processor.perform_periodic_calls())
                    return
            case "ping":
                md = notification_json["message_data"]
                app.logger.info(f"TBA pinged server:\nTitle: {md["title"]}\nDesc: {md["desc"]}")
                send_generic_notification({
                    "title": f"TBA Ping: {md["title"]}",
                    "body": md["desc"]
                })
                return
            case "broadcast":
                md = notification_json["message_data"]
                app.logger.info(f"TBA announcement:\nTitle: {notification_json["message_data"]["title"]}\nDesc: {notification_json["message_data"]["desc"]}\nUrl: {notification_json["message_data"]["url"] if "url" in notification_json["message_data"] else "None"}")
                send_generic_notification({
                    "title": f"TBA Broadcast: {md["title"]}",
                    "body": md["desc"] + (md["url"] if "url" in md else "")
                })
            case "verification":
                app.logger.info(f"TBA webhook verification recieved")
                send_generic_notification({
                    "title": "TBA Webhook verification",
                    "body": notification_json["message_data"]["verification_key"]
                })
                return
        return
            
    def rm_row_hash(hashes):
        """Deletes rows of data from the input csv by matching their hashes with the ones provided and then reprocesses the data"""
        hasty_hashes = set(hashes)  # set is O(1) trust
        temp_fd, temp_path = tempfile.mkstemp()
        firstLine = True
        os.close(temp_fd)
        with (
            open(infile, "r", newline="", encoding="utf-8") as inf,
            open(temp_path, "w", newline="", encoding="utf-8") as outf,
        ):

            for line in inf:
                if apputils.line_str_hash(line.strip()) not in hasty_hashes:
                    outf.write(("" if firstLine else "\n") + line.strip())
                    firstLine = False
        apputils.safer_replace(temp_path, infile)
        run_async_task(do_data_processing())

    def append_lines_nofile(lines_to_write: list[str], sending: bool = False):
        """Appends `lines_to_write` to the input csv and reprocesses the data. <br>
        This will emit a notification if restrict append level is 1"""
        exists = os.path.exists(infile)
        with open(infile, "a" if exists else "w", encoding="utf-8") as append:
            if len(lines_to_write) > 0:
                lines_to_write[-1] = lines_to_write[-1].strip()  # yeet trailing newline
                if exists:
                    append.write("\n")
                else:
                    append.write(",".join(processor.config_data["headers"]) + "\n")
                append.writelines(lines_to_write)
                if sending:
                    for l in lines_to_write:
                        mesh.send_message(l)
                elif app.config["RESTRICT_APPEND_LEVEL"] == 1:
                    app.logger.info(f"Adding notification for data {lines_to_write}")
                    send_change_notification(lines_to_write)
        if not sending or mesh.get_is_meshed():
            run_async_task(do_data_processing())

    def save_photo(filestorage, team: str):
        """saves the given filestorage to PHOTO_STORAGE/{team}.ext where ext is the extension of filestorage"""
        ext = os.path.splitext(filestorage.filename)[1].lower()  # . + type (ex: .png)
        file_save = os.path.join("photos", team + ext)
        filestorage.save(file_save)

    def save_auto(filestorage, match: str):
        ext = os.path.splitext(filestorage.filename)[1].lower()
        file_save = os.path.join("autos", f"match-{match}{ext}")
        filestorage.save(file_save)

    def handle_mesh_line(message: str):
        """This function is a consumer for a meshtastic message (line of csv) and will append it to the csv much like /append"""
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
                rest = "".join(rest)
                match (cmd):
                    case "rm":
                        processor.delete_match_scouter(infile, *rest.split(","))
                # room for more

        elif app.config["RESTRICT_APPEND_LEVEL"] == 2:
            process_queue.append(message)
            send_change_notification()
        else:
            lines = [message]
            append_lines_nofile(lines)
            reload_js() # TODO: is this call redundant?

    def mesh_listen():
        """Binds `handle_mesh_line` to the meshtastic port (listens for meshages)"""
        mesh.main(handle_mesh_line)

    # =======================================================
    # Endpoints
    # =======================================================

    @app.before_request
    def log_request():
        """logs the method and path of the request"""
        if not "/notifyq" in request.path and not '/jobs' in request.path:  # that spam is annoying
            app.logger.info(f"{request.method} {request.path}")
        elif "/notifyq" in request.path:
            app.logger.info(f"Service worker polled queue")


    @app.errorhandler(Exception)
    def handle_exception(e):
        """Dampens the app's explosion"""
        app.logger.exception(f"Unhandled Exception: {apputils.exception_format(e)}")
        return f"Internal server error: {apputils.exception_format(e)}", 500

    @app.errorhandler(401)
    def handle_401(e):
        app.logger.warning(f"Tried to access page without login: {e.description}")
        return f"Error: unauthorized", 401
    
    @app.errorhandler(403)
    def handle_403(e):
        app.logger.warning(f"Tried to access restricted page: {e.description}")
        return f"Error: restricted", 403

    @app.errorhandler(404)
    def handle_404(e):
        """Custom 404 handler"""
        app.logger.warning(f"Tried to access nonexistent page: {e}")
        return render_template_style("404.html")

    # RESTRICTED (can pop the entire queue by spamming the endpoint)
    @app.route("/notifyq")
    @login_required
    @require_admin
    def notify_q():
        """pop the queue of notifications for frontend to consume and notify the user"""
        if not ("X-Cid" in request.headers) or request.headers.get("X-Cid") == None:
            cid = None
        else:
            cid = request.headers.get("X-Cid")
        if cid == "null":
            cid = None
        for n in reversed(notification_queue):
            if n[1] <= time.time():
                notification_queue.remove(n)
            elif not (cid in n[2]) and cid != None:
                n[2].append(cid)
                return jsonify(n[0])
        if cid == None:
            return "Error, invalid cid", 400
        return "No notifications in queue", 204  # 204 => no content

    # RESTRICTED
    @app.get("/")
    @login_required
    def main():
        """Renders homepage html template, passes the public key to the client to bind the service worker"""
        csv_data = []
        if os.path.exists(infile):
            with open(infile, "r") as r:
                for line in r.readlines():
                    if "MN" not in line:
                        csv_data.append(line.replace("\n", ""))
        return render_template_style(
            "home.html",
            headers=json.dumps(processor.config_data["headers"]).replace("\uffef", ""),
            inp_data=json.dumps(csv_data).replace("\\", "\\\\"),
            graf_url=app.config["GRAFANA_URL"],
        )

    # OPEN (need to log in before you can be logged in)
    @app.route("/login", methods=["GET", "POST"])
    def login():
        """Sends login page and validates login un/pw"""
        form = LoginForm()
        if form.validate_on_submit():
            username = form.username.data
            password = form.password.data.strip()
            if (
                username == admin_login["un"]
                and password == admin_login["pwd"]
            ):
                login_user(BigBrother())
                return "Login Successful", 200
            elif (
                username == viewer_login["un"]
                and password == viewer_login["pwd"]
            ):
                login_user(Winston())
                return "Login Successful", 200
            else:
                return "Invalid Credentials", 401
        return render_template_style("login.html", form=form)
    
    @app.post('/login-ovr')
    @require_json
    def login_override():
        if request.json and "key" in request.json:
            if request.json["key"] == SUPERUSER_CODE:
                login_user(BigBrother())
                return "Login Override Successful", 200
            else:
                return "Invalid credentials", 401
        return "No key", 401
    
    @app.get("/explore")
    @login_required
    @require_admin
    def explore():
        def build_tree(path):
            tree = []
            try:
                for item in sorted(os.listdir(path)):
                    full_path = os.path.join(path, item)
                    if os.path.isdir(full_path):
                        tree.append({
                            "type": "folder",
                            "name": item,
                            "children": build_tree(full_path)
                        })
                    else:
                        tree.append({
                            "type": "file",
                            "name": item
                        })
            except PermissionError:
                pass
            
            tree.sort(key=lambda x: (
                0 if x["type"] == "folder" else 1,
                x["name"].lower()
            ))

            return tree
        return render_template_style("file-explorer.html", tree=build_tree("."))
    
    @app.get("/edit-file")
    @login_required
    @require_admin
    def edit_file():
        if request.args and "filepath" in request.args:
            filepath = request.args.get("filepath")
            filepath =  html.unescape(filepath)
            if os.path.exists(filepath) and os.path.isfile(filepath):
                with open(filepath, 'r', encoding='utf-8') as r:
                    content = r.read()
                return render_template_style(
                    "edit-file.html",
                    file_path=filepath,
                    file_name=os.path.basename(filepath),
                    file_content=content
                )
            else:
                return "Error, path does not exist"
        else:
            return "Invalid Request", 400
    
    @app.get('/jobs')
    def jobs():
        return jsonify(Event.get_event_progress())

    @app.get("/我是谁")
    def whoami():
        if current_user.is_authenticated:
            return jsonify({"logged_in": True, "username": current_user.id, "admin": current_user.is_admin})
        return jsonify({"logged_in": False})

    # PARTIALLY OPEN (just need to be logged in so basically closed, but technically no admin is necessary)
    @app.get("/logout")
    @login_required
    def logout():
        logout_user()
        return redirect(url_for("login"))

    # OPEN (sends immutable copy, nothing to hide bc open source)
    @app.get("/service_worker.js")
    def send_sw():
        """Passthrough for the service worker so that the client can fetch it"""
        return send_file("service_worker.js")
    
    @app.get('/pit')
    @login_required
    def pit_scout():
        return render_template_style('pit-scouting.html')
    
    @app.get('/auton-simple')
    def auto_scout_simple():
        return render_template_style('auton-scouting-simple.html')
    
    @app.post('/submit-pit')
    @login_required
    @require_json
    def save_pit():
        if request and request.json:
            try:
                csv_file = os.path.join("dataout", "output.csv" + "-pit-scouting.csv")
                need_write = not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0 
                with open(os.path.join("dataout", "output.csv" + "-pit-scouting.csv"), mode='a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=request.json.keys())
                    if need_write: writer.writeheader()
                    writer.writerow(request.json)
                return "Success", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Error: invalid request", 400

    @app.post('/submit-auto-simple')
    @require_json
    def save_auto_simple():
        if request and request.json:
            try:
                csv_file = os.path.join("dataout", "output.csv" + "-auton-scouting.csv")
                need_write = not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0
                js: dict = request.json
                was_pre = js.pop("wasPre") if "wasPre" in js else False
                if os.path.exists(csv_file):
                    with open(csv_file, mode='r', newline='') as r:
                        matches_scouted = len(list(filter(lambda s: (f"{js["tn"]}" in s.strip()) and ((not was_pre) ^ ("Pre" in s.strip())), r.readlines())))
                else:
                    matches_scouted = 0
                js = { "Match": f"{"Pre " if was_pre else ""}{matches_scouted + 1}" } | js
                with open(csv_file, mode='a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=js.keys())
                    if need_write: writer.writeheader()
                    writer.writerow(js)
                return "Success", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Error: invalid request", 400

    # RESTRICTED (can edit things and delete data = restrict)
    @app.get("/changes")
    @login_required
    @require_admin
    def changes():
        """Renders page listing changes to apply for restrict append level 2"""
        return render_template_style("change.html", append_queue=process_queue)

    # OPEN + CORS OPEN (just health, literally returns a string)
    @app.get("/health")
    def health():
        """Health check, primarily so that QRScout can know its url is correct"""
        if not processor.has_sched_data:
            return "No event data", 200
        return "Sentinel is watching", 200

    # OPEN (immutable passthrough for other-metrics = fine)
    @app.get("/percent")
    def percent():  # need because grafana infinity can't do local JSON
        """Rest passthrough for other-metrics.json, named percent because its current data is the percentage of teams scouted."""
        return jsonify(js) if js else ""

    # RESTRICTED (completely wipes out input data = bad)
    @app.post("/upload")
    @login_required
    @require_admin
    def upload_file():
        """Overrides the current input data with an uploaded file from the frontend and reprocesses the data"""
        try:
            if "data" in request.files:
                d_file = request.files["data"]
                if d_file.filename != "":
                    d_file.save(infile)  # saves the file
                    run_async_task(do_data_processing())
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500
        
    @app.post("/upload-alt")
    @login_required
    @require_admin
    def upload_other_files():
        try:
            if "data" in request.files and "name" in request.headers:
                d_file = request.files["data"]
                if d_file.filename != "" and request.headers.get("name", "").strip() != "":
                    d_file.save(os.path.join("dataout", request.headers.get("name")))
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500
        
    @app.post("/upload-auto")
    @login_required
    def upload_auto():
        try:
            if ("photo" in request.files) and (
                match := request.headers.get("mkey")
            ) != None:
                photo = request.files["photo"]
                if photo.filename != "":
                    save_auto(photo, match)
                    return "Saved!", 200
            return "Error: invalid request", 400
        except Exception as e:
            return apputils.exception_format(e), 500
                

    # RESTRICTED (uploads files = writes to server directory = bad)
    @app.post("/upload-photo")
    @login_required
    def upload_photo():
        """Save a photo for the cooresponding team"""
        try:
            if ("photo" in request.files) and (
                team := request.headers.get("team")
            ) != None:
                photo = request.files["photo"]
                if photo.filename != "":
                    save_photo(photo, team)
                    return "Saved!", 200
            return "Error: invalid request", 400
        except Exception as e:
            return apputils.exception_format(e), 500

    # OPEN (immutable passthrough, literally a yaml schema)
    @app.get("/schema.json")
    def send_schema():
        """Passthrough for the field-config.yaml schema, because I want to pretend it works with monaco"""
        f = os.path.join("config", "schema.json")
        return send_file(f)

    # RESTRICTED (not as bad, but still requires decent processing power)
    @app.post("/reproc")
    @login_required
    @require_admin
    def reprocess():
        """Reprocesses the data with no additional inputs; for testing or manual csv changes"""
        try:
            run_async_task(do_data_processing())
            return "Data reloaded.", 200
        except Exception as e:
            return apputils.exception_format(e), 500
        
    @app.post('/tba-webhook')
    @require_json
    def consume_tba_webhook():
        if not request.headers or "X-TBA-HMAC" not in request.headers or request.headers.get("X-TBA-HMAC") != tba_hmac:
            return "", 401
        if not request.json:
            return "Invalid request", 400
        try:
            handle_tba_webhook(request.json)
        except Exception as e:
            app.logger.exception(f"Error handling tba webhook: {apputils.exception_format(e)}")
            return apputils.exception_format(e), 500
        
    # OPEN (grafana required)
    @app.get("/team-photo")
    def get_team_pics():
        """Return the uploaded picture cooresponding to the team given in the ?team urlparam"""
        if "team" in request.args:
            try:
                files_match = list(
                    Path("photos").glob(
                        f"{request.args.get("team", 0).strip()}*.*"
                    )
                )
                try:
                    index = int(request.args.get("index", 0))
                except:
                    index = 0 # if not int
                files_match = list(map(lambda p: p.absolute().as_posix(), files_match))
                return (
                    send_file(files_match[min(index, len(files_match))])
                    if files_match and len(files_match) > 0
                    else ("Error, team picture not found", 400)
                )
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Error: invalid request", 400
    
    @app.get("/team-photo-indicies")
    def get_team_indicies():
        if "team" in request.args:
            try:
                max_idx = len(list(
                    Path("photos").glob(
                        f"{request.args.get("team", 0).strip()}*.*"
                    )
                ))
                return jsonify(list(range(max_idx)))
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Error: invalid request", 400

    # OPEN (grafana needs this)
    @app.get("/next-3")
    def n3():
        """Returns the next app.config["NEXT_N_MATCHES_NUMBER"] (currently 3, hence the name) matches after `?mkey` that contain `?team`<br>
        where `?x` is the url parameter named `x`"""
        if not request.args["mkey"]:
            return "", 400  # bad request
        curr_match = request.args["mkey"]
        foundit = False  # whether it has found the curr_match yet
        next_3 = []
        team = int(app.config["TEAM"])
        if not processor.has_sched_data:
            return jsonify([""] * app.config["NEXT_N_MATCHES_NUMBER"])
        for m in processor.tba_data_static.schedule:
            if foundit:
                if (
                    len(next_3) >= app.config["NEXT_N_MATCHES_NUMBER"]
                ):  # app is compatible for >3 next, but keep endpoint name because i don't want to redo dash
                    break  # stop looking once you find all of them
                if (
                    team == -1  # if team == -1, just get the next 3
                    or (
                        ("frc" + str(team)) in m["b"]
                    )  # if team in this match, return it (blue)
                    or (
                        ("frc" + str(team)) in m["r"]
                    )  # if team in this match, return it (red)
                ):
                    next_3.append(m["k"])
            elif m["k"] == curr_match:
                foundit = True
        return jsonify(next_3)

    # RESTRICTED (edits input data = bad)
    @app.post("/append")
    @login_required
    @require_admin
    def append_lines():
        """Appends a series of csv lines to the input data based off of the file sent via request.files.<br>
        Complies with the restrict append level."""
        try:
            if (
                request.headers.get("sending", "false") == "false"
                and app.config["RESTRICT_APPEND_LEVEL"] == 2
                and "data" in request.files
            ):
                d_file = request.files["data"]
                # \/ readlines returns a buffer (so use decode) and also use "MN" (which doesn't change from year to year bc theres always matches), to filter out in case its a header
                lines_to_write = [
                    l.decode("utf-8").strip() + "\n"
                    for l in d_file.readlines()
                    if l
                    and (
                        not "MN" in l.decode("utf-8")
                    )  # if there's "MN", then it's the header line
                ]
                for line in lines_to_write:
                    process_queue.append(line)
                    send_change_notification()
            elif "data" in request.files:
                d_file = request.files["data"]
                lines_to_write = [
                    l.decode("utf-8").strip() + "\n"
                    for l in d_file.readlines()
                    if l and (not "MN" in l.decode("utf-8"))
                ]  # dodge header if exists
                append_lines_nofile(
                    lines_to_write, request.headers.get("sending", "false") == "true"
                )
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (edits input data = bad)
    @app.post("/apply-change/<int:idx>")
    @login_required
    @require_admin
    def apply_change(idx):
        """For restrict append level 2: applies the `idx`'th queued append"""
        if idx != None:
            item = process_queue.pop(idx)  # delete it from the array and return it
            try:
                append_lines_nofile([item])  # pass in as a single-element list
                return "", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid request", 400

    # RESTRICTED (can delete queued data = bad)
    @app.post("/delete-change/<int:idx>")
    @login_required
    @require_admin
    def delete_change(idx):
        """For restrict append level 2: drops the `idx`'th queued append"""
        if idx != None:
            process_queue.pop(idx)  # rm it from the queue/
            return "", 200
        return "Invalid request", 400

    # RESTRICTED (can delete input data = bad)
    @app.post("/delete-lines")
    @login_required
    @require_admin
    @require_json
    def delete_lines():
        """For restrict append level 1: deletes the already applied append cooresponding to the input json's `lines` field using `rm_row_hash`"""
        if (
            "sending" in request.headers
            and request.headers.get("sending", "false") == "true"
            and "si" in request.json
            and "mn" in request.json
            and mesh.get_is_meshed()
        ):
            mesh.send_command(
                f"rm {request.json["mn"]},{request.json["si"]}", admin_login["pwd"]
            )
        if request.json and request.json["lines"]:
            rm_row_hash(request.json["lines"])
            return "", 200
        return "Invalid request", 400
    
    @app.get('/auton-scout')
    @login_required
    def auton_scout():
        return render_template_style("auton-scouting.html")
    
    @app.get("/teams-in-match")
    @login_required
    def teams_in_match():
        if "mkey" in request.args:
            _match = request.args.get('mkey', "")
            if not processor.has_sched_data:
                return jsonify({
                    'k': "",
                    'r': [""] * 3,
                    'b': [""] * 3
                })
            for mat in processor.tba_data_static.schedule:
                if mat['k'] == _match:
                    return jsonify({
                        'k': mat['k'],
                        'r': list(map(int, mat['r'])),
                        'b': list(map(int, mat['b']))
                    })
            return "Error, match not in data", 404
        return "Error: invalid request", 400
    
    @app.get('/current-event')
    @login_required
    def get_current_event():
        if not processor.has_sched_data:
            return "None"
        return processor.event_key
    
    @app.get("/matches-in-comp")
    @login_required
    def matches_in_comp():
        if not processor.has_sched_data:
            return jsonify([])
        return jsonify(list(map(lambda m: m['k'], processor.tba_data_static.schedule)))

    @app.get("/events-from-team")
    @login_required
    def events_from_team():
        app.logger.info(f"Get events from team {app.config["TEAM"]} for year {processor.year}")
        return jsonify(apputils.get_tba_events(auth_key, processor.year, app.config["TEAM"]))

    @app.post("/load-event-data")
    @login_required
    @require_admin
    def load_event_data():
        if "event" in request.headers:
            event = request.headers.get('event', "").strip()
            if event and event != "":
                try:
                    run_async_task(processor.clear_database())
                    processor.event_key = event
                    with open("last_loaded_event_key.txt", 'w') as w:
                        w.write(event)
                    run_async_task(processor.load_event_data())
                    run_async_task(processor.perform_periodic_calls())
                    return "", 200
                except Exception as e:
                    return apputils.exception_format(e), 500
            return "Invalid event key", 400
        return "Error: invalid request", 400 
            
    
    @app.post("/dash-reset")
    @login_required
    @require_admin
    def reset_dash():
        try:
            compile_scouting_dashboard(request.host_url)
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500
        
    @app.post('/clear-datain-spreadsheet')
    @login_required
    @require_admin
    def clear_datain():
        try:
            os.remove(infile)
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500
        
    @app.post('/clear-db')
    @login_required
    @require_admin
    def clear_db():
        try:
            processor.clear_database()
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (can edit field-config = bad (technically not but still))
    @app.get("/edit-yaml")
    @login_required
    @require_admin
    def edit_yaml():
        """Renders the editing template for web editing the field-config.yaml file"""
        with open(
            config_file, "r"
        ) as r:  # load CONFIG_FILE into mem to pass into jinja2
            content = r.read()
        with open(os.path.join("config", "schema.json"), "r") as r:
            schema = r.read()
        return render_template_style("edit-config.html", yaml_content=content, schema=schema)
    
    @app.post('/save-file')
    @login_required
    @require_admin
    @require_json
    def save_file():
        data = html.unescape(request.json.get("code", ""))
        path = html.unescape(request.json.get("path", ""))
        app.logger.info(f"Overwriting file {path}")
        try:
            if (os.path.exists(path) and os.path.isfile(path)):
                with open(path, 'w') as f:
                    f.write(data)
                return jsonify({"ok": True, "message": "Saved"})
            else:
                return jsonify({"ok": False, "message": "File does not exist"})
        except Exception as e:
            return jsonify({"ok": False, "message": f"Error: {apputils.exception_format(e)}"})

    # RESTRICTED (overwrites field-config = bad)
    @app.post("/save-yaml")
    @login_required
    @require_admin
    @require_json
    def save_yaml():
        """A file consumer that saves the field-config.yaml file during web editing"""
        data = html.unescape(request.json.get("code", ""))
        try:
            dat = yaml.safe_load(data)
            apputils.yaml_check_schema_raise_errors(dat)
            with open(config_file, "w") as f:  # save
                f.write(data)
            processor.config_data = lex_config(app.config["YEAR"])  # reload config data
            run_async_task(do_data_processing())
            return jsonify({"ok": True, "message": "Saved"})
        except Exception as e:
            return jsonify({"ok": False, "message": apputils.exception_format(e)})

    # RESTRICTED (technically doesn't write to app config but still bad)
    @app.get("/edit-app-conf")
    @login_required
    @require_admin
    def edit_app_conf_page():
        """Returns a template for editing the app configuration"""
        return render_template_style("appconfig.html", years=POSS_YEARS)

    # RESTRICTED (don't want to share app config because it has secrets)
    # TODO: make POST probably
    @app.get("/get-config")
    @login_required
    @require_admin
    def get_app_config():
        """Returns the app configuration for the editor at /edit-app-conf to read"""
        with open(os.path.join(app.root_path, "config", "app-config.json"), "r") as r:
            return jsonify(json.load(r))
        
    @app.post("/get-log")
    @login_required
    @require_admin
    def get_log():
        if not request.headers or "log" not in request.headers:
            return "Error: invalid request", 400
        logfile = request.headers.get("log", "")
        if os.path.exists(os.path.join("log", "gunicorn", os.path.basename(logfile))):
            with open(os.path.join("log", "gunicorn", os.path.basename(logfile)), 'r') as r:
                # TODO: more secure transfer
                return r.read(), 200
        else:
            return f"File {os.path.join("log", "gunicorn", os.path.basename(logfile))} does not exist", 400
        
    @app.get("/read-log")
    @login_required
    @require_admin
    def read_log():
        return render_template_style("view-logs.html")
            

    # RESTRICTED (obviously don't want to share this)
    @app.get("/tba-key")
    @login_required
    @require_admin
    def get_tba_key():
        """returns the current TBA api key and whether or not it is good"""
        return jsonify({"key": auth_key, "good": apputils.test_tba_key(auth_key)})

    @app.get("/test-notification")
    @login_required
    @require_admin
    def test_notification():
        notification_queue.append(
            (
                {
                    "title": "Test",
                    "body": "this is a test notification",
                    "icon": url_for("static", filename="favicon.ico"),
                },
                time.time() + 10,
                [],
            )
        )
        return "Sent", 200

    # RESTRICTED (may as well, not used by grafana)
    @app.post("/test-tba-key/", defaults={"key": ""})
    @app.post("/test-tba-key/<path:key>")
    @login_required
    @require_admin
    def test_tba_key(
        key,
    ):  # necessary because fetching client-side runs into sad caching server-side
        """tests whether or not the given TBA api key is valid, returning a string 'true' for good and 'false' for bad"""
        if apputils.test_tba_key(key):
            return "true", 200
        return "false", 200

    @app.post("/set-tba-key")
    @login_required
    @require_admin
    @require_json
    def set_tba_key():
        """verifies the inputted api key sent via json["key"], applies it and writes it to tba.txt, and reprocesses data"""
        nonlocal auth_key
        try:
            if request.json and request.json["key"]:
                auth_key = request.json["key"].strip()
                if not apputils.test_tba_key(auth_key):  # health check
                    return "Bad TBA key", 400
                apputils.set_auth_key(auth_key)
                processor.tba_key = auth_key
                return "", 200
            else:
                return "Invalid Request", 400
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (overwrites un/pwd = bad)
    @app.post("/set-admin-creds")
    @login_required
    @require_admin
    @require_json
    def set_admin_creds():
        """resets the admin username and password to json["un"] and json["pwd"]"""
        nonlocal admin_login
        if request and request.json:
            try:
                un = request.json.get("un", admin_login["un"])
                pwd = (
                    apputils.line_str_hash(request.json["pwd"])
                    if "pwd" in request.json
                    else admin_login["pwd"]
                )
                apputils.change_un_pwd_admin(app.config["SECRET_KEY"], un, pwd)
                admin_login = {"un": un, "pwd": pwd}
                return "Password change successful", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid Request", 400
    
    @app.post('/set-viewer-creds')
    @login_required
    @require_admin
    @require_json
    def set_viewer_creds():
        nonlocal viewer_login
        if request and request.json:
            try:
                un = request.json.get('un', viewer_login["un"])
                pwd = (apputils.line_str_hash(request.json["pwd"]) if "pwd" in request.json else viewer_login["pwd"])
                apputils.change_un_pwd_viewer(app.config["SECRET_KEY"], un, pwd)
                viewer_login = { "un": un, "pwd": pwd }
                return "Password change successful", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid Request", 400


    @app.get("/calc-team-score")
    @login_required
    def calc_team_score():
        teams = [app.config["TEAM"]]
        if "pick1" in request.args:
            teams.append(request.args.get("pick1"))
        if "pick2" in request.args:
            teams.append(request.args.get("pick2"))
        if "pick3" in request.args:
            teams.append(request.args.get("pick3"))
        sum = 0
        if not processor.has_sched_data:
            return jsonify({"score": 0})
        for team in teams:
            if int(team) in processor.tba_data_static.teams:
                sum += processor.get_team_pred_score(team)
        return jsonify({"score": round(sum)})

    @app.get("/picklist")
    @login_required
    def picklist():
        return render_template_style(
            "picklist.html",
            initTeam=app.config["TEAM"],
            teams=filter(
                lambda x: x != int(app.config["TEAM"]),
                sorted([int(x) for x in processor.tba_data_static.teams]),
            ) if processor.has_sched_data else [],
            dashes=json.dumps(DASHBOARD_UIDS),
            grafana_base=app.config["GRAFANA_URL"] + "/d/",
        )

    # RESTRICTED (overwrites app config = bad)
    @app.post("/save-app-config")
    @login_required
    @require_admin
    @require_json
    def save_app_config():
        """Consumes an app configuration json, saves it, and applies it"""
        if request and request.json:
            try:
                with open(
                    os.path.join(app.root_path, "config", "app-config.json"), "w"
                ) as w:
                    json.dump(
                        request.json, w, indent=4
                    )  # indent=4 auto-formats the json with \t = 4 spaces
                app.config.from_file(os.path.join(app.root_path, "config", "app-config.json"), load=json.load)
                processor.year = app.config["YEAR"].split("_")[0] if "_" in app.config["YEAR"] else app.config["YEAR"]
                processor.period_min = app.config["TBA_FETCH_PERIOD_MIN"]
                if processor.has_sched_data and apputils.data_in_exists():
                    try:
                        run_async_task(processor.proccess_data(
                            infile
                        )) # updated processor, so this
                        app.logger.info("Finished processing data.")
                        reload_js()
                    except Exception as e:
                        app.logger.error(apputils.exception_format(e))
                return "", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid Request", 400

    # RESTRICTED (save bandwidth)
    @app.get("/download/<file>")
    @login_required
    def dload(file):
        """Sends a stream using the `stream` helper for the requested file to download it"""
        if not file:
            return "File not found.", 403
        file = os.path.basename(file)  # no .. touchy
        file = os.path.join(
            (
                "dataout"
                if (
                    "output.csv" in file
                    or file == "other-metrics.json"
                    or file == "sentinel.db"
                )
                else "datain"
            ),
            file,
        )  # get dir based on name
        if not os.path.exists(file):
            return "File not found.", 403
        return Response(
            apputils.stream(file),
            mimetype=("text/json" if ".json" in file else "text/csv"),
            headers={  # send over a file stream to be handled by the client XHR
                "Content-Disposition": f"attachment; filename={os.path.basename(file)}"
            },
        )

    # RESTRICTED (edits input data = bad)
    @app.get("/test-mesh")
    @login_required
    @require_admin
    def test_mesh():
        """Debug endpoint to test the meshtastic listener without a mesh radio"""
        m = request.args.get("m")  # (m is the message)
        if m:
            mesh.send_mesh_test(m)  # debug without a meshtastic
            return "Message sent"
        else:
            return "Use the 'm' url param to specify a test message"

    # =======================================================
    # Initialize Meshtastic Listener
    # =======================================================
    Thread(target=mesh_listen, daemon=True).start()  # peak multithreading
    Thread(target=start_loop, args=(loop,), daemon=True).start()

    return app


# =======================================================
# Handle running from this file for debug and keygen
# =======================================================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "pwd":
            apputils.generate_admin()
        elif sys.argv[1] == "sign":
            apputils.generate_ssl_sign()
    else:
        create_app(False, True).run(port=5001, use_reloader=False)  # debug run python
