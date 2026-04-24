from enum import Enum
import hashlib
import re
import time
import zipfile
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
from flask_login import (
    login_user,
    logout_user,
    current_user,
    AnonymousUserMixin,
)
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS

try:  # janky import stuff: running src/app.py and running scouting_app.py via gunicorn lead to different import paths
    from lib.data_main import Processor, Event
    import lib.mesh as mesh
    from lib.data_config import lex_config
    import apputils
    from endpoint_schemas import wrap_flask_routing
    import auth
except ModuleNotFoundError:
    from src.lib.data_main import Processor, Event
    import src.lib.mesh as mesh
    from src.lib.data_config import lex_config
    import src.apputils as apputils
    from src.endpoint_schemas import wrap_flask_routing
    import src.auth as auth
import csv
import os
import json
from threading import Thread
import html
import yaml
import hmac
import sys
import tempfile
import logging
from pathlib import Path
import argparse
import shlex
import shutil
from jinja2 import Environment, FileSystemLoader
import asyncio

class Notification:
    def __init__(self, data: dict, expire_time: float, clients: list[str]):
        self.data = data
        self.expire_time = expire_time
        self.clients = clients

def create_app(
    inital_process=True, skip_last_opr_fetching_for_testing_because_its_slow=False
):  # cursed but whatever
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
    app.config.from_file(os.path.join("config", "app-config.json"), load=json.load)
    app.get = wrap_flask_routing(app.get)
    app.post = wrap_flask_routing(app.post)
    app.route = wrap_flask_routing(app.route)
    # match the x in field-config-x.yaml to get the different configs
    POSS_YEARS = [
        f.stem.split("-", 2)[-1]
        for f in Path("config").relative_to(".").glob("field-config-*.yaml")
    ]
    config_file = os.path.join("config", f"field-config-{app.config["YEAR"]}.yaml")
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = False

    notification_queue: list[Notification] = []

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
            uid=(
                "anonymous"
                if isinstance(current_user, AnonymousUserMixin)
                else current_user.id
            ),
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
        os.makedirs("notes", exist_ok=True)
        os.makedirs("autos", exist_ok=True)
        os.makedirs("picklists", exist_ok=True)
        os.makedirs(
            "secrets", exist_ok=True
        )  # make sure secrets exists because we will soon open some files

    ensure_untracked_dirs()

    # =======================================================
    # Load Auth Keys
    # =======================================================

    (
        app.config["SECRET_KEY"],
        auth_key,
        tba_webhook_secret,
    ) = apputils.read_secrets()

    auth.generate_login_db("admin", apputils.line_str_hash("admin"))

    # =======================================================
    # Parse field config and setup processor
    # =======================================================
    processor = Processor(
        skip_last_opr_fetching_for_testing_because_its_slow,
        auth_key,
        (
            app.config["YEAR"].split("_")[0]
            if "_" in app.config["YEAR"]
            else app.config["YEAR"]
        ),
        lex_config(app.config["YEAR"]),
    )  # what's wrong with my copy of python why are their pointers (it's just the unpack operator)

    infile = os.path.join("datain", "data_in.csv")
    js = None  # load the json file into mem so we don't have to read it every time its requested

    process_queue = []

    # =======================================================
    # Helper functions
    # =======================================================

    # set up the login manager
    auth.init_loginm_app(app)

    def compile_scouting_dashboard(url: str):
        """Uses jinja templating to create dashboard jsons for provisioning that cast all of the number fields to numbers"""
        app.logger.info("Compiling dashboards...")
        env = Environment(loader=FileSystemLoader("."))
        for path in Path("src/templates").relative_to(".").glob("*.ji"):
            app.logger.info(f"Compiling {path}...")
            tmpl = env.get_template(path.relative_to(".").as_posix())
            # make acronym from title
            template_vars = {
                "sentinel_url": url,
                "grafana_url": app.config["GRAFANA_URL"],
                "event_prefix": processor.event_key + "_",
            }
            for k, v in processor.config_data["dash-panel"].items():
                template_vars |= {k.lower() + "_headers": v}
            os.makedirs("grafana-dashboard", exist_ok=True)
            out_path = os.path.join(
                "grafana-dashboard",
                path.relative_to(".").as_posix().rsplit(".", 2)[0].rsplit("/")[-1]
                + ".json",
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
            run_async_task(do_data_processing()).add_done_callback(
                lambda _: app.logger.info("initial processing finished.")
            )
        except Exception as e:
            app.logger.warning(
                f"initial processing failed: {apputils.exception_format(e)}"
            )

    DASHBOARD_UIDS = {}
    for dash in list(
        map(lambda p: p.stem, Path("src/templates").relative_to(".").glob("*.ji"))
    ):
        if not os.path.exists(
            f"/var/lib/grafana/dashboards/{dash}"
            if is_docker
            else os.path.join("grafana-dashboard", f"{dash}")
        ):
            compile_scouting_dashboard("http://localhost:5000")
        with open(
            (
                f"/var/lib/grafana/dashboards/{dash}"
                if is_docker
                else os.path.join("grafana-dashboard", f"{dash}")
            ),
            "r",
        ) as r:
            DASHBOARD_UIDS |= {dash: json.load(r)["uid"]}

    def reload_js():
        """Updates the copy of the 'other-metrics.json' file in memory (used for '/percent' endpoint) to use the newest file"""
        nonlocal js
        if not os.path.exists(os.path.join("dataout", "other-metrics.json")):
            js = None
            return
        with open(
            os.path.join("dataout", "other-metrics.json"),
            "r",
        ) as r:
            js = (
                json.load(r)
                if os.path.exists(os.path.join("dataout", "other-metrics.json"))
                else None
            )

    reload_js()

    def send_generic_notification(data: dict):
        notification_queue.append(
            Notification(data | { "icon": "/static/favicon.ico" }, time.time() + 300, [])
        )

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
            "icon": "/static/favicon.ico",
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
            Notification(data, time.time() + 300, [])
        )  # gone is the toilsome webpush shenanigens (ik i spelled that wrong)

    def handle_tba_webhook(notification_json):
        if (
            not "message_type" in notification_json
            or not "message_data" in notification_json
        ):
            return
        match (notification_json["message_type"]):
            case "match_score":
                if (
                    processor.event_key
                    and notification_json["message_data"]["event_key"]
                    == processor.event_key
                ):
                    run_async_task(processor.perform_periodic_calls())
                return
            case "schedule_updated":
                if (
                    processor.event_key
                    and notification_json["message_data"]["event_key"]
                    == processor.event_key
                ):
                    event = processor.event_key
                    run_async_task(processor.clear_database())
                    processor.event_key = event
                    with open("last_loaded_event_key.txt", "w") as w:
                        w.write(event)
                    run_async_task(processor.load_event_data())
                    run_async_task(processor.perform_periodic_calls())
                    return
            case "ping":
                md = notification_json["message_data"]
                app.logger.info(
                    f"TBA pinged server:\nTitle: {md["title"]}\nDesc: {md["desc"]}"
                )
                send_generic_notification(
                    {"title": f"TBA Ping: {md["title"]}", "body": md["desc"]}
                )
                return
            case "broadcast":
                md = notification_json["message_data"]
                app.logger.info(
                    f"TBA announcement:\nTitle: {notification_json["message_data"]["title"]}\nDesc: {notification_json["message_data"]["desc"]}\nUrl: {notification_json["message_data"]["url"] if "url" in notification_json["message_data"] else "None"}"
                )
                send_generic_notification(
                    {
                        "title": f"TBA Broadcast: {md["title"]}",
                        "body": md["desc"] + (md["url"] if "url" in md else ""),
                    }
                )
            case "verification":
                app.logger.info(f"TBA webhook verification recieved")
                send_generic_notification(
                    {
                        "title": "TBA Webhook verification",
                        "body": notification_json["message_data"]["verification_key"],
                    }
                )
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
        pattern = re.compile(rf"^{re.escape(team)}-\d+.*$")
        files_there = []
        for path in Path("photos").glob("*"):
            if path.is_file() and pattern.match(path.name):
                files_there.append(path)
        file_save = os.path.join("photos", f"{team}-{len(files_there)}{ext}")
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
            if args.pwd.strip() == SUPERUSER_CODE:
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

    def mesh_listen():
        """Binds `handle_mesh_line` to the meshtastic port (listens for meshages)"""
        mesh.main(handle_mesh_line)

    # =======================================================
    # Endpoints
    # =======================================================

    @app.before_request
    def log_request():
        """logs the method and path of the request"""
        if (
            not "/notifyq" in request.path and not "/jobs" in request.path
        ):  # that spam is annoying
            app.logger.info(f"{request.method} {request.path}")
        elif "/notifyq" in request.path:
            app.logger.info(f"Service worker polled queue")

    @app.errorhandler(Exception)
    def handle_exception(e):
        """Dampens the app's explosion"""
        app.logger.exception(f"Unhandled Exception: {apputils.exception_format(e)}")
        if request.method == "GET":
            return render_template_style(
                "error.html",
                head="500: Internal server error",
                msg=f"Internal server error in page LOC<br><span style='color: red'>{html.escape(apputils.exception_format(e))}</span>",
            )
        else:
            return f"Internal server error: {apputils.exception_format(e)}", 500

    @app.errorhandler(401)
    def handle_401(e):
        app.logger.warning(f"Tried to access page without login: {e.description}")
        if request.method == "GET":
            return render_template_style(
                "error.html", head="401: Unauthorized", msg="Error: LOC is unauthorized"
            )
        else:
            return "Error: unauthorized", 401

    @app.errorhandler(403)
    def handle_403(e):
        app.logger.warning(f"Tried to access restricted page: {e.description}")
        if request.method == "GET":
            return render_template_style(
                "error.html",
                head="403: Restricted",
                msg=f"Error: LOC is restricted for user '{"anonymous" if isinstance(current_user, AnonymousUserMixin) else current_user.un}'",
            )
        else:
            return "Error: restricted", 403

    @app.errorhandler(404)
    def handle_404(e):
        """Custom 404 handler"""
        app.logger.warning(f"Tried to access nonexistent page: {e}")
        if request.method == "GET":
            return render_template_style(
                "error.html",
                head="404: Page not found",
                msg="Error: the path LOC is not defined for this server. If it should, please submit an issue at https://github.com/Team4028/Sentinel/issues",
            )
        else:
            return "Error: page not found", 404

    # RESTRICTED (can pop the entire queue by spamming the endpoint)
    @app.route("/notifyq")
    def notify_q():
        """pop the queue of notifications for frontend to consume and notify the user"""
        cid = request.headers.get("X-Cid")
        if cid == "null":
            cid = None
        for n in reversed(notification_queue):
            if n.expire_time <= time.time():
                notification_queue.remove(n)
            elif not (cid in n.clients) and cid != None:
                n.clients.append(cid)
                return jsonify(n.data)
        if cid == None:
            return "Error, invalid cid", 400
        return "No notifications in queue", 204  # 204 => no content

    # RESTRICTED
    @app.get("/")
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
        form = auth.LoginForm()
        if form.validate_on_submit():
            username = form.username.data
            password = form.password.data.strip()
            if user := auth.get_user_from_db_unpw(username, password):
                login_user(user)
                return "Login Successful", 200
            else:
                return "Invalid Credentials", 401
        return render_template_style("login.html", form=form)

    @app.post("/create-login")
    def create_login():
        try:
            username, password, is_admin = (
                request.json["un"],
                request.json["pwd"],
                request.json["isadmin"],
            )
            auth.add_user_to_db(username, password, is_admin)
            return "Success", 200
        except Exception as e:
            return f"Error: {apputils.exception_format(e)}", 500

    @app.post("/get-user-display")
    def get_user_display():
        return auth.display_user(request.headers["id"])

    @app.post("/login-ovr")
    def login_override():
        if request.json["key"] == SUPERUSER_CODE:
            login_user(auth.override_get_admin())
            return "Login Override Successful", 200
        else:
            return "Invalid Credentials", 401

    @app.get("/explore")
    def explore():
        def build_tree(path):
            tree = []
            try:
                for item in sorted(os.listdir(path)):
                    full_path = os.path.join(path, item)
                    if os.path.isdir(full_path):
                        tree.append(
                            {
                                "type": "folder",
                                "name": item,
                                "children": build_tree(full_path),
                            }
                        )
                    else:
                        tree.append({"type": "file", "name": item})
            except PermissionError:
                pass

            tree.sort(
                key=lambda x: (0 if x["type"] == "folder" else 1, x["name"].lower())
            )

            return tree

        return render_template_style("file-explorer.html", tree=build_tree("."))

    @app.get("/edit-file")
    def edit_file():
        filepath = request.args.get("filepath").strip()
        filepath = html.unescape(filepath)
        if os.path.exists(filepath) and os.path.isfile(filepath):
            with open(filepath, "r", encoding="utf-8") as r:
                content = r.read()
            return render_template_style(
                "edit-file.html",
                file_path=filepath,
                file_name=os.path.basename(filepath),
                file_content=content,
            )
        else:
            return "Error, path does not exist", 400

    @app.get("/view-file")
    def view_file():
        filepath = request.args.get("filepath").strip()
        filepath = html.unescape(filepath)
        if os.path.exists(filepath) and os.path.isfile(filepath):
            return send_file(Path(filepath).resolve())
        else:
            return "Error, path does not exist", 400

    @app.post("/rename-file")
    def rename_file():
        oldf = request.json["old"].strip()
        if not is_docker:
            app.logger.warning(
                f"Tried to rename file {oldf}, only allowed in virtual container."
            )
            return "Not allowed in local testing", 401
        newf = request.json["new"].strip()
        newf = Path(oldf).with_name(newf).as_posix()
        if os.path.exists(oldf) and (os.path.isfile(oldf) or os.path.isdir(oldf)):
            os.rename(oldf, newf)
            return "Success", 200
        else:
            return "Error, path does not exist", 400

    @app.post("/delete-file")
    def delete_file():
        filepath = request.json["filepath"].strip()
        if "resolve" in request.json:
            match (filepath):
                case "data_in.csv":
                    filepath = os.path.join("datain", filepath)
                case _:
                    filepath = os.path.join("dataout", filepath)
        if not is_docker:
            app.logger.warning(
                f"Tried to delete file {filepath}, only allowed in virtual container."
            )
            return "Not allowed in local testing", 401
        if os.path.exists(filepath) and (
            os.path.isfile(filepath) or os.path.isdir(filepath)
        ):
            if os.path.isfile(filepath):
                os.remove(filepath)
            elif os.path.isdir(filepath):
                os.removedirs(filepath)
            return "Success", 200
        else:
            return "Error, path does not exist", 400

    @app.get("/jobs")
    def jobs():
        return jsonify(Event.get_event_progress())

    @app.get("/我是谁")
    def whoami():
        if current_user.is_authenticated:
            return jsonify(
                {
                    "logged_in": True,
                    "id": current_user.id,
                    "username": current_user.un,
                    "admin": current_user.is_admin,
                }
            )
        return jsonify({"logged_in": False})

    # PARTIALLY OPEN (just need to be logged in so basically closed, but technically no admin is necessary)
    @app.get("/logout")
    def logout():
        logout_user()
        return redirect(url_for("login"))

    # OPEN (sends immutable copy, nothing to hide bc open source)
    @app.get("/service_worker.js")
    def send_sw():
        """Passthrough for the service worker so that the client can fetch it"""
        return send_file("service_worker.js")

    @app.get("/pit")
    def pit_scout():
        return render_template_style("pit-scouting.html")

    @app.get("/auton-simple")
    def auto_scout_simple():
        return render_template_style("auton-scouting-simple.html")

    @app.post("/submit-pit")
    def save_pit():
        try:
            csv_file = os.path.join("dataout", "output.csv" + "-pit-scouting.csv")
            need_write = not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0
            with open(
                os.path.join("dataout", "output.csv" + "-pit-scouting.csv"),
                mode="a",
                newline="",
            ) as f:
                writer = csv.DictWriter(f, fieldnames=request.json.keys())
                if need_write:
                    writer.writeheader()
                writer.writerow(request.json)
            return "Success", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.post("/submit-auto-simple")
    def save_auto_simple():
        try:
            csv_file = os.path.join("dataout", "output.csv" + "-auton-scouting.csv")
            need_write = not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0
            js: dict = request.json
            was_pre = js.pop("wasPre") if "wasPre" in js else False
            if os.path.exists(csv_file):
                with open(csv_file, mode="r", newline="") as r:
                    matches_scouted = len(
                        list(
                            filter(
                                lambda s: (f"{js["tn"]}" in s.strip())
                                and ((not was_pre) ^ ("Pre" in s.strip())),
                                r.readlines(),
                            )
                        )
                    )
            else:
                matches_scouted = 0
            js = {"Match": f"{"Pre " if was_pre else ""}{matches_scouted + 1}"} | js
            with open(csv_file, mode="a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=js.keys())
                if need_write:
                    writer.writeheader()
                writer.writerow(js)
            return "Success", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (can edit things and delete data = restrict)
    @app.get("/changes")
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

    @app.post("/restart")
    def restart():
        # docker is set to restart: unless-stopped, and so exiting the PID 1 process will restart the container
        os._exit(1)

    # OPEN (immutable passthrough for other-metrics = fine)
    @app.get("/percent")
    def percent():  # need because grafana infinity can't do local JSON
        """Rest passthrough for other-metrics.json, named percent because its current data is the percentage of teams scouted."""
        return jsonify(js) if js else ""

    # RESTRICTED (completely wipes out input data = bad)
    @app.post("/upload")
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
    def upload_other_files():
        try:
            d_file = request.files["data"]
            if d_file.filename != "" and request.headers.get("name", "").strip() != "":
                d_file.save(os.path.join("dataout", request.headers.get("name")))
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.post("/upload-auto")
    def upload_auto():
        try:
            match = request.headers.get("mkey")
            photo = request.files["photo"]
            if photo.filename != "":
                save_auto(photo, match)
                return "Saved!", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (uploads files = writes to server directory = bad)
    @app.post("/upload-photo")
    def upload_photo():
        """Save a photo for the cooresponding team"""
        try:
            team = request.headers.get("team")
            photo = request.files["photo"]
            if photo.filename != "":
                save_photo(photo, team)
                return "Saved!", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (not as bad, but still requires decent processing power)
    @app.post("/reproc")
    def reprocess():
        """Reprocesses the data with no additional inputs; for testing or manual csv changes"""
        try:
            run_async_task(do_data_processing())
            return "Data reloaded.", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.post("/tba-webhook")
    def consume_tba_webhook():
        if (
            request.headers.get("X-TBA-HMAC")
            != hmac.new(
                tba_webhook_secret.encode("utf-8"),
                json.dumps(request.json, ensure_ascii=True).encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
        ):
            app.logger.warning(
                f"Invalid webhook: key = {request.headers.get('X-TBA-HMAC') if request.headers and 'X-TBA-HMAC' in request.headers else "None"}"
            )
            return "", 401
        try:
            handle_tba_webhook(request.json)
            return "", 200
        except Exception as e:
            app.logger.exception(
                f"Error handling tba webhook: {apputils.exception_format(e)}"
            )
            return apputils.exception_format(e), 500

    # OPEN (grafana required)
    @app.get("/team-photo")
    def get_team_pics():
        """Return the uploaded picture cooresponding to the team given in the ?team urlparam"""
        if "team" in request.args:
            try:
                files_match = list(
                    Path("photos").glob(f"{request.args.get("team", 0).strip()}*.*")
                )
                try:
                    index = int(request.args.get("index", 0))
                except:
                    index = 0  # if not int
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
                max_idx = len(
                    list(
                        Path("photos").glob(f"{request.args.get("team", 0).strip()}*.*")
                    )
                )
                return jsonify(list(range(max_idx)))
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Error: invalid request", 400

    @app.get("/take-notes")
    def take_notes():
        return render_template_style("take-notes.html")

    @app.get("/get-notes")
    def get_notes():
        path = os.path.join(
            "notes",
            html.unescape(request.headers["team"]),
            html.unescape(request.headers["pre"] + request.headers["match"]),
            html.unescape(current_user.id) + ".txt",
        )
        if os.path.exists(path):
            with open(path, "r") as r:
                return r.read()
        else:
            return "File not found", 400

    @app.get("/note-tables")
    def get_note_tables():
        team = request.args["team"]
        tn_path = os.path.join("notes", team)
        if not os.path.isdir(tn_path):
            return {}

        table = []
        for mn in os.listdir(tn_path):
            mn_path = os.path.join(tn_path, mn)
            if not os.path.isdir(mn_path):
                continue
            row = {"Match": mn}
            for file in os.listdir(mn_path):
                if file.endswith(".txt"):
                    si = os.path.splitext(file)[0]
                    file_path = os.path.join(mn_path, file)

                    with open(file_path, "r", encoding="utf-8") as r:
                        row[si] = r.read()

            table.append(row)
        return table

    # OPEN (grafana needs this)
    @app.get("/next-3")
    def n3():
        """Returns the next app.config["NEXT_N_MATCHES_NUMBER"] (currently 3, hence the name) matches after `?mkey` that contain `?team`<br>
        where `?x` is the url parameter named `x`"""
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
    def append_lines():
        """Appends a series of csv lines to the input data based off of the file sent via request.files.<br>
        Complies with the restrict append level."""
        try:
            if (
                request.headers.get("sending", "false") == "false"
                and app.config["RESTRICT_APPEND_LEVEL"] == 2
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
    def delete_change(idx):
        """For restrict append level 2: drops the `idx`'th queued append"""
        if idx != None:
            process_queue.pop(idx)  # rm it from the queue/
            return "", 200
        return "Invalid request", 400

    # RESTRICTED (can delete input data = bad)
    @app.post("/delete-lines")
    def delete_lines():
        """For restrict append level 1: deletes the already applied append cooresponding to the input json's `lines` field using `rm_row_hash`"""
        if request.headers.get("sending", "false") == "true" and mesh.get_is_meshed():
            mesh.send_command(
                f"rm {request.json["mn"]},{request.json["si"]}", SUPERUSER_CODE
            )
        rm_row_hash(request.json["lines"])
        return "", 200

    @app.get("/auton-scout")
    def auton_scout():
        return render_template_style("auton-scouting.html")

    @app.get("/teams-in-match")
    def teams_in_match():
        _match = request.args.get("mkey", "")
        if not processor.has_sched_data:
            return jsonify({"k": "", "r": [""] * 3, "b": [""] * 3})
        for mat in processor.tba_data_static.schedule:
            if mat["k"] == _match:
                return jsonify(
                    {
                        "k": mat["k"],
                        "r": list(map(int, mat["r"])),
                        "b": list(map(int, mat["b"])),
                    }
                )
        return "Error, match not in data", 404

    @app.get("/current-event")
    def get_current_event():
        if not processor.has_sched_data:
            return "None"
        return processor.event_key

    @app.get("/matches-in-comp")
    def matches_in_comp():
        if not processor.has_sched_data:
            return jsonify([])
        return jsonify(list(map(lambda m: m["k"], processor.tba_data_static.schedule)))

    @app.get("/events-from-team")
    def events_from_team():
        app.logger.info(
            f"Get events from team {app.config["TEAM"]} for year {processor.year}"
        )
        return jsonify(
            apputils.get_tba_events(auth_key, processor.year, app.config["TEAM"])
        )

    @app.post("/load-event-data")
    def load_event_data():
        event = request.headers.get("event", "").strip()
        if event and event != "":
            try:
                run_async_task(processor.clear_database())
                processor.event_key = event
                with open("last_loaded_event_key.txt", "w") as w:
                    w.write(event)
                run_async_task(processor.load_event_data())
                run_async_task(processor.perform_periodic_calls())
                return "", 200
            except Exception as e:
                return apputils.exception_format(e), 500
        return "Invalid event key", 400

    @app.post("/dash-reset")
    def reset_dash():
        try:
            compile_scouting_dashboard(request.host_url)
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.post("/clear-datain-spreadsheet")
    def clear_datain():
        try:
            os.remove(infile)
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.post("/clear-db")
    def clear_db():
        try:
            processor.clear_database()
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (can edit field-config = bad (technically not but still))
    @app.get("/edit-yaml")
    def edit_yaml():
        """Renders the editing template for web editing the field-config.yaml file"""
        with open(
            config_file, "r"
        ) as r:  # load CONFIG_FILE into mem to pass into jinja2
            content = r.read()
        with open(os.path.join("config", "schema.json"), "r") as r:
            schema = r.read()
        return render_template_style(
            "edit-config.html", yaml_content=content, schema=schema
        )

    @app.post("/save-file")
    def save_file():
        data = html.unescape(request.json.get("code", ""))
        path = html.unescape(request.json.get("path", ""))
        app.logger.info(f"Overwriting file {path}")
        try:
            if os.path.exists(path) and os.path.isfile(path):
                with open(path, "w") as f:
                    f.write(data)
                return jsonify({"ok": True, "message": "Saved"})
            else:
                return jsonify({"ok": False, "message": "File does not exist"})
        except Exception as e:
            return jsonify(
                {"ok": False, "message": f"Error: {apputils.exception_format(e)}"}
            )

    @app.post("/save-notes")
    def save_notes():
        data = html.unescape(request.json.get("data", ""))
        team = html.unescape(request.json.get("team", ""))
        name = current_user.id
        pre = html.unescape(request.json.get("pre", ""))
        match = pre + html.unescape(request.json.get("match", ""))
        app.logger.info(f"Saving notes for {team} match {match} from scouter {name}")
        path = os.path.join("notes", team, match, name + ".txt")
        os.makedirs(os.path.join("notes", team, match), exist_ok=True)
        try:
            with open(path, "w") as f:
                f.write(data)
            return jsonify({"ok": True, "message": "Saved"})
        except Exception as e:
            return jsonify(
                {"ok": False, "message": f"Error: {apputils.exception_format(e)}"}
            )

    # RESTRICTED (overwrites field-config = bad)
    @app.post("/save-yaml")
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
    def edit_app_conf_page():
        """Returns a template for editing the app configuration"""
        return render_template_style("appconfig.html", years=POSS_YEARS)

    # RESTRICTED (don't want to share app config because it has secrets)
    @app.get("/get-config")
    def get_app_config():
        """Returns the app configuration for the editor at /edit-app-conf to read"""
        with open(os.path.join(app.root_path, "config", "app-config.json"), "r") as r:
            return jsonify(json.load(r))

    @app.post("/get-log")
    def get_log():
        logfile = request.headers.get("log", "")
        if os.path.exists(os.path.join("log", "gunicorn", os.path.basename(logfile))):
            with open(
                os.path.join("log", "gunicorn", os.path.basename(logfile)), "r"
            ) as r:
                # TODO: more secure transfer
                return r.read(), 200
        else:
            return (
                f"File {os.path.join("log", "gunicorn", os.path.basename(logfile))} does not exist",
                400,
            )

    @app.get("/read-log")
    def read_log():
        return render_template_style("view-logs.html")

    # RESTRICTED (obviously don't want to share this)
    @app.get("/tba-key")
    def get_tba_key():
        """returns the current TBA api key and whether or not it is good"""
        return jsonify(
            {
                "key": auth_key,
                "webkey": tba_webhook_secret,
                "good": apputils.test_tba_key(auth_key),
            }
        )

    @app.get("/test-notification")
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
    @app.post("/test-tba-key")
    def test_tba_key():
        """tests whether or not the given TBA api key is valid, returning a string 'true' for good and 'false' for bad"""
        key = request.headers["key"]
        if apputils.test_tba_key(key):
            return "true", 200
        return "false", 200

    @app.post("/set-tba-key")
    def set_tba_key():
        """verifies the inputted api key sent via json["key"], applies it and writes it to tba.txt, and reprocesses data"""
        nonlocal auth_key
        try:
            auth_key = request.json["key"].strip()
            if not apputils.test_tba_key(auth_key):  # health check
                return "Bad TBA key", 400
            apputils.set_auth_key(auth_key)
            processor.tba_key = auth_key
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.post("/set-tba-whook-key")
    def set_tba_whook_key():
        nonlocal tba_webhook_secret
        try:
            tba_webhook_secret = request.json["key"].strip()
            apputils.set_tba_whook_key(tba_webhook_secret)
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    # RESTRICTED (overwrites un/pwd = bad)
    @app.post("/set-creds")
    def set_creds():
        """add or overwrite a login"""
        try:
            un = request.json["un"].strip()
            pwd = request.json["pwd"].strip()
            is_admin = request.json["isadmin"]
            auth.add_user_to_db(un, pwd, is_admin)
            return "Success", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.get("/calc-team-score")
    def calc_team_score():
        teams = [app.config["TEAM"]]
        teams.append(request.args.get("pick1", 0))
        teams.append(request.args.get("pick2", 0))
        teams.append(request.args.get("pick3", 0))
        sum = 0
        if not processor.has_sched_data:
            return jsonify({"score": 0})
        for team in teams:
            if not apputils.can_cast(team, int): continue
            if int(team) in processor.tba_data_static.teams:
                sum += processor.get_team_pred_score(team)
        return jsonify({"score": round(sum)})

    @app.get("/multi-team-view")
    def multi_view():
        return render_template_style(
            "multi-team-view.html",
            teams=(
                sorted([int(x) for x in processor.tba_data_static.teams])
                if processor.has_sched_data
                else []
            ),
            dashes=json.dumps(DASHBOARD_UIDS),
            grafana_base=app.config["GRAFANA_URL"] + "/d/",
        )

    @app.get("/view-picklist")
    def view_picklist():
        return render_template_style(
            "view-picklists.html",
            names=list(map(lambda x: x.stem, Path("picklists").glob("*"))),
            initTeam=app.config["TEAM"],
            dashes=json.dumps(DASHBOARD_UIDS),
            grafana_base=app.config["GRAFANA_URL"] + "/d/",
        )

    @app.get("/get-picklist")
    def get_picklist():
        jsfiles = list(Path("picklists").glob(f"{request.headers["name"]}.json"))
        if len(jsfiles) > 0:
            jsfile = jsfiles[0]
            with open(jsfile, "r") as r:
                return jsonify(json.load(r))
        else:
            return "File not found", 400

    @app.post("/make-comment")
    def make_comment():
        if os.path.exists(os.path.join("picklists", f"{request.json["list"]}.json")):
            with open(
                os.path.join("picklists", f"{request.json["list"]}.json"), "r"
            ) as r:
                js = json.load(r)
                if request.json["pick"] in js and any(
                    x["team"] == request.json["team"]
                    for x in js[request.json["pick"]]
                ):
                    jsteam = [
                        x
                        for x in js[request.json["pick"]]
                        if x["team"] == request.json["team"]
                    ][0]
                    jsteam["comments"].append({
                        "name": current_user.id,
                        "body": request.json["msg"]
                    })
            with open(os.path.join("picklists", f"{request.json["list"]}.json"), 'w') as w:
                json.dump(js, w, indent=4)
            return "", 200
        else:
            return "File not found", 404

    @app.post("/update-like")
    def update_like():
        if os.path.exists(os.path.join("picklists", f"{request.headers["list"]}.json")):
            with open(
                os.path.join("picklists", f"{request.headers["list"]}.json"), "r"
            ) as r:
                js = json.load(r)
                if request.headers["pick"] in js and any(
                    [
                        x["team"] == request.headers["team"]
                        for x in js[request.headers["pick"]]
                    ]
                ):
                    jsteam = [
                        x
                        for x in js[request.headers["pick"]]
                        if x["team"] == request.headers["team"]
                    ][0]
                    match (request.headers["like"]):
                        case "like":
                            (
                                jsteam["like"].append(current_user.id)
                                if current_user.id not in jsteam["like"]
                                else ()
                            )
                            jsteam["dlike"] = [
                                x for x in jsteam["dlike"] if x != current_user.id
                            ]
                        case "dlike":
                            jsteam["like"] = [
                                x for x in jsteam["like"] if x != current_user.id
                            ]
                            (
                                jsteam["dlike"].append(current_user.id)
                                if current_user.id not in jsteam["dlike"]
                                else ()
                            )
                        case _:
                            jsteam["like"] = [
                                x for x in jsteam["like"] if x != current_user.id
                            ]
                            jsteam["dlike"] = [
                                x for x in jsteam["dlike"] if x != current_user.id
                            ]
            with open(
                os.path.join("picklists", f"{request.headers["list"]}.json"), "w"
            ) as w:
                json.dump(js, w, indent=4)
            return "", 200
        else:
            return "File not found", 400

    @app.get("/picklist")
    def picklist():
        return render_template_style(
            "picklist.html",
            initTeam=app.config["TEAM"],
            teams=(
                filter(
                    lambda x: x != int(app.config["TEAM"]),
                    sorted([int(x) for x in processor.tba_data_static.teams]),
                )
                if processor.has_sched_data
                else []
            ),
            dashes=json.dumps(DASHBOARD_UIDS),
            grafana_base=app.config["GRAFANA_URL"] + "/d/",
        )

    @app.post("/save-picklist")
    def save_picklist():  # TODO: don't overwrite likes/comments
        pickname = current_user.id
        pickpath = os.path.join("picklists", pickname + ".json")
        js = request.json
        for _list in js.keys():
            teams_new = []
            for team in js[_list]:
                teams_new.append({"team": team, "like": [], "dlike": []})
            js[_list] = teams_new
        with open(pickpath, "w") as w:
            json.dump(request.json, w, indent=4)
        return "Success", 200

    # RESTRICTED (overwrites app config = bad)
    @app.post("/save-app-config")
    def save_app_config():
        """Consumes an app configuration json, saves it, and applies it"""
        try:
            with open(
                os.path.join(app.root_path, "config", "app-config.json"), "w"
            ) as w:
                json.dump(
                    request.json, w, indent=4
                )  # indent=4 auto-formats the json with \t = 4 spaces
            app.config.from_file(
                os.path.join(app.root_path, "config", "app-config.json"),
                load=json.load,
            )
            processor.year = (
                app.config["YEAR"].split("_")[0]
                if "_" in app.config["YEAR"]
                else app.config["YEAR"]
            )
            if processor.has_sched_data and apputils.data_in_exists():
                try:
                    run_async_task(
                        processor.proccess_data(infile)
                    )  # updated processor, so this
                    app.logger.info("Finished processing data.")
                    reload_js()
                except Exception as e:
                    app.logger.error(apputils.exception_format(e))
            return "", 200
        except Exception as e:
            return apputils.exception_format(e), 500

    @app.get("/download")
    def dload():
        """Sends a stream using the `stream` helper for the requested file to download it"""
        file = request.headers["file"]
        file = os.path.basename(file)  # no .. touchy
        file = os.path.join(
            (
                "dataout"
                if (
                    "output.csv" in file
                    or file in ["other-metrics.json", "sentinel.db"]
                )
                else "datain"
            ),
            file,
        )  # get dir based on name
        if not os.path.exists(file):
            return "File not found.", 404
        return Response(
            apputils.stream(file),
            mimetype=("text/json" if ".json" in file else "text/csv"),
            headers={  # send over a file stream to be handled by the client XHR
                "Content-Disposition": f"attachment; filename={os.path.basename(file)}"
            },
        )

    @app.get("/download-folder")
    def download_folder():
        path = html.unescape(request.headers.get("path"))
        temp_dir = tempfile.gettempdir()
        zip_name = os.path.basename(path) + ".zip"
        zip_path = os.path.join(temp_dir, zip_name)
        shutil.make_archive(zip_path.replace(".zip", ""), "zip", path)
        return send_file(zip_path, as_attachment=True)

    @app.post("/upload-folder")
    def upload_folder():
        file = request.files["data"]
        if file.filename == "":
            return "Error, no selected file", 400
        filename = file.filename
        temp_dir = tempfile.gettempdir()
        zip_path = os.path.join(temp_dir, filename)
        file.save(zip_path)

        try:
            with zipfile.ZipFile(zip_path, "r") as r:
                r.extractall(request.headers["folderPath"])
        except zipfile.BadZipFile:
            return "Invalid Zip file", 400
        return "File uploaded successfully", 200

    # RESTRICTED (edits input data = bad)
    @app.get("/test-mesh")
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
