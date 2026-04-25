from functools import wraps
from flask import request, current_app
from flask_login import login_required
from enum import Enum

try:
    from auth import require_admin
except ModuleNotFoundError:
    from src.auth import require_admin

class EndpointAccess(Enum):
    OPEN = 0
    LOGIN = 1
    ADMIN = 2

class EndpointSchema:

    @staticmethod
    def __verify_schema(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            return ENDPOINT_HEADERS[f.__name__].__check_schema(request, f, *args, **kwargs)
        return decorated
    
    @staticmethod
    def wrap_flask_routing(flask_route_method):
        def new_route(*args, **kwargs):
            def decorator(f):

                if getattr(f, '_schema_wrapped', False):
                    wrapped = f
                else:
                    wrapped = EndpointSchema.__verify_schema(f)
                    wrapped._schema_wrapped = True
                    wraps(f)(wrapped)

                return flask_route_method(*args, **kwargs)(wrapped)
            return decorator
        return new_route

    def __init__(self, access: EndpointAccess, check_json: bool = False, headers: list[str] = [], args: list[str] = [], json: list[str] = [], files: list[str] = []):
        self.access = access
        self.check_json = check_json
        self.headers = headers
        self.args = args
        self.json = json
        self.files = files

    def __check_schema(self, request, f, *args, **kwargs):
        match self.access:
            case EndpointAccess.ADMIN:
                f = require_admin(login_required(f))
            case EndpointAccess.LOGIN:
                f = login_required(f)

        if self.check_json or len(self.json) > 0:
            if not request.is_json or request.get_json(silent=True) == None:
                return "Error, Content-Type must be application/json and JSON must be valid", 415
                
        if request:
            if len(self.headers) > 0 and not all([x in request.headers for x in self.headers]):
                return "Error: invalid header configuration", 400
            if len(self.args) > 0 and not all([x in request.args for x in self.args]):
                return "Error: invalid urlparam configuration", 400
            if len(self.json) > 0 and not all([x in request.json for x in self.json]):
                return "Error: invalid json schema", 400
            if len(self.files) > 0 and not all([x in request.files for x in self.files]):
                return "Error: missing files", 400
            if callable(getattr(current_app, "ensure_sync", None)):
                return current_app.ensure_sync(f)(*args, **kwargs)
            return f(*args, **kwargs)
        return "Error, invalid request", 400
    
wrap_flask_routing = EndpointSchema.wrap_flask_routing

ENDPOINT_HEADERS = {
    "notify_q": EndpointSchema(access=EndpointAccess.ADMIN, headers=["X-Cid"]),
    "main": EndpointSchema(access=EndpointAccess.LOGIN),
    "login": EndpointSchema(access=EndpointAccess.OPEN),
    "create_account": EndpointSchema(access=EndpointAccess.OPEN),
    "delete_account": EndpointSchema(access=EndpointAccess.ADMIN, json=["uid"]),
    "manage_accounts": EndpointSchema(access=EndpointAccess.ADMIN),
    "create_login": EndpointSchema(access=EndpointAccess.ADMIN, json=["un", "pwd", "isadmin"]),
    "get_user_display": EndpointSchema(access=EndpointAccess.LOGIN, headers=["id"]),
    "login_override": EndpointSchema(access=EndpointAccess.OPEN, json=["key"]),
    "explore": EndpointSchema(access=EndpointAccess.ADMIN),
    "edit_file": EndpointSchema(access=EndpointAccess.ADMIN, args=["filepath"]),
    "view_file": EndpointSchema(access=EndpointAccess.ADMIN, args=["filepath"]),
    "rename_file": EndpointSchema(access=EndpointAccess.ADMIN, json=["old", "new"]),
    "delete_file": EndpointSchema(access=EndpointAccess.ADMIN, json=["filepath"]),
    "jobs": EndpointSchema(access=EndpointAccess.LOGIN),
    "whoami": EndpointSchema(access=EndpointAccess.OPEN),
    "logout": EndpointSchema(access=EndpointAccess.LOGIN),
    "send_sw": EndpointSchema(access=EndpointAccess.OPEN),
    "pit_scout": EndpointSchema(access=EndpointAccess.OPEN),
    "auto_scout_simple": EndpointSchema(access=EndpointAccess.OPEN),
    "save_pit": EndpointSchema(access=EndpointAccess.LOGIN, check_json=True),
    "save_auto_simple": EndpointSchema(access=EndpointAccess.LOGIN, check_json=True),
    "changes": EndpointSchema(access=EndpointAccess.ADMIN),
    "health": EndpointSchema(access=EndpointAccess.OPEN),
    "percent": EndpointSchema(access=EndpointAccess.OPEN),
    "upload_file": EndpointSchema(access=EndpointAccess.ADMIN, files=["data"]),
    "upload_other_files": EndpointSchema(access=EndpointAccess.ADMIN, headers=["name"], files=["data"]),
    "upload_auto": EndpointSchema(access=EndpointAccess.ADMIN, headers=["mkey"], files=["photo"]),
    "upload_photo": EndpointSchema(access=EndpointAccess.ADMIN, headers=["team"], files=["photo"]),
    "reprocess": EndpointSchema(access=EndpointAccess.ADMIN),
    "restart": EndpointSchema(access=EndpointAccess.ADMIN),
    "consume_tba_webhook": EndpointSchema(access=EndpointAccess.OPEN, headers=["X-TBA-HMAC"], json=["message_type"]),
    "get_team_pics": EndpointSchema(access=EndpointAccess.OPEN, args=["team"]),
    "get_team_indicies": EndpointSchema(access=EndpointAccess.OPEN, args=["team"]),
    "take_notes": EndpointSchema(access=EndpointAccess.LOGIN),
    "get_notes": EndpointSchema(access=EndpointAccess.LOGIN, headers=["match", "team", "pre"]),
    "get_note_tables": EndpointSchema(access=EndpointAccess.OPEN, args=["team"]),
    "n3": EndpointSchema(access=EndpointAccess.OPEN, args=["mkey"]),
    "append_lines": EndpointSchema(access=EndpointAccess.ADMIN, files=["data"]),
    "apply_change": EndpointSchema(access=EndpointAccess.ADMIN),
    "delete_change": EndpointSchema(access=EndpointAccess.ADMIN),
    "delete_lines": EndpointSchema(access=EndpointAccess.ADMIN, headers=["sending", "si", "mn"], json=["lines"]),
    "auton_scout": EndpointSchema(access=EndpointAccess.LOGIN),
    "teams_in_match": EndpointSchema(access=EndpointAccess.LOGIN, args=["mkey"]),
    "get_current_event": EndpointSchema(access=EndpointAccess.LOGIN),
    "matches_in_comp": EndpointSchema(access=EndpointAccess.LOGIN),
    "events_from_team": EndpointSchema(access=EndpointAccess.LOGIN),
    "load_event_data": EndpointSchema(access=EndpointAccess.ADMIN, headers=["event"]),
    "reset_dash": EndpointSchema(access=EndpointAccess.ADMIN),
    "clear_datain": EndpointSchema(access=EndpointAccess.ADMIN),
    "clear_db": EndpointSchema(access=EndpointAccess.ADMIN),
    "edit_yaml": EndpointSchema(access=EndpointAccess.ADMIN),
    "save_file": EndpointSchema(access=EndpointAccess.ADMIN, json=["code", "path"]),
    "save_notes": EndpointSchema(access=EndpointAccess.LOGIN, json=["team", "data", "match", "pre"]),
    "save_yaml": EndpointSchema(access=EndpointAccess.ADMIN, json=["code"]),
    "edit_app_conf_page": EndpointSchema(access=EndpointAccess.ADMIN),
    "get_app_config": EndpointSchema(access=EndpointAccess.ADMIN),
    "get_log": EndpointSchema(access=EndpointAccess.ADMIN, headers=["log"]),
    "read_log": EndpointSchema(access=EndpointAccess.ADMIN),
    "get_tba_key": EndpointSchema(access=EndpointAccess.ADMIN),
    "test_notification": EndpointSchema(access=EndpointAccess.ADMIN),
    "test_tba_key": EndpointSchema(access=EndpointAccess.ADMIN, headers=["key"]),
    "set_tba_key": EndpointSchema(access=EndpointAccess.ADMIN, json=["key"]),
    "set_tba_whook_key": EndpointSchema(access=EndpointAccess.ADMIN, json=["key"]),
    "set_creds": EndpointSchema(access=EndpointAccess.ADMIN, json=["un", "pwd", "isadmin"]),
    "calc_team_score": EndpointSchema(access=EndpointAccess.LOGIN),
    "multi_view": EndpointSchema(access=EndpointAccess.LOGIN),
    "view_picklist": EndpointSchema(access=EndpointAccess.LOGIN),
    "get_picklist": EndpointSchema(access=EndpointAccess.LOGIN),
    "make_comment": EndpointSchema(access=EndpointAccess.LOGIN),
    "update_like": EndpointSchema(access=EndpointAccess.LOGIN, headers=["list", "pick", "team", "like"]),
    "picklist": EndpointSchema(access=EndpointAccess.LOGIN),
    "save_picklist": EndpointSchema(access=EndpointAccess.LOGIN),
    "save_app_config": EndpointSchema(access=EndpointAccess.ADMIN, check_json=True),
    "dload": EndpointSchema(access=EndpointAccess.LOGIN, headers=["file"]),
    "download_folder": EndpointSchema(access=EndpointAccess.LOGIN, headers=["path"]),
    "upload_folder": EndpointSchema(access=EndpointAccess.ADMIN, headers=["folderPath"], files=["data"]),
    "test_mesh": EndpointSchema(access=EndpointAccess.ADMIN, args=["m"]),
}