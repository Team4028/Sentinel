import os
import re

from flask import Flask, abort, request
from flask_login import LoginManager, UserMixin, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired
from functools import wraps
import sqlite3

def text_slug(text: str):
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s-]+', '-', text)
    return text.strip('-')


def require_admin(f):
    """Ensures that the user is logged into an admin account on the modifiee endpoint"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)  # unauthorized
        if not current_user.is_admin:
            abort(403)  # forbidden
        return f(*args, **kwargs)

    return decorated

def require_json(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not request.is_json:
            return "Error, Content-Type must be application/json", 415
        return f(*args, **kwargs)
    return decorated


class 人(UserMixin):
    """ A user """
    def __init__(self, id, un, pwd, isadmin):
        super().__init__()
        self.id = id
        self.un = un
        self.pwd = pwd
        self.is_admin = (True if isadmin == 1 else False)

class LoginForm(FlaskForm):
    """Form used on the login page to log in"""
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])
    submit = SubmitField("Log in")


login_manager = LoginManager()
login_manager.login_view = "login"

@login_manager.user_loader
def get_user_from_db(uid) -> 人 | None:
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, un, pw, isadmin FROM logins WHERE id=?", (uid,))
        rows = cursor.fetchall()
        if len(rows) > 0: return 人(*rows[0])
        else: return None

def override_get_admin():
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, un, pw, isadmin FROM logins WHERE isadmin=1")
        rows = cursor.fetchall()
        if len(rows) > 0: return 人(*rows[0])
        else: return None

def get_user_from_db_unpw(un, pwd) -> 人 | None:
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, un, pw, isadmin FROM logins WHERE un=? AND pw=?", (un, pwd))
        rows = cursor.fetchall()
        if len(rows) > 0: return 人(*rows[0])
        else: return None


def init_loginm_app(app: Flask) -> None:
    """Registers a login manager for admin to the flask app"""
    login_manager.init_app(app)


def add_user_to_db(username, password, is_admin=False):
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        conn.execute("INSERT OR REPLACE INTO logins (id, un, pw, isadmin) VALUES (?, ?, ?, ?)", (
            text_slug(username),
            username,
            password,
            is_admin
        ))

def get_password_is_admin(password):
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT isadmin FROM logins WHERE pw=?", (password,))
        rows = cursor.fetchall()
        if len(rows) > 0: return rows[0] == 1
        else: return False

def get_table_exists():
    if not os.path.exists(os.path.join("secrets", "logins.db")): return False
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tablenames = cursor.fetchall()
        if len(tablenames) <= 0: return False
        cursor.execute("SELECT isadmin FROM logins")
        return any([x == 1 for x in cursor.fetchall()]) # make sure there's an admin

def generate_login_db(un, pwd):
    if get_table_exists(): return
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        conn.executescript(f"""
            CREATE TABLE IF NOT EXISTS logins (
                id TEXT PRIMARY KEY,
                un TEXT,
                pw TEXT,
                isadmin INT
            );
        """)

    add_user_to_db(un, pwd, True)