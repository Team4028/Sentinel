import os
import random
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

ADMIN_COLOR = "#b000f0"

USER_PALATTE = [
    "#ff9500",
    "#00d5ff",
    "#00f0a0",
    "#f03000",
    "#f00098",
    "#f0d000"
]

def display_user(id: str):
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logins WHERE id=?", (id,))
        rows = cursor.fetchall()
        if len(rows) > 0: user = 人(*rows[0])
        else: return f""
    return f"""
        <style>
            .user-pill {{
                display: inline-flex;
                align-items: center;
                gap: 8px;

                padding: 6px 12px;
                border-radius: 999px;
                background: #444;
                font-size: 14px;
            }}

            .avatar {{
                width: 24px;
                height: 24px;
                border-radius: 50%;
                text-align: center;
                font-size: 20px;
                line-height: 24px;
            }}

            .name {{
                white-space: nowrap;
            }}
        </style>
        <div class="user-pill">
            <div class="avatar" style="background-color: {user.color};">{user.un[0].upper()}</div>
            <span class="name">{user.un}</span>
        </div>
    """

def generate_random_color(isadmin) -> str:
    return  ADMIN_COLOR if isadmin else USER_PALATTE[random.randint(0, len(USER_PALATTE) - 1)]


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


class 人(UserMixin):
    """ A user """
    def __init__(self, id, un, pwd, isadmin, color):
        super().__init__()
        self.id = id
        self.un = un
        self.pwd = pwd
        self.is_admin = (True if isadmin == 1 else False)
        self.color = color

class LoginForm(FlaskForm):
    """Form used on the login page to log in"""
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])
    submit = SubmitField("Log in")

class CreateAccount(FlaskForm):
    """Form used on the login page to log in"""
    username = StringField("Username", validators=[DataRequired()], render_kw={ "placeholder": "Enter username (ie. 'Frodo Baggins')..." })
    password = PasswordField("Password", validators=[DataRequired()], render_kw={ "placeholder": "Enter password..." })
    submit = SubmitField("Create Account")


login_manager = LoginManager()
login_manager.login_view = "login"

@login_manager.user_loader
def get_user_from_db(uid) -> 人 | None:
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logins WHERE id=?", (uid,))
        rows = cursor.fetchall()
        if len(rows) > 0: return 人(*rows[0])
        else: return None

def get_user_is_admin(user: UserMixin):
    if isinstance(user, 人):
        return user.is_admin
    return False

def override_get_admin():
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logins WHERE isadmin=1")
        rows = cursor.fetchall()
        if len(rows) > 0: return 人(*rows[0])
        else: return None

def get_user_from_db_unpw(un, pwd) -> 人 | None:
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logins WHERE un=? AND pw=?", (un, pwd))
        rows = cursor.fetchall()
        if len(rows) > 0: return 人(*rows[0])
        else: return None


def init_loginm_app(app: Flask) -> None:
    """Registers a login manager for admin to the flask app"""
    login_manager.init_app(app)


def add_user_to_db(username, password, is_admin=False):
    with sqlite3.connect(os.path.join("secrets", "logins.db")) as conn:
        conn.execute("INSERT OR REPLACE INTO logins (id, un, pw, isadmin, color) VALUES (?, ?, ?, ?, ?)", (
            text_slug(username),
            username,
            password,
            is_admin,
            generate_random_color(is_admin)
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
                isadmin INT,
                color TEXT
            );
        """)

    add_user_to_db(un, pwd, True)