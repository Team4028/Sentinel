from flask import Flask, abort
from flask_login import LoginManager, UserMixin, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired
from functools import wraps

def require_admin(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401) # unauthorized
            if not current_user.is_admin:
                abort(403) # forbidden
            return f(*args, **kwargs)
        return decorated

class BigBrother(UserMixin):
        id = "admin"
        is_admin = True

class LoginForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])
    submit = SubmitField("Log in")

login_manager = LoginManager()
login_manager.login_view = "login"
def init_loginm_app(app: Flask):
    login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id) -> UserMixin | None:
    if user_id == "admin":
        return BigBrother()
    return None