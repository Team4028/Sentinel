#!/bin/bash
# run the flask server on port 5000, get app from scouting_app.py
mkdir -p /app/log/gunicorn

exec gunicorn -b :5000 --log-config /app/config/logging.conf scouting_app:app
# warning: NEEDS TO BE LF AND NOT CRLF