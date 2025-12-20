#!/bin/bash
# run the flask server on port 5000, get app from scouting_app.py
exec gunicorn -b :5000 --access-logfile - --error-logfile - scouting_app:app

# warning: NEEDS TO BE LF AND NOT CRLF