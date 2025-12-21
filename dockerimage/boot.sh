#!/bin/bash
# run the flask server on port 5000, get app from scouting_app.py

mkdir -p /app/log/gunicorn
exec gunicorn -b :5000 --access-logfile /app/log/gunicorn/access.log --error-logfile /app/log/gunicorn/error.log scouting_app:app

# warning: NEEDS TO BE LF AND NOT CRLF