#!/bin/bash
# run the flask server on port 5000, get app from scouting_app.py
mkdir -p /app/log/gunicorn

# DO NOT USE -w <NUMBER GREATER THAN 1> (all workers have seperate memory so everything kills itself)
exec gunicorn -c gunicorn-conf.py -b :5000 --log-config /app/config/logging.conf scouting_app:app
# warning: NEEDS TO BE LF AND NOT CRLF