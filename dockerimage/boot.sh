#!/bin/bash
mkdir datain
mkdir dataout
exec gunicorn -b :5000 --access-logfile - --error-logfile - scouting_app:app

# warning: NEEDS TO BE LF AND NOT CRLF