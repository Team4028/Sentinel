#!/bin/bash
# run the flask server on port 5000, get app from scouting_app.py
USE_HTTPS=false
mkdir -p /app/log/gunicorn


if [ "$USE_HTTPS" -eq "true" ]; then
    mkdir -p /app/secrets
    if [ ! -f "/app/secrets/sentinel-key.pem" ]; then
        python src/app.py sign
    fi
    exec gunicorn --certfile=/app/secrets/certinel.pem --keyfile=/app/secrets/sentinel-key.pem -b :443 --log-config /app/config/logging.conf scouting_app:app
else
    exec gunicorn -b :5000 --log-config /app/config/logging.conf scouting_app:app
fi
# warning: NEEDS TO BE LF AND NOT CRLF