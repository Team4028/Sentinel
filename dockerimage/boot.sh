#!/bin/bash
# run the flask server on port 5000, get app from scouting_app.py
mkdir -p /app/log/gunicorn
mkdir -p /app/secrets

if [ ! -f "/app/secrets/sentinel-key.pem" ]; then
    python src/app.py sign
fi

exec gunicorn --certfile=/app/secrets/certinel.pem --keyfile=/app/secrets/sentinel-key.pem -b :443 --log-config /app/config/logging.conf scouting_app:app

# warning: NEEDS TO BE LF AND NOT CRLF