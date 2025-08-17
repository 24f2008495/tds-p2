#!/bin/bash
cd /home/dhruva/tds-p2

# Activate virtualenv
source venv/bin/activate

# Export environment variables
export FLASK_APP=app.py
export FLASK_ENV=production   # use production, not development, for systemd

# Start with gunicorn (4 workers, bound to port 5000)
exec gunicorn -w 4 -b 0.0.0.0:5000 app:app