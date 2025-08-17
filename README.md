# Data Analysis Agent

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the Application

```bash
export FLASK_APP=app.py
export FLASK_ENV=development
flask run
```

To test run:
```bash
curl "http://127.0.0.1:5000/api/" -F "file=@question.txt"
```


# Deployment
1. SSH into the server
```bash
ssh -i tds-iitm.pem dhruva@20.193.136.77
```
2. Clone the repo
3. Add the .env file

(also install chromium sudo apt-get install -y chromium-browser)

4. Run the application
```bash
export FLASK_APP=app.py
export FLASK_ENV=production
/home/dhruva/tds-p2/venv/bin/gunicorn app:app \
  -k sync \
  -w 2 \
  -b 0.0.0.0:5000 \
  --timeout 420 \
  --graceful-timeout 60 \
  --keep-alive 10 \
  --worker-tmp-dir /dev/shm \
  --max-requests 200 \
  --max-requests-jitter 50 \
  --log-level info
```

Or with service
```bash
sudo systemctl daemon-reload
sudo systemctl enable flaskapp
sudo systemctl start flaskapp
sudo systemctl status flaskapp
```

To see logs:
```bash
sudo journalctl -u flaskapp -f
```

Caddy log:
```bash
sudo journalctl -u caddy -f
AND
also in /var/log/caddy
```
