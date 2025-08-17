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
4. Run the application
```bash
export FLASK_APP=app.py
export FLASK_ENV=production
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```


