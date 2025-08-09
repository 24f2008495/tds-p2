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
