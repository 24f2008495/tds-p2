import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).parent.absolute()

# Flask Configuration
class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    PORT = int(os.getenv('FLASK_PORT', 5000))

# Storage paths
STORAGE_DIR = BASE_DIR / 'storage'
SCRAPED_DIR = STORAGE_DIR / 'scraped'
REPORTS_DIR = STORAGE_DIR / 'reports'
DOWNLOADS_DIR = STORAGE_DIR / 'downloads'
LOGS_DIR = BASE_DIR / 'logs'

# Ensure directories exist
for directory in [STORAGE_DIR, SCRAPED_DIR, REPORTS_DIR, DOWNLOADS_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

# LLM Configuration
LLM_API_KEY = os.getenv('LLM_API_KEY')
