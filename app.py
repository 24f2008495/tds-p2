from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import logging
from pathlib import Path
from agents.orchestrator import OrchestratorAgent
# Import our custom modules
from config import LOGS_DIR
# Load environment variables from .env file
load_dotenv()

# Setup logging
def setup_logging():
    """Setup logging configuration"""
    # Ensure logs directory exists
    LOGS_DIR.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOGS_DIR / 'app.log'),
            logging.StreamHandler()
        ]
    )

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configure Flask for larger responses
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB limit
app.config['JSON_SORT_KEYS'] = False  # Preserve key order

# Enable CORS for all routes with fully open configuration
CORS(app, resources={r"/*": {"origins": "*"}})

orchestrator = OrchestratorAgent()

@app.route('/api/', methods=['POST'])
def api_file_upload():
    """
    Main API endpoint that accepts file uploads containing analysis requests.
    Expected format: curl "https://app.example.com/api/" -F "@question.txt"
    """
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded. Please upload a text file with your question."}), 400
        
        file = request.files['file']
        
        # Check if file was selected
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        # Check if it's a text file
        if not file.filename.lower().endswith(('.txt', '.text')):
            return jsonify({"error": "Please upload a text file (.txt)"}), 400
        
        # Read the file content
        try:
            question_content = file.read().decode('utf-8').strip()
        except UnicodeDecodeError:
            return jsonify({"error": "Unable to read file. Please ensure it's a valid text file."}), 400
        
        if not question_content:
            return jsonify({"error": "File is empty"}), 400
        
        logger.info(f"Received file upload: {file.filename}")
        logger.info(f"Question content: {question_content}")
        
        result = orchestrator.process_question(question_content)

        # Check if the orchestrator returned an error
        if isinstance(result, dict) and result.get("status") == "error":
            logger.error(f"Orchestrator failed: {result.get('error')}")
            return jsonify(result), 500
        elif result is False:
            logger.error("Orchestrator returned False without proper error details")
            return jsonify({
                "status": "error",
                "error": "Processing failed - see logs for details"
            }), 500
        else:
            # Log response size for debugging
            result_str = str(result)
            logger.info(f"Result length: {len(result_str)} characters")
            
            if len(result_str) > 100000:
                logger.warning(f"Response is very large ({len(result_str)} chars), may cause issues")
            
            return jsonify({"status": "success", "result": result})
        
    except Exception as e:
        logger.error(f"Error in api_file_upload endpoint: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500