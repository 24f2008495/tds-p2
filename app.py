from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import logging
import time
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

@app.route('/api/', methods=['POST'])
def api_file_upload():
    """
    Main API endpoint that accepts file uploads containing analysis requests.
    Expected format: curl "https://app.example.com/api/" -F "questions.txt=@question.txt" -F "image.png=@image.png" -F "data.csv=@data.csv"
    questions.txt will ALWAYS be sent and contain the questions. There may be zero or more additional files passed.
    """
    
    # Initialize orchestrator agent (new for each request)
    orchestrator = OrchestratorAgent()

    # Start timer for this request
    start_time = time.time()
    max_retry_time = 4 * 60  # 4 minutes in seconds
    
    try:
        # Check if questions.txt was uploaded (this is required)
        if 'questions.txt' not in request.files:
            return jsonify({"error": "questions.txt file is required. Please upload a text file with your questions."}), 400
        
        questions_file = request.files['questions.txt']
        
        # Check if questions.txt was selected
        if questions_file.filename == '':
            return jsonify({"error": "questions.txt file not selected"}), 400
        
        # Check if questions.txt is a text file
        if not questions_file.filename.lower().endswith(('.txt', '.text')):
            return jsonify({"error": "questions.txt must be a text file (.txt)"}), 400
        
        # Read the questions file content
        try:
            questions_content = questions_file.read().decode('utf-8').strip()
        except UnicodeDecodeError:
            return jsonify({"error": "Unable to read questions.txt. Please ensure it's a valid text file."}), 400
        
        if not questions_content:
            return jsonify({"error": "questions.txt file is empty"}), 400
        
        # Collect additional files (optional)
        additional_files = {}
        for filename, file_obj in request.files.items():
            if filename != 'questions.txt' and file_obj.filename != '':
                additional_files[filename] = file_obj
        
        logger.info(f"Received questions file: {questions_file.filename}")
        logger.info(f"Questions content: {questions_content}")
        logger.info(f"Additional files: {list(additional_files.keys())}")
        
        # Retry logic for orchestrator
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Check if we've exceeded the time limit
                elapsed_time = time.time() - start_time
                if elapsed_time >= max_retry_time:
                    logger.error(f"Request exceeded {max_retry_time} seconds, returning error")
                    return jsonify({
                        "status": "error",
                        "error": f"Request processing exceeded time limit of {max_retry_time} seconds"
                    }), 408
                
                # Process the question with additional files
                result = orchestrator.process_question(questions_content, additional_files)
                
                # Check if the orchestrator returned an error
                if isinstance(result, dict) and result.get("status") == "error":
                    elapsed_time = time.time() - start_time
                    if elapsed_time < max_retry_time and retry_count < max_retries - 1:
                        retry_count += 1
                        logger.warning(f"Orchestrator failed (attempt {retry_count}/{max_retries}): {result.get('error')}. Retrying...")
                        continue
                    else:
                        logger.error(f"Orchestrator failed after {retry_count + 1} attempts: {result.get('error')}")
                        return jsonify(result), 500
                elif result is False:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < max_retry_time and retry_count < max_retries - 1:
                        retry_count += 1
                        logger.warning(f"Orchestrator returned False (attempt {retry_count}/{max_retries}). Retrying...")
                        continue
                    else:
                        logger.error(f"Orchestrator returned False after {retry_count + 1} attempts")
                        return jsonify({
                            "status": "error",
                            "error": "Processing failed after multiple attempts - see logs for details"
                        }), 500
                else:
                    # Success case
                    elapsed_time = time.time() - start_time
                    logger.info(f"Request completed successfully in {elapsed_time:.2f} seconds after {retry_count + 1} attempts")
                    
                    # Log response size for debugging
                    result_str = str(result)
                    logger.info(f"Result length: {len(result_str)} characters")
                    
                    if len(result_str) > 100000:
                        logger.warning(f"Response is very large ({len(result_str)} chars), may cause issues")
                    
                    return jsonify({"status": "success", "result": result})
                    
            except Exception as orchestrator_error:
                elapsed_time = time.time() - start_time
                if elapsed_time < max_retry_time and retry_count < max_retries - 1:
                    retry_count += 1
                    logger.warning(f"Orchestrator exception (attempt {retry_count}/{max_retries}): {str(orchestrator_error)}. Retrying...")
                    continue
                else:
                    logger.error(f"Orchestrator exception after {retry_count + 1} attempts: {str(orchestrator_error)}")
                    return jsonify({
                        "status": "error",
                        "error": f"Processing failed after multiple attempts: {str(orchestrator_error)}"
                    }), 500
        
        # If we get here, we've exhausted all retries
        elapsed_time = time.time() - start_time
        logger.error(f"Exhausted all {max_retries} retries after {elapsed_time:.2f} seconds")
        return jsonify({
            "status": "error",
            "error": f"Processing failed after {max_retries} attempts within time limit"
        }), 500
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Error in api_file_upload endpoint after {elapsed_time:.2f} seconds: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500