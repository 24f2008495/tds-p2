from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import logging
import time
import json
from pathlib import Path
from agents.orchestrator import OrchestratorAgent
# Import our custom modules
from config import LOGS_DIR
import os

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

def validate_and_fix_base64_urls(data):
    """
    Validate and fix malformed base64 URLs that might have duplicate prefixes.
    This prevents issues with test frameworks that incorrectly process base64 data.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str):
                # Check for various malformed base64 patterns
                if value.startswith("data:image/png;base64,data:image/png;base64,"):
                    # Fix duplicate PNG prefix
                    fixed_value = value.replace("data:image/png;base64,data:image/png;base64,", "data:image/png;base64,")
                    data[key] = fixed_value
                    logger.warning(f"Fixed duplicate PNG base64 prefix in key '{key}'")
                elif value.startswith("data:image/jpeg;base64,data:image/jpeg;base64,"):
                    # Fix duplicate JPEG prefix
                    fixed_value = value.replace("data:image/jpeg;base64,data:image/jpeg;base64,", "data:image/jpeg;base64,")
                    data[key] = fixed_value
                    logger.warning(f"Fixed duplicate JPEG base64 prefix in key '{key}'")
                elif value.startswith("data:image/") and "data:image/" in value[11:]:
                    # Generic fix for any duplicate image prefix
                    first_colon = value.find(":", 11)  # Find second colon after "data:image/"
                    if first_colon != -1:
                        fixed_value = value[:first_colon] + value[first_colon:].replace("data:image/", "", 1)
                        data[key] = fixed_value
                        logger.warning(f"Fixed duplicate image base64 prefix in key '{key}'")
                elif value.startswith("data:") and value.count("data:") > 1:
                    # Generic fix for any duplicate data URI prefix
                    first_semicolon = value.find(";")
                    if first_semicolon != -1:
                        prefix = value[:first_semicolon + 1]
                        if prefix in value[first_semicolon + 1:]:
                            fixed_value = value.replace(prefix, "", 1)
                            data[key] = fixed_value
                            logger.warning(f"Fixed duplicate data URI prefix in key '{key}'")
            elif isinstance(value, (dict, list)):
                # Recursively check nested structures
                validate_and_fix_base64_urls(value)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                validate_and_fix_base64_urls(item)
    return data

def validate_base64_integrity(data):
    """
    Validate that base64 data URIs are properly formatted and contain valid base64 data.
    This ensures the API returns only valid base64 strings.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and value.startswith("data:"):
                try:
                    # Check if it's a valid data URI format
                    if ";" not in value or "," not in value:
                        logger.error(f"Invalid data URI format in key '{key}': missing separator")
                        continue
                    
                    # Extract the base64 part
                    header, base64_data = value.split(",", 1)
                    
                    # Validate base64 data
                    if base64_data:
                        # Try to decode a small portion to validate base64
                        test_data = base64_data[:100]  # Test first 100 chars
                        try:
                            import base64 as base64_module
                            base64_module.b64decode(test_data + "=" * (-len(test_data) % 4))
                        except Exception as e:
                            logger.error(f"Invalid base64 data in key '{key}': {str(e)}")
                            # Remove the invalid base64 string
                            data[key] = f"[INVALID_BASE64: {str(e)}]"
                    
                except Exception as e:
                    logger.error(f"Error validating base64 in key '{key}': {str(e)}")
            elif isinstance(value, (dict, list)):
                # Recursively check nested structures
                validate_base64_integrity(value)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                validate_base64_integrity(item)
    return data

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
                        return jsonify(result), 500
                else:
                    # Success case
                    elapsed_time = time.time() - start_time
                    logger.info(f"Request completed successfully in {elapsed_time:.2f} seconds after {retry_count + 1} attempts")
                    
                    # Log response size for debugging
                    result_str = str(result)
                    logger.info(f"Result length: {len(result_str)} characters")
                    
                    if len(result_str) > 100000:
                        logger.warning(f"Response is very large ({len(result_str)} chars), may cause issues")
                    
                    # Validate and fix any malformed base64 URLs before returning
                    if isinstance(result, dict):
                        result = validate_and_fix_base64_urls(result)
                        result = validate_base64_integrity(result)
                        logger.info("Validated and fixed base64 URLs in response")
                    
                    # If result is a JSON string, parse and return directly (generic approach)
                    if isinstance(result, str) and result.strip().startswith('{'):
                        try:
                            parsed_result = json.loads(result)
                            # Validate and fix base64 URLs in parsed result
                            parsed_result = validate_and_fix_base64_urls(parsed_result)
                            parsed_result = validate_base64_integrity(parsed_result)
                            logger.info("Parsed JSON result, validated base64 URLs, returning directly")
                            response = jsonify(parsed_result)
                            response.headers['X-Base64-Validated'] = 'true'
                            return response
                        except json.JSONDecodeError:
                            logger.warning("Failed to parse result as JSON, returning wrapped")
                    
                    # Add validation header for direct dict responses
                    response = jsonify(result)
                    response.headers['X-Base64-Validated'] = 'true'
                    return response
                    
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