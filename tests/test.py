#!/usr/bin/env python3
"""
Testbench for the API endpoint that tests all 5 question scenarios.
Each test case correlates with a specific question file and expected response format.
"""

import requests
import json
import time
import os
from pathlib import Path

# Configuration
API_BASE_URL = "http://localhost:5000/api/"
TEST_FILES_DIR = Path(__file__).parent

def send_request(question_file_path, additional_files=None):
    """
    Send a request to the API endpoint with the specified files.
    
    Args:
        question_file_path (str): Path to the questions.txt file
        additional_files (dict): Optional additional files to send
    
    Returns:
        dict: Response from the API
    """
    files = {'questions.txt': open(question_file_path, 'rb')}
    
    if additional_files:
        for filename, filepath in additional_files.items():
            if os.path.exists(filepath):
                files[filename] = open(filepath, 'rb')
    
    try:
        print(f"\n{'='*60}")
        print(f"Sending request for: {question_file_path}")
        if additional_files:
            print(f"Additional files: {list(additional_files.keys())}")
        print(f"{'='*60}")
        
        start_time = time.time()
        response = requests.post(API_BASE_URL, files=files)
        end_time = time.time()
        
        # Close all file handles
        for file_obj in files.values():
            file_obj.close()
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Time: {end_time - start_time:.2f} seconds")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"Response Status: {result.get('status', 'unknown')}")
                if 'result' in result:
                    print(f"Result Type: {type(result['result']).__name__}")
                    if isinstance(result['result'], str):
                        # Try to parse as JSON if it's a string
                        try:
                            parsed_result = json.loads(result['result'])
                            print(f"Parsed Result Keys: {list(parsed_result.keys()) if isinstance(parsed_result, dict) else 'Not a dict'}")
                        except json.JSONDecodeError:
                            print(f"Result Length: {len(result['result'])} characters")
                    else:
                        print(f"Result: {result['result']}")
                return result
            except json.JSONDecodeError:
                print(f"Raw Response: {response.text[:500]}...")
                return {"error": "Invalid JSON response"}
        else:
            print(f"Error Response: {response.text}")
            return {"error": f"HTTP {response.status_code}", "details": response.text}
            
    except Exception as e:
        print(f"Request failed: {str(e)}")
        return {"error": str(e)}

def run_test_suite():
    """Run all test cases with their respective question files and additional data."""
    
    print("🚀 Starting API Test Suite")
    print(f"API Endpoint: {API_BASE_URL}")
    print(f"Test Files Directory: {TEST_FILES_DIR}")
    
    # Test Case 1: Wikipedia Film Analysis
    print("\n📊 Test Case 1: Wikipedia Film Analysis")
    print("Expected: JSON array with film analysis answers and base64 image")
    test1_result = send_request(
        TEST_FILES_DIR / "question1.txt"
    )
    
    # Test Case 2: Indian High Court Judgments
    print("\n⚖️ Test Case 2: Indian High Court Judgments")
    print("Expected: JSON object with court analysis and base64 image")
    test2_result = send_request(
        TEST_FILES_DIR / "question2.txt"
    )
    
    # Test Case 3: Weather Data Analysis
    print("\n🌤️ Test Case 3: Weather Data Analysis")
    print("Expected: JSON object with weather stats and base64 charts")
    test3_result = send_request(
        TEST_FILES_DIR / "question3.txt",
        additional_files={'sample-weather.csv': TEST_FILES_DIR / "sample-weather.csv"}
    )
    
    # Test Case 4: Sales Data Analysis
    print("\n💰 Test Case 4: Sales Data Analysis")
    print("Expected: JSON object with sales stats and base64 charts")
    test4_result = send_request(
        TEST_FILES_DIR / "question4.txt",
        additional_files={'sample-sales.csv': TEST_FILES_DIR / "sample-sales.csv"}
    )
    
    # Test Case 5: Network Analysis
    print("\n🕸️ Test Case 5: Network Analysis")
    print("Expected: JSON object with network stats and base64 charts")
    test5_result = send_request(
        TEST_FILES_DIR / "question5.txt",
        additional_files={'edges.csv': TEST_FILES_DIR / "edges.csv"}
    )
    
    # Summary Report
    print(f"\n{'='*80}")
    print("📋 TEST SUITE SUMMARY")
    print(f"{'='*80}")
    
    test_results = [
        ("Wikipedia Film Analysis", test1_result),
        ("Indian High Court Judgments", test2_result),
        ("Weather Data Analysis", test3_result),
        ("Sales Data Analysis", test4_result),
        ("Network Analysis", test5_result)
    ]
    
    for test_name, result in test_results:
        status = "✅ PASS" if result.get('status') == 'success' else "❌ FAIL"
        print(f"{test_name}: {status}")
        if 'error' in result:
            print(f"  Error: {result['error']}")
    
    print(f"\n{'='*80}")
    print("🎯 Test Suite Complete!")
    print(f"{'='*80}")

if __name__ == "__main__":
    try:
        run_test_suite()
    except KeyboardInterrupt:
        print("\n\n⏹️ Test suite interrupted by user")
    except Exception as e:
        print(f"\n💥 Test suite failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
