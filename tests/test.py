#!/usr/bin/env python3
"""
Testbench for the API endpoint that tests all 5 question scenarios.
Each test case correlates with a specific question file and expected response format.
"""

import requests
import json
import time
import os
import sys
import argparse
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

# Test case definitions
TEST_CASES = {
    1: {
        "name": "Wikipedia Film Analysis",
        "emoji": "📊",
        "description": "Expected: JSON array with film analysis answers and base64 image",
        "question_file": "question1.txt",
        "additional_files": None
    },
    2: {
        "name": "Indian High Court Judgments", 
        "emoji": "⚖️",
        "description": "Expected: JSON object with court analysis and base64 image",
        "question_file": "question2.txt",
        "additional_files": None
    },
    3: {
        "name": "Weather Data Analysis",
        "emoji": "🌤️", 
        "description": "Expected: JSON object with weather stats and base64 charts",
        "question_file": "question3.txt",
        "additional_files": {'sample-weather.csv': 'sample-weather.csv'}
    },
    4: {
        "name": "Sales Data Analysis",
        "emoji": "💰",
        "description": "Expected: JSON object with sales stats and base64 charts", 
        "question_file": "question4.txt",
        "additional_files": {'sample-sales.csv': 'sample-sales.csv'}
    },
    5: {
        "name": "Network Analysis",
        "emoji": "🕸️",
        "description": "Expected: JSON object with network stats and base64 charts",
        "question_file": "question5.txt", 
        "additional_files": {'edges.csv': 'edges.csv'}
    }
}

def run_single_test(test_number):
    """Run a single test case by number."""
    if test_number not in TEST_CASES:
        print(f"❌ Invalid test number: {test_number}")
        print(f"Available tests: {list(TEST_CASES.keys())}")
        return None
        
    test_case = TEST_CASES[test_number]
    
    print(f"\n{test_case['emoji']} Test Case {test_number}: {test_case['name']}")
    print(test_case['description'])
    
    # Prepare additional files if needed
    additional_files = None
    if test_case['additional_files']:
        additional_files = {}
        for filename, filepath in test_case['additional_files'].items():
            full_path = TEST_FILES_DIR / filepath
            if full_path.exists():
                additional_files[filename] = full_path
            else:
                print(f"⚠️ Warning: Additional file not found: {full_path}")
    
    # Run the test
    result = send_request(
        TEST_FILES_DIR / test_case['question_file'],
        additional_files
    )
    
    # Display result
    status = "✅ PASS" if result.get('status') == 'success' else "❌ FAIL"
    print(f"\nResult: {status}")
    
    # Print full output
    print(f"\n{'='*60}")
    print("📄 FULL TEST OUTPUT:")
    print(f"{'='*60}")
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        if 'details' in result:
            print(f"Details: {result['details']}")
    else:
        # Pretty print the full result
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    print(f"{'='*60}")
    
    return result

def run_test_suite(test_numbers=None):
    """Run test cases. If test_numbers is None, run all tests."""
    
    if test_numbers is None:
        test_numbers = list(TEST_CASES.keys())
    
    print("🚀 Starting API Test Suite")
    print(f"API Endpoint: {API_BASE_URL}")
    print(f"Test Files Directory: {TEST_FILES_DIR}")
    print(f"Running tests: {test_numbers}")
    
    test_results = []
    
    for test_num in test_numbers:
        if test_num in TEST_CASES:
            result = run_single_test(test_num)
            if result is not None:
                test_results.append((test_num, TEST_CASES[test_num]['name'], result))
        else:
            print(f"❌ Skipping invalid test number: {test_num}")
    
    # Summary Report
    if len(test_results) > 1:
        print(f"\n{'='*80}")
        print("📋 TEST SUITE SUMMARY")
        print(f"{'='*80}")
        
        for test_num, test_name, result in test_results:
            status = "✅ PASS" if result.get('status') == 'success' else "❌ FAIL"
            print(f"Test {test_num} - {test_name}: {status}")
            if 'error' in result:
                print(f"  Error: {result['error']}")
        
        print(f"\n{'='*80}")
        print("🎯 Test Suite Complete!")
        print(f"{'='*80}")

def list_tests():
    """List all available tests."""
    print("📋 Available Tests:")
    print("="*50)
    for num, test_case in TEST_CASES.items():
        print(f"{num}. {test_case['emoji']} {test_case['name']}")
        print(f"   {test_case['description']}")
        if test_case['additional_files']:
            print(f"   Additional files: {list(test_case['additional_files'].keys())}")
        print()

def interactive_mode():
    """Interactive mode for selecting tests."""
    while True:
        print("\n" + "="*60)
        print("🎯 API Test Suite - Interactive Mode")
        print("="*60)
        list_tests()
        print("Options:")
        print("  - Enter test number(s) (e.g., '1', '1,3,5', '1-3')")
        print("  - 'all' or 'a' to run all tests")
        print("  - 'list' or 'l' to list tests again")
        print("  - 'quit' or 'q' to exit")
        
        choice = input("\nEnter your choice: ").strip().lower()
        
        if choice in ['quit', 'q', 'exit']:
            print("👋 Goodbye!")
            break
        elif choice in ['list', 'l']:
            continue
        elif choice in ['all', 'a']:
            run_test_suite()
        else:
            # Parse test numbers
            try:
                test_numbers = parse_test_numbers(choice)
                if test_numbers:
                    run_test_suite(test_numbers)
                else:
                    print("❌ No valid test numbers provided")
            except ValueError as e:
                print(f"❌ Invalid input: {e}")

def parse_test_numbers(input_str):
    """Parse test numbers from input string."""
    test_numbers = []
    
    # Split by comma
    parts = [part.strip() for part in input_str.split(',')]
    
    for part in parts:
        if '-' in part:
            # Handle ranges like "1-3"
            try:
                start, end = map(int, part.split('-'))
                test_numbers.extend(range(start, end + 1))
            except ValueError:
                raise ValueError(f"Invalid range format: {part}")
        else:
            # Handle single numbers
            try:
                test_numbers.append(int(part))
            except ValueError:
                raise ValueError(f"Invalid number: {part}")
    
    # Remove duplicates and sort
    return sorted(list(set(test_numbers)))

def main():
    """Main function to handle command line arguments."""
    global API_BASE_URL
    
    parser = argparse.ArgumentParser(
        description="API Test Suite - Choose which tests to run",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test.py                    # Interactive mode
  python test.py --all              # Run all tests
  python test.py --test 1           # Run test 1 only
  python test.py --test 1,3,5       # Run tests 1, 3, and 5
  python test.py --test 1-3         # Run tests 1, 2, and 3
  python test.py --list             # List all available tests
        """
    )
    
    parser.add_argument('--test', '-t', type=str, help='Test number(s) to run (e.g., "1", "1,3,5", "1-3")')
    parser.add_argument('--all', '-a', action='store_true', help='Run all tests')
    parser.add_argument('--list', '-l', action='store_true', help='List all available tests')
    parser.add_argument('--url', '-u', type=str, default=API_BASE_URL, help=f'API base URL (default: {API_BASE_URL})')
    
    args = parser.parse_args()
    
    # Update API URL if provided
    API_BASE_URL = args.url
    
    if args.list:
        list_tests()
        return
    
    if args.all:
        run_test_suite()
        return
    
    if args.test:
        try:
            test_numbers = parse_test_numbers(args.test)
            if test_numbers:
                run_test_suite(test_numbers)
            else:
                print("❌ No valid test numbers provided")
                list_tests()
        except ValueError as e:
            print(f"❌ Invalid test input: {e}")
            list_tests()
        return
    
    # No arguments provided - start interactive mode
    interactive_mode()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️ Test suite interrupted by user")
    except Exception as e:
        print(f"\n💥 Test suite failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
