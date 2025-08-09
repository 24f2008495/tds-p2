import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64
import re
from typing import Any, Dict, List
import warnings
from openai import OpenAI
from config import LLM_API_KEY
import duckdb

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

class AnalysisAgent:
    def __init__(self):
        self.model = "gpt-4.1"
        self.client = OpenAI(api_key=LLM_API_KEY)
        # Initialize DuckDB connection
        self.duckdb_conn = duckdb.connect(':memory:')
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Initialize DuckDB with necessary extensions and configurations"""
        try:
            # Install and load commonly needed extensions
            self.duckdb_conn.execute("INSTALL httpfs;")
            self.duckdb_conn.execute("LOAD httpfs;")
            self.duckdb_conn.execute("INSTALL parquet;")
            self.duckdb_conn.execute("LOAD parquet;")
            print("DuckDB initialized with httpfs and parquet extensions")
        except Exception as e:
            print(f"Warning: Could not initialize some DuckDB extensions: {e}")

    def _execute_duckdb_query(self, query: str) -> pd.DataFrame:
        """Execute a DuckDB query and return results as a pandas DataFrame"""
        try:
            result = self.duckdb_conn.execute(query).df()
            return result
        except Exception as e:
            raise Exception(f"DuckDB query failed: {str(e)}")

    def _get_response(self, prompt: dict):
        """Get a response from the OpenAI API"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt["system_prompt"]}, 
                    {"role": "user", "content": prompt["user_prompt"]}
                ],
                temperature=0.1
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error calling OpenAI API: {e}")
            return None

    def _analyze_data_structure(self, data: Any) -> str:
        """Analyze the structure of the input data to understand what we're working with"""
        if isinstance(data, list) and len(data) > 0:
            # If it's a list, analyze the first few items
            sample_size = min(3, len(data))
            sample_data = data[:sample_size]
            
            if isinstance(sample_data[0], dict):
                # List of dictionaries - get the keys from first item
                keys = list(sample_data[0].keys())
                structure = f"List of {len(data)} dictionaries with keys: {keys}"
                
                # Add sample values to help understand data types
                sample_values = {}
                for key in keys:
                    sample_values[key] = [item.get(key, None) for item in sample_data]
                
                structure += f"\nSample values: {sample_values}"
                return structure
            else:
                return f"List of {len(data)} items. Sample: {sample_data}"
        
        elif isinstance(data, dict):
            keys = list(data.keys())
            return f"Dictionary with keys: {keys}"
        
        elif isinstance(data, str):
            return f"String data (length: {len(data)}): {data[:200]}..."
        
        else:
            return f"Data type: {type(data)}, Value: {str(data)[:200]}"

    def _generate_analysis_code(self, question: str, instructions: str, data_structure: str, sample_data: Any) -> str:
        """Generate Python code to perform the requested analysis"""
        
        system_prompt = """You are a Python code generation expert specializing in data analysis. Your task is to generate Python code that performs the requested analysis on the given data.

Key requirements:
1. The code should be complete and executable
2. Use pandas, numpy, matplotlib for analysis and visualization
3. For DuckDB queries, use the available duckdb_conn connection object
4. For visualizations, return base64 encoded PNG images as data URIs
5. Handle data type conversions (like removing '$' and 'T' from monetary values)
6. The code should store results in a variable called 'analysis_results'
7. For images, store them as base64 data URIs in the results
8. Be robust to data variations and missing values
9. Use proper statistical methods for correlations and analysis

CRITICAL: Return ONLY plain Python code. Do NOT use markdown code blocks (```python or ```). Do NOT include any explanatory text, comments outside the code, or formatting. Just return the raw Python code that can be executed directly.

DUCKDB USAGE: When working with remote datasets or SQL-like operations:
- Use duckdb_conn.execute(query).df() to run DuckDB queries and get pandas DataFrames
- DuckDB supports S3 URLs, parquet files, and various data sources
- Extensions httpfs and parquet are already loaded
- Example: df = duckdb_conn.execute("SELECT * FROM read_parquet('s3://bucket/file.parquet')").df()

DATA CLEANING GUIDELINES: Always handle data type conversions robustly:
- Use pd.to_numeric(df['column'], errors='coerce') for converting to numbers safely
- Use df['column'].astype(str).str.replace(r'[^\\d.]', '', regex=True) to extract numbers from strings
- Handle missing values with df.dropna() or df.fillna() as appropriate
- Convert dates with pd.to_datetime(df['column'], errors='coerce')

MATPLOTLIB GUIDELINES: Always close figures properly:
- Use plt.close() or plt.close('all') after saving plots to prevent memory leaks
- For multiple plots, use plt.figure() to create new figures and close each one

RESULT FORMAT: The final result MUST be stored in a variable called 'analysis_results' as a DICTIONARY with descriptive keys, not a list or array. For example:
analysis_results = {
    "movies_2bn_before_2020": count_value,
    "earliest_film_over_1_5bn": film_title,
    "correlation_rank_peak": correlation_value,
    "scatterplot_image": image_data_uri
}

Available libraries: pandas, numpy, matplotlib, seaborn, re, base64, io, duckdb (via duckdb_conn)
"""

        user_prompt = f"""
Question: {question}

Instructions: {instructions}

Data Structure: {data_structure}

Sample Data (first few items): {str(sample_data)[:1000]}

Generate Python code that:
1. Processes the data (data is available in variable 'data')
2. Performs the requested analysis
3. Stores results in 'analysis_results' dictionary
4. For visualizations, convert to base64 data URI format
5. Handle data cleaning robustly - remove '$', commas, and any non-numeric prefixes/suffixes from monetary values
6. Convert string numbers to appropriate data types safely

CRITICAL REMINDER: Return ONLY the raw Python code. NO markdown formatting, NO explanations, NO code blocks.

The code should be ready to execute with the data variable already available.
"""

        prompt = {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt
        }
        
        return self._get_response(prompt)

    def _extract_python_code(self, response: str) -> str:
        """Extract Python code from markdown code blocks"""
        import re
        
        # Remove any leading/trailing whitespace
        response = response.strip()
        
        # Try to find code blocks with ```python or ``` or just ```
        python_code_patterns = [
            r'```python\s*\n(.*?)\n```',
            r'```\s*\n(.*?)\n```',
            r'```(?:python)?\s*(.*?)```'
        ]
        
        for pattern in python_code_patterns:
            matches = re.findall(pattern, response, re.DOTALL)
            if matches:
                # Return the first code block found
                return matches[0].strip()
        
        # If no code blocks found, try to extract code after common phrases
        code_start_patterns = [
            r'Here\'s the complete code:\s*\n(.*)',
            r'Here is the code:\s*\n(.*)',
            r'Complete code:\s*\n(.*)',
            r'Python code:\s*\n(.*)',
            r'^import\s+.*',  # Starts with import
            r'^from\s+.*',    # Starts with from
            r'^#.*\n(.*)',    # Starts with comment
        ]
        
        for pattern in code_start_patterns:
            match = re.search(pattern, response, re.DOTALL | re.MULTILINE)
            if match:
                if len(match.groups()) > 0:
                    return match.group(1).strip()
                else:
                    return match.group(0).strip()
        
        # If the response looks like Python code (contains imports, assignments, etc.)
        if any(keyword in response for keyword in ['import ', 'from ', '=', 'def ', 'class ', 'if ', 'for ', 'while ']):
            return response
        
        # If still no match, return the original response
        return response

    def _process_analysis_results(self, results) -> Dict:
        """Process analysis results to handle image data properly"""
        import os
        
        # Handle both dict and list formats
        if isinstance(results, list):
            # Convert list to dict with indices as keys
            dict_results = {f"result_{i}": value for i, value in enumerate(results)}
        elif isinstance(results, dict):
            dict_results = results.copy()
        else:
            # Single value, wrap in dict
            dict_results = {"result": results}
        
        processed_results = {}
        
        for key, value in dict_results.items():
            if isinstance(value, str) and value.startswith("data:image/"):
                # This is a base64 image data URI
                try:
                    # Extract the base64 data
                    header, base64_data = value.split(",", 1)
                    
                    # Save to storage directory (relative to project root)
                    os.makedirs("storage", exist_ok=True)
                    image_filename = f"analysis_image_{key}_{hash(base64_data) % 10000}.png"
                    image_path = f"storage/{image_filename}"
                    
                    # Decode and save the image
                    import base64
                    image_bytes = base64.b64decode(base64_data)
                    with open(image_path, "wb") as f:
                        f.write(image_bytes)
                    
                    # Store the full data URI but print truncated version
                    processed_results[key] = value
                    print(f"Image saved to: {image_path}")
                    print(f"Base64 data (first 50 chars): {value[:50]}...")
                    
                except Exception as e:
                    print(f"Error processing image for {key}: {e}")
                    processed_results[key] = value
            else:
                # Convert numpy types to Python native types
                if hasattr(value, 'item'):  # numpy scalars have .item() method
                    processed_results[key] = value.item()
                elif isinstance(value, np.ndarray):
                    processed_results[key] = value.tolist()
                else:
                    processed_results[key] = value
        
        # If original was a list, also preserve the list format
        if isinstance(results, list):
            processed_results["_original_format"] = "list"
            processed_results["_list_data"] = [processed_results[f"result_{i}"] for i in range(len(results))]
                
        return processed_results

    def _execute_analysis_code(self, code: str, data: Any) -> Dict:
        """Safely execute the generated analysis code"""
        try:
            # Extract Python code from markdown if needed
            clean_code = self._extract_python_code(code)
            
            # Set up the execution environment with necessary imports and data
            exec_globals = {
                'data': data,
                'pd': pd,
                'np': np,
                'plt': plt,
                'sns': sns,
                'BytesIO': BytesIO,
                'base64': base64,
                're': re,
                'duckdb_conn': self.duckdb_conn,
                'analysis_results': {}
            }
            
            # Execute the generated code
            exec(clean_code, exec_globals)
            
            # Close any open matplotlib figures to prevent memory leaks
            plt.close('all')
            
            # Get the results
            results = exec_globals.get('analysis_results', {})
            
            if not results:
                return {"status": "error", "error": "No analysis_results found in executed code"}
            
            # Process results to handle images properly
            processed_results = self._process_analysis_results(results)
            
            return {"status": "success", "data": processed_results}
            
        except Exception as e:
            return {"status": "error", "error": f"Error executing analysis code: {str(e)}"}

    def analyze(self, question: str, instructions: str, parameter: Any):
        """Main analysis method that coordinates the entire analysis process"""
        print("Analyzing data...")
        print("Question:")
        print(question)
        print("Instructions:")  
        print(instructions)
        print("Parameter type:", type(parameter))
        print("--------------------------------")
        
        try:
            # Handle the parameter - it could be a list with data
            if isinstance(parameter, list) and len(parameter) > 0:
                data = parameter[0]  # Take the first parameter as the data
            else:
                data = parameter
            
            # Step 1: Analyze the data structure
            data_structure = self._analyze_data_structure(data)
            print("Data Structure Analysis:")
            print(data_structure)
            print("--------------------------------")
            
            # Step 2: Generate analysis code
            sample_data = data[:3] if isinstance(data, list) and len(data) > 3 else data
            analysis_code = self._generate_analysis_code(question, instructions, data_structure, sample_data)
            
            if not analysis_code:
                return {"status": "error", "error": "Failed to generate analysis code"}
            
            print("Generated Analysis Code:")
            print(analysis_code)
            print("--------------------------------")
            
            # Step 3: Execute the analysis code
            execution_result = self._execute_analysis_code(analysis_code, data)
            
            if execution_result["status"] == "error":
                return execution_result
            
            analysis_results = execution_result["data"]
            print("Analysis Results:")
            print(analysis_results)
            print("--------------------------------")
            
            return {"status": "success", "data": analysis_results}
            
        except Exception as e:
            print(f"Error in analysis: {str(e)}")
            return {"status": "error", "error": str(e)}