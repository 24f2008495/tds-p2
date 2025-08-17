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
from langfuse.openai import OpenAI
from config import LLM_API_KEY
from file_manager import file_manager
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
                # Langfuse tagging
                metadata={
                    "langfuse_tags": ["analysis-agent"]
                }
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

    def _generate_analysis_code(self, question: str, instructions: str, data_structure: str, sample_data: Any, file_mapping: dict = None) -> str:
        """Generate Python code to perform the requested analysis"""
        if file_mapping is None:
            file_mapping = {}
        
        system_prompt = """You are a Python code generation expert specializing in data analysis. Your task is to generate Python code that performs the requested analysis on the given data.

Key requirements:
1. The code should be complete and executable
2. Use pandas, numpy, matplotlib for analysis and visualization
3. For DuckDB queries, use the available duckdb_conn connection object
4. For visualizations, save as PNG files using plt.savefig() and return the filename
5. Handle data type conversions (like removing '$' and 'T' from monetary values)
6. The code should store results in a variable called 'analysis_results'
7. For images, store the saved filename in the results (not base64)
8. Be robust to data variations and missing values
9. Use proper statistical methods for correlations and analysis
10. When finding specific items from data, return the actual data values (e.g., names, titles, IDs) not descriptive text like "Item at index X"

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

MATPLOTLIB GUIDELINES: Save plots as files and return filenames:
- Create plots using matplotlib
- Save plots using file_manager.save_generated_file() with the image bytes
- Example: 
  ```
  fig, ax = plt.subplots()
  # ... create plot ...
  buffer = BytesIO()
  plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
  image_bytes = buffer.getvalue()
  plt.close()
  saved_filename = file_manager.save_generated_file("scatterplot.png", image_bytes, "graph")
  ```
- Close figures with plt.close() to prevent memory leaks
- Return ONLY the saved filename in analysis_results (never base64 data)

CRITICAL: NEVER include base64 data in analysis_results. Always save images as files and return only filenames.

RESULT FORMAT: The final result MUST be stored in a variable called 'analysis_results' as a DICTIONARY with descriptive keys. For images, store ONLY the filename:
analysis_results = {
    "key1": actual_value_from_data,  # MUST be actual values from the data, NOT index references
    "key2": another_value,
    "correlation": correlation_value,
    "visualization": saved_filename  # FILENAME ONLY, NOT BASE64
}

CRITICAL FOR DATA VALUES: When returning specific items from the data (e.g., names, titles, categories), always return the ACTUAL VALUE from the data, not an index, position, or reference like "Item at index 3". Extract the actual data value from the appropriate column/field.

Available libraries: pandas, numpy, matplotlib, seaborn, re, base64, io, duckdb (via duckdb_conn), file_manager, json, datetime, networkx (as nx), scipy (with stats), sklearn (with linear_model, metrics), plotly, statsmodels

ADDITIONAL FILES ACCESS:
- Additional files are saved and their paths are available in the 'file_mapping' dictionary
- Use file_manager.get_file_path(file_mapping['original_name']) to get the full path
- Example: df = pd.read_csv(file_manager.get_file_path(file_mapping['data.csv']))
- Available files: """ + str(list(file_mapping.keys()) if file_mapping else "None") + """
"""

        # Get file previews for context
        file_previews = {}
        for original_name, saved_name in file_mapping.items():
            try:
                file_path = file_manager.get_file_path(saved_name)
                if file_path and original_name.lower().endswith(('.csv', '.txt', '.json', '.tsv')):
                    # Read first few lines for preview
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = []
                        for i, line in enumerate(f):
                            if i >= 3:  # Only first 3 lines
                                break
                            lines.append(line.strip())
                        file_previews[original_name] = '\n'.join(lines)
            except Exception as e:
                file_previews[original_name] = f"Error reading preview: {str(e)}"

        user_prompt = f"""
Question: {question}

Instructions: {instructions}

Data Structure: {data_structure}

Sample Data (first few items): {str(sample_data)[:1000]}

Additional Files Available: {list(file_mapping.keys()) if file_mapping else "None"}
File Mapping: {file_mapping}

File Previews:
{json.dumps(file_previews, indent=2) if file_previews else "No additional files"}

Generate Python code that:
1. Processes the data (data is available in variable 'data')
2. Use additional files as needed (access via file_manager.get_file_path(file_mapping['filename']))
3. Performs the requested analysis
4. Stores results in 'analysis_results' dictionary
5. For visualizations, save using file_manager.save_generated_file() and store filename in results
6. Handle data cleaning robustly - remove '$', commas, and any non-numeric prefixes/suffixes from monetary values
7. Convert string numbers to appropriate data types safely
8. CRITICAL: When returning specific data items, extract the ACTUAL VALUES from the data columns, NOT index references like "Item at index 3"

CRITICAL REMINDER: Return ONLY the raw Python code. NO markdown formatting, NO explanations, NO code blocks.

The code should be ready to execute with the data variable and file_mapping dictionary already available.

REMINDER: For any visualizations/plots:
1. Create the plot using matplotlib
2. Save to BytesIO buffer: buffer = BytesIO(); plt.savefig(buffer, format='png'); image_bytes = buffer.getvalue()
3. Use file_manager.save_generated_file(filename, image_bytes, "graph") to save
4. Store ONLY the returned filename in analysis_results
5. NEVER store base64 data in analysis_results
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
        """Process analysis results - simplified version that just handles numpy types"""
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

    # Remove the old _save_additional_files method - now handled by file_manager

    def _execute_analysis_code(self, code: str, data: Any, file_mapping: dict = None) -> Dict:
        """Safely execute the generated analysis code"""
        if file_mapping is None:
            file_mapping = {}
            
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
                'analysis_results': {},
                'file_mapping': file_mapping,  # Make file mapping available to the code
                'file_manager': file_manager,  # Make file manager available
                'os': __import__('os'),  # Make os module available
                'json': __import__('json'),  # JSON module
                'datetime': __import__('datetime'),  # Datetime module
            }
            
            # Add optional libraries that might be used
            try:
                exec_globals['networkx'] = __import__('networkx')
                exec_globals['nx'] = __import__('networkx')
            except ImportError:
                pass
            
            try:
                exec_globals['scipy'] = __import__('scipy')
                from scipy import stats
                exec_globals['stats'] = stats
            except ImportError:
                pass
            
            try:
                exec_globals['sklearn'] = __import__('sklearn')
                from sklearn import linear_model, metrics
                exec_globals['linear_model'] = linear_model
                exec_globals['metrics'] = metrics
            except ImportError:
                pass
            
            try:
                exec_globals['plotly'] = __import__('plotly')
            except ImportError:
                pass
            
            try:
                exec_globals['statsmodels'] = __import__('statsmodels')
            except ImportError:
                pass
            
            # Execute the generated code
            exec(clean_code, exec_globals)
            
            # Close any open matplotlib figures to prevent memory leaks
            plt.close('all')
            
            # Get the results
            results = exec_globals.get('analysis_results', {})
            
            if not results:
                return {"status": "error", "error": "No analysis_results found in executed code"}
            
            # Process results to handle numpy types
            processed_results = self._process_analysis_results(results)
            
            return {"status": "success", "data": processed_results}
            
        except Exception as e:
            return {"status": "error", "error": f"Error executing analysis code: {str(e)}"}

    def analyze(self, question: str, instructions: str, parameter: Any, file_mapping: dict = None):
        """Main analysis method that coordinates the entire analysis process"""
        if file_mapping is None:
            file_mapping = {}
            
        print("Analyzing data...")
        print("Question:")
        print(question)
        print("Instructions:")  
        print(instructions)
        print("Parameter type:", type(parameter))
        print("File mapping:", file_mapping)
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
            
            # Step 2: Generate analysis code (include file info in the prompt)
            sample_data = data[:3] if isinstance(data, list) and len(data) > 3 else data
            analysis_code = self._generate_analysis_code(question, instructions, data_structure, sample_data, file_mapping)
            
            if not analysis_code:
                return {"status": "error", "error": "Failed to generate analysis code"}
            
            # Step 3: Execute the analysis code
            execution_result = self._execute_analysis_code(analysis_code, data, file_mapping)
            
            if execution_result["status"] == "error":
                return execution_result
            
            analysis_results = execution_result["data"]
            
            return {"status": "success", "data": analysis_results}
            
        except Exception as e:
            print(f"Error in analysis: {str(e)}")
            return {"status": "error", "error": str(e)}