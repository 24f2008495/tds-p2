from langfuse.openai import OpenAI
import json
import os
from config import LLM_API_KEY
from agents.analysis import AnalysisAgent
from agents.formatter import FormatterAgent
from agents.scraper import ScraperAgent
from file_manager import file_manager
from urllib.parse import urlparse
import logging

class OrchestratorAgent:
    def __init__(self):
        self.model = "gpt-4.1"
        self.client = OpenAI(api_key=LLM_API_KEY)
        self.logger = logging.getLogger(__name__)

        self.context = {
            "current_data": None, # contains the current data model is using for analysis (will also contain the data from the additional files and results from scraping (if scrape tool is used))
            "analysis_results": None, # contains the analysis results after data_analysis tool is used
            "final_output": None # contains the final output after format_final_output tool is used (this is the final answer to the question to be returned to the user)
        }
        
        # Available tools for the agent
        self.available_tools = {
            "scrape": {
                "description": "Scrapes data from a website and returns it in a structured format",
                "parameters": {"url": "string"},
                "returns": "structured data from the website"
            },
            "data_analysis": {
                "description": "Analyzes data, performs statistical analysis, and creates visualizations. Supports DuckDB for SQL queries on remote datasets (S3, parquet files). Returns analysis results and graphs as base64 images",
                "parameters": {"data": "any"},
                "returns": "dict with analysis details and base64 encoded graphs"
            },
            "format_final_output": {
                "description": "Formats the final output for API response in expected format",
                "parameters": {"data": "any"},
                "returns": "formatted final output"
            }
        }

    def _get_prompt(self, question: str, additional_files: dict = None):
        """Returns a structured prompt for the OpenAI completions API"""
        if additional_files is None:
            additional_files = {}
        
        system_prompt = f"""You are an intelligent task orchestrator for a data analysis task that decides the next action based on the question, current context, and available tools.

AVAILABLE TOOLS:
{json.dumps(self.available_tools, indent=2)}

INSTRUCTIONS:
1. Analyze the question and current context
2. Choose the MOST APPROPRIATE single tool to use next to answer the question entirely
3. Return ONLY a JSON object with the following structure:
   {{
     "reasoning": "Brief explanation of why this tool was chosen",
     "instructions": "Detailed instructions for the chosen tool",
     "tool_name": "name of the chosen tool",
     "tool_parameter": "parameters for the chosen tool(if any)"
   }}

4. Only choose ONE tool per response
5. Consider the conversation flow and current context
6. If you need new data that is not in the current context, use scrape
7. If you have data but need analysis, use data_analysis (This also does visualization, graph generation, etc.)
8. If you have all the information you need to finally answer the question, use format_final_output

IMPORTANT PARAMETER USAGE:
- The question and additional files are ALWAYS automatically sent to every tool, so empty context is perfectly fine
- Use tool_parameter ONLY when the tool specifically needs data from context (like "context.current_data" or "context.analysis_results")
- If the tool only needs the question, use an empty string "" or "context.current_data" as appropriate

CRITICAL DATA HANDLING GUIDELINES:
1. BEFORE choosing scrape for large datasets (>1GB, >100K records, or academic/research datasets):
    - Think logically about whether scraping is realistic and appropriate
    - Large government datasets, academic repositories, or APIs often provide direct access methods
    - Questions involving "big data" (court records, financial data, census data) likely reference existing datasets
    - If a question mentions specific dataset URLs or sources, check if it's meant to be accessed directly rather than scraped
    - Consider if the data is already publicly available in structured formats (parquet, CSV, SQL databases)
2. For questions involving remote datasets or databases, prefer data_analysis over scrape when:
    - The dataset is mentioned by name and is publicly accessible
    - URLs point to structured data files (parquet, CSV, JSON)
    - The question involves SQL-like operations or large-scale analytics
    - DuckDB can handle the data source directly (S3, parquet files, etc.)

EXAMPLES:
Question: "What has been the trend in Apple's stock price over the past year?"
Context: {{}}
Answer:
{{
  "reasoning": "The question asks for stock price trends, but we don't have the data yet",
  "instructions": "Please scrape historical stock price data for AAPL from Yahoo Finance for the past 12 months",
  "tool_name": "scrape", 
  "tool_parameter": "https://finance.yahoo.com/quote/AAPL/history?p=AAPL"
}}

Question: "What has been the trend in Apple's stock price over the past year?"
Context: {{
    "current_data": [
      {{"date": "2023-01-01", "price": 150.23}},
      {{"date": "2023-02-01", "price": 155.45}},
      // ... more price data
    ],
}}
Answer:
{{
  "reasoning": "We have raw stock data and need to analyze the trends and create visualizations",
  "instructions": "Calculate monthly average prices, volatility, identify price trends, and create a line chart showing stock price trends with 30-day moving average",
  "tool_name": "data_analysis",
  "tool_parameter": "context.current_data",
}}

Question: "What has been the trend in Apple's stock price over the past year?"
Context: {{
    "current_data": [
      {{"date": "2023-01-01", "price": 150.23}},
      {{"date": "2023-02-01", "price": 155.45}}
    ],
    "analysis_results": {{
      "avg_price": 152.84,
      "volatility": 12.3,
      "trend": "upward",
      "graph": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAA..."
    }}
}}
Answer:
{{
  "reasoning": "We have all the analysis and visualizations needed to answer the question",
  "instructions": "Format a response summarizing the stock price trends, key statistics, and include the generated graph",
  "tool_name": "format_final_output",
  "tool_parameter": "context.analysis_results",
}}

Question: "Analyze the sales data from the company database and create a monthly trend graph"
Context: {{}}
Answer:
{{
  "reasoning": "This question involves accessing a structured dataset that should be queried directly rather than scraped. The database can be accessed via DuckDB.",
  "instructions": "Use DuckDB to connect to the database and query sales data. Analyze monthly trends and create visualizations. The question contains all necessary information.",
  "tool_name": "data_analysis",
  "tool_parameter": ""
}}
"""
        # Convert context values to informative summaries for display
        display_context = {}
        for key, value in self.context.items():
            if key == "current_data" and isinstance(value, list):
                # For data lists, show count and sample
                if len(value) > 0:
                    sample_items = value[:2]  # Show first 2 items
                    remaining = len(value) - 2
                    if remaining > 0:
                        display_context[key] = f"[{len(value)} items total] Sample: {sample_items} + {remaining} more items"
                    else:
                        display_context[key] = f"[{len(value)} items total] {value}"
                else:
                    display_context[key] = "[]"
            elif isinstance(value, list):
                # For other lists, show count and brief content
                if len(value) > 3:
                    display_context[key] = f"[{len(value)} items] {value[:3]}... + {len(value)-3} more"
                else:
                    display_context[key] = str(value)
            elif isinstance(value, str):
                # For strings, truncate but show if truncated
                if len(value) > 300:
                    display_context[key] = value[:300] + f"... (total: {len(value)} chars)"
                else:
                    display_context[key] = value
            else:
                # For other types, convert to string with reasonable limit
                str_value = str(value)
                if len(str_value) > 300:
                    display_context[key] = str_value[:300] + f"... (total: {len(str_value)} chars)"
                else:
                    display_context[key] = str_value
        
        # Process additional files information for prompt with previews
        files_info = {}
        if additional_files:
            for filename, file_obj in additional_files.items():
                try:
                    # Reset file pointer and get size
                    file_obj.seek(0, 2)  # Seek to end
                    file_size = file_obj.tell()
                    file_obj.seek(0)  # Reset to beginning
                    
                    file_type = filename.split('.')[-1] if '.' in filename else "unknown"
                    
                    # Get content preview for readable files
                    content_preview = ""
                    if filename.lower().endswith(('.csv', '.txt', '.json', '.tsv')):
                        try:
                            content = file_obj.read(500).decode('utf-8', errors='ignore')
                            lines = content.split('\n')[:3]
                            content_preview = '\n'.join(lines) + ('...' if len(lines) >= 3 else '')
                            file_obj.seek(0)  # Reset again
                        except:
                            content_preview = "Text file (content not readable)"
                    elif filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                        content_preview = f"Image file ({file_size} bytes)"
                    else:
                        content_preview = f"Binary file ({file_size} bytes)"
                    
                    files_info[filename] = {
                        "type": file_type,
                        "size": file_size,
                        "available": True,
                        "preview": content_preview
                    }
                except Exception as e:
                    files_info[filename] = {"type": "unknown", "available": False, "error": str(e)}

        user_prompt = f"""
        Based on the question and the current context, decide what tool to use next and return the structured JSON response. 
        
        CONTEXT:
        ```
        {json.dumps(display_context, indent=2)}
        ```

        CURRENT QUESTION:
        ```
        {question}
        ```
        
        ADDITIONAL FILES PROVIDED:
        ```
        {json.dumps(files_info, indent=2) if files_info else "None"}
        ```

        ONLY GIVE JSON RESPONSE, NO OTHER TEXT. DO NOT INCLUDE ANY MARKDOWN FORMATTING OF THE JSON RESPONSE."""

        return {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt
        }
    
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
                    "langfuse_tags": ["orchestrator-agent"]
                }
            )
            if response and response.choices and len(response.choices) > 0:
                return response.choices[0].message.content
            else:
                self.logger.error("Error: Empty response from OpenAI API")
                return None
        except Exception as e:
            self.logger.error(f"Error calling OpenAI API: {e}")
            return None

    def process_question(self, question: str, additional_files: dict = None):
        """Process a question and return the next action decision"""
        if additional_files is None:
            additional_files = {}

        analysis_agent = AnalysisAgent()
        formatter_agent = FormatterAgent()
        scraper_agent = ScraperAgent()

        # Main agent loop
        while True:
            # Get the structured prompt
            try:
                prompt = self._get_prompt(question, additional_files)
                response = self._get_response(prompt)
                try:
                    response_json = json.loads(response)
                except json.JSONDecodeError:
                    raise Exception("Failed to parse response as JSON")
                
                # Execute the tool
                tool_name = response_json["tool_name"]
                instructions = response_json["instructions"]
                output = None

                # Set the parameter to the context data or context
                def build_parameter(parameters: str):
                    parameters = parameters.split(" ")
                    parameter_list = []
                    for parameter in parameters:
                        if parameter == "context.current_data":
                            parameter_list.append(self.context["current_data"])
                        elif parameter == "context.analysis_results":
                            parameter_list.append(self.context["analysis_results"])
                        else:
                            parameter_list.append(parameter)
                    return parameter_list

                parameters = build_parameter(response_json["tool_parameter"])

                if tool_name == "scrape":
                    if len(parameters) > 1:
                        raise Exception("Scrape tool can only take one parameter")
                    if not urlparse(parameters[0]).scheme:
                        raise Exception("Parameter is not a valid URL")
                    output = scraper_agent.scrape(question, instructions, parameters[0])
                    if output and output.get("status") == "success":
                        self.context["current_data"] = output["data"]
                    else:
                        if output:
                            error_msg = output.get("error", f"Scraper failed with status: {output.get('status', 'unknown')}")
                            self.logger.error(f"Scraper error details: {output}")
                        else:
                            error_msg = "Scraper returned no output"
                        raise Exception(f"Scraper failed: {error_msg}")
                elif tool_name == "data_analysis":
                    # Process additional files using file manager
                    file_mapping = file_manager.get_files_for_analysis(additional_files)
                    
                    output = analysis_agent.analyze(question, instructions, parameters, file_mapping)
                    if output and output.get("status") == "success":
                        # Process any generated files in the results
                        processed_results = file_manager.process_analysis_results_files(output["data"])
                        self.context["analysis_results"] = processed_results
                    else:
                        if output:
                            error_msg = output.get("error", f"Analysis failed with status: {output.get('status', 'unknown')}")
                            self.logger.error(f"Analysis error details: {output}")
                        else:
                            error_msg = "Analysis agent returned no output"
                        raise Exception(f"Analysis failed: {error_msg}")
                elif tool_name == "format_final_output":
                    output = formatter_agent.format(question, instructions, parameters, api_mode=True)
                    if output and output.get("status") == "success":
                        self.context["final_output"] = output["data"]
                        return output["data"]
                    else:
                        if output:
                            error_msg = output.get("error", f"Formatter failed with status: {output.get('status', 'unknown')}")
                            self.logger.error(f"Formatter error details: {output}")
                        else:
                            error_msg = "Formatter agent returned no output"
                        raise Exception(f"Formatter failed: {error_msg}")
                else:
                    self.logger.error(f"Tool {tool_name} not found")
                    raise Exception(f"Tool {tool_name} not found")
            except Exception as e:
                self.logger.error(f"Error in orchestrator agent: {e}")
                return {
                    "status": "error",
                    "error": str(e),
                    "context": self.context
                }

    
    