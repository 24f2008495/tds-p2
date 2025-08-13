from langfuse.openai import OpenAI
from config import LLM_API_KEY
from file_manager import file_manager
import json
from typing import Any, Dict, List
import logging

class FormatterAgent:
    def __init__(self):
        self.model = "gpt-4.1"
        self.client = OpenAI(api_key=LLM_API_KEY)
        self.logger = logging.getLogger(__name__)

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
                    "langfuse_tags": ["formatter-agent"]
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

    # Removed - now using file_manager.convert_files_in_response()

    def format(self, question: str, instructions: str, parameters: List[Any]):
        """Format the final output based on question, instructions, and available data"""
        self.logger.info("Formatting final output...")
        self.logger.info(f"Question: {question}")
        self.logger.info(f"Instructions: {instructions}")
        self.logger.info(f"Parameters: {len(parameters) if parameters else 0}")
        self.logger.info("--------------------------------")
        
        try:
            # Handle parameters - could be analysis results, data, etc.
            if not parameters or len(parameters) == 0:
                return {"status": "error", "error": "No parameters provided for formatting"}
            
                         # Get the main data (usually analysis results)
            main_data = parameters[0] if len(parameters) > 0 else {}
            
            # Check if original question asks for JSON array format
            json_array_requested = "JSON array" in question.lower() or "json array" in question
            
            # Filter data for LLM using file manager
            filtered_data = file_manager.filter_data_for_llm(main_data)
            
            # Create prompt for formatting
            system_prompt = f"""You are an expert at formatting data analysis results into clear, well-structured responses. Your task is to format the analysis results according to the specific requirements in the question and instructions.

FORMATTING GUIDELINES:
1. Answer the question directly and completely
2. Present results in the exact format requested (JSON array, numbered list, table, etc.)
3. Include all requested information (numbers, titles, correlations, etc.)
4. Handle file references properly - they will be converted to base64 automatically
5. Be precise with numbers (use appropriate decimal places)
6. Structure the response clearly and professionally

FILE HANDLING INSTRUCTIONS:
- Analysis results may contain filenames instead of actual file content (e.g., "graph_20241201_143022_456_scatterplot.png")
- When you see a filename in the data, include it directly in your response
- The system will automatically convert filenames to base64 data URIs after formatting
- For images specifically, include the filename exactly as provided in the data

{"JSON ARRAY FORMAT DETECTED:" if json_array_requested else "GENERAL FORMAT:"}
{"- Return ONLY a valid JSON array of strings containing the answers in order" if json_array_requested else "- Follow the exact format specified in the question"}
{"- Each answer should be a string, even numbers should be converted to strings" if json_array_requested else "- Include filenames where files are referenced"}
{"- For images, include the filename as provided (e.g., 'graph_20241201_143022_456_scatterplot.png')" if json_array_requested else "- Be concise but complete"}

IMPORTANT: 
- If the question asks for a JSON array, return ONLY the JSON array with no other text
- If the question asks for specific format, follow it exactly
- Include filenames exactly as they appear in the analysis results
- Files will be automatically converted to proper format (base64 for images) after formatting
- Be precise and complete

OUTPUT FORMAT: Return the final formatted response exactly as requested in the question."""

            user_prompt = f"""
Original Question: {question}

Formatting Instructions: {instructions}

Analysis Results Available:
{json.dumps(filtered_data, indent=2)}

Please format this data into the exact response format requested in the original question. Pay special attention to the specific output format requested (JSON array, numbered list, etc.).
"""

            prompt = {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt
            }
            
            # Get formatted response from LLM
            formatted_response = self._get_response(prompt)
            
            if not formatted_response:
                return {"status": "error", "error": "Failed to generate formatted response"}
            
            # Convert file references to base64 data URIs for final output using file manager
            final_response = file_manager.convert_files_in_response(main_data, formatted_response)
            
            # Check if response is too long and might cause issues
            if len(final_response) > 50000:
                self.logger.warning(f"Response is very long ({len(final_response)} chars), might cause transmission issues")
            
            # Try to validate JSON if it looks like JSON
            if final_response.strip().startswith(('{', '[')):
                try:
                    json.loads(final_response)
                    self.logger.info("JSON validation passed")
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON validation failed: {e}")
                    # Try to fix common JSON issues
                    if not final_response.strip().endswith(('}', ']')):
                        if final_response.strip().startswith('{'):
                            final_response = final_response.strip() + '}'
                        elif final_response.strip().startswith('['):
                            final_response = final_response.strip() + ']'
                        self.logger.info("Attempted to fix incomplete JSON")
            
            return {"status": "success", "data": final_response}
            
        except Exception as e:
            self.logger.error(f"Error in formatting: {str(e)}")
            return {"status": "error", "error": str(e)}