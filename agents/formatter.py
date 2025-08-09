from openai import OpenAI
from config import LLM_API_KEY
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
                temperature=0.1
            )
            if response and response.choices and len(response.choices) > 0:
                return response.choices[0].message.content
            else:
                self.logger.error("Error: Empty response from OpenAI API")
                return None
        except Exception as e:
            self.logger.error(f"Error calling OpenAI API: {e}")
            return None

    def _filter_data_for_llm(self, data: Any) -> Any:
        """Filter out base64 image data and other large/irrelevant data for LLM processing"""
        if isinstance(data, dict):
            filtered_data = {}
            for key, value in data.items():
                # Skip internal formatting keys
                if key.startswith("_"):
                    continue
                elif isinstance(value, str) and value.startswith("data:image/"):
                    # Replace base64 image with placeholder
                    filtered_data[key] = f"[IMAGE_DATA_AVAILABLE: {key}]"
                elif isinstance(value, str) and len(value) > 1000:
                    # Truncate very long strings
                    filtered_data[key] = value[:500] + f"... [TRUNCATED: {len(value)} total chars]"
                else:
                    filtered_data[key] = self._filter_data_for_llm(value)
            
            # If this was originally a list format, mention that
            if data.get("_original_format") == "list" and "_list_data" in data:
                filtered_data["_note"] = "Original analysis returned as list - converting to structured format"
                
            return filtered_data
        elif isinstance(data, list):
            return [self._filter_data_for_llm(item) for item in data]
        else:
            return data

    def _preserve_original_data(self, original_data: Any, formatted_response: str) -> str:
        """Replace image placeholders in the formatted response with actual image data"""
        import re
        
        # Find all base64 image data URIs in the original data
        image_data_uris = []
        
        if isinstance(original_data, dict):
            # Check regular dict keys
            for key, value in original_data.items():
                if isinstance(value, str) and value.startswith("data:image/"):
                    image_data_uris.append(value)
            
            # Check _list_data if it exists
            if "_list_data" in original_data:
                for item in original_data["_list_data"]:
                    if isinstance(item, str) and item.startswith("data:image/"):
                        image_data_uris.append(item)
        
        self.logger.info(f"Found {len(image_data_uris)} image data URIs to restore")
        
        # If we have image data to restore
        for i, image_uri in enumerate(image_data_uris):
            # Escape quotes in the image URI for JSON safety
            escaped_uri = image_uri.replace('"', '\\"')
            
            # Replace exact placeholder first
            placeholder_patterns = [
                r'\[IMAGE_DATA_AVAILABLE: [^\]]+\]',
                r'"?\[Image saved as: storage/analysis_image_[^"]+"?"?',
                r'"?Image generated and saved as a base64-encoded data URI[^"]*"?',
                r'"?\[Image: [^\]]+\]"?',
                r'"?Image saved as: storage/[^"]*"?',
                r'"[^"]*\[Image saved as: storage/[^"]*"',
                r'"[^"]*Image generated[^"]*"'
            ]
            
            replaced = False
            for pattern in placeholder_patterns:
                if re.search(pattern, formatted_response):
                    # Always use proper JSON string format
                    formatted_response = re.sub(pattern, f'"{escaped_uri}"', formatted_response)
                    replaced = True
                    self.logger.info(f"Replaced pattern {pattern} with image URI {i+1}")
                    break
            
            if not replaced:
                self.logger.warning(f"No placeholder found for image URI {i+1}")
                            
        return formatted_response

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
            
            # Filter data for LLM (remove base64 images, etc.)
            filtered_data = self._filter_data_for_llm(main_data)
            
            self.logger.info("Filtered data for LLM:")
            self.logger.info(filtered_data)
            self.logger.info(f"JSON Array Requested: {json_array_requested}")
            self.logger.info("--------------------------------")
            
            # Create prompt for formatting
            system_prompt = f"""You are an expert at formatting data analysis results into clear, well-structured responses. Your task is to format the analysis results according to the specific requirements in the question and instructions.

FORMATTING GUIDELINES:
1. Answer the question directly and completely
2. Present results in the exact format requested (JSON array, numbered list, table, etc.)
3. Include all requested information (numbers, titles, correlations, etc.)
4. If images were requested, note that they have been generated and saved
5. Be precise with numbers (use appropriate decimal places)
6. Structure the response clearly and professionally

{"JSON ARRAY FORMAT DETECTED:" if json_array_requested else "GENERAL FORMAT:"}
{"- Return ONLY a valid JSON array of strings containing the answers in order" if json_array_requested else "- Follow the exact format specified in the question"}
{"- Each answer should be a string, even numbers should be converted to strings" if json_array_requested else "- Include image references where appropriate but not the actual base64 data"}
{"- For images, include the placeholder text as the array element" if json_array_requested else "- Be concise but complete"}

IMPORTANT: 
- If the question asks for a JSON array, return ONLY the JSON array with no other text
- If the question asks for specific format, follow it exactly
- Replace image data URIs with descriptive text about the saved image
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
            
            self.logger.info("Raw LLM Response:")
            self.logger.info(formatted_response)
            self.logger.info("--------------------------------")
            
            # Restore any image references in the response
            self.logger.info("Before restoration:")
            self.logger.info(f"Formatted response length: {len(formatted_response)}")
            self.logger.info(f"Has base64 data in original: {any(isinstance(v, str) and v.startswith('data:image/') for v in (main_data.values() if isinstance(main_data, dict) else []))}")
            
            final_response = self._preserve_original_data(main_data, formatted_response)
            
            self.logger.info("After restoration:")
            self.logger.info(f"Final response length: {len(final_response)}")
            
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
            
            self.logger.info("Final Formatted Response (first 500 chars):")
            self.logger.info(final_response[:500] + "..." if len(final_response) > 500 else final_response)
            self.logger.info("--------------------------------")
            
            return {"status": "success", "data": final_response}
            
        except Exception as e:
            self.logger.error(f"Error in formatting: {str(e)}")
            return {"status": "error", "error": str(e)}