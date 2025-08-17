import os
import base64
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from io import BytesIO
import json

class FileManager:
    """
    Centralized file manager for handling all file operations across agents.
    
    Key principles:
    1. All files are stored in the storage/ directory with timestamps
    2. Only reference files by their filename during processing
    3. Base64 conversion only happens at final output formatting
    4. Simple, consistent interface for all agents
    """
    
    def __init__(self, storage_dir: str = "storage"):
        self.storage_dir = storage_dir
        self._ensure_storage_dir()
    
    def _ensure_storage_dir(self):
        """Ensure the storage directory exists"""
        os.makedirs(self.storage_dir, exist_ok=True)
    
    def _generate_timestamped_filename(self, original_filename: str, prefix: str = "") -> str:
        """Generate a timestamped filename to avoid collisions"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
        name, ext = os.path.splitext(original_filename)
        safe_name = "".join(c for c in name if c.isalnum() or c in "._-")[:50]  # Sanitize and limit length
        
        if prefix:
            return f"{prefix}_{timestamp}_{safe_name}{ext}"
        else:
            return f"{timestamp}_{safe_name}{ext}"
    
    def save_uploaded_file(self, filename: str, file_obj) -> str:
        """
        Save an uploaded file (from user) to storage.
        
        Args:
            filename: Original filename
            file_obj: File object with read() method
            
        Returns:
            str: The saved filename (with timestamp)
        """
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)
            
            # Generate timestamped filename
            saved_filename = self._generate_timestamped_filename(filename, "upload")
            file_path = os.path.join(self.storage_dir, saved_filename)
            
            # Save the file
            with open(file_path, 'wb') as f:
                f.write(file_obj.read())
            
            # Reset file pointer for potential future use
            file_obj.seek(0)
            
            print(f"Saved uploaded file: {filename} -> {saved_filename}")
            return saved_filename
            
        except Exception as e:
            print(f"Error saving uploaded file {filename}: {str(e)}")
            raise
    
    def save_generated_file(self, filename: str, content: Union[bytes, str], file_type: str = "generated") -> str:
        """
        Save a generated file (from agents) to storage.
        
        Args:
            filename: Desired filename 
            content: File content (bytes or string)
            file_type: Type prefix for the file (e.g., "graph", "analysis", "generated")
            
        Returns:
            str: The saved filename (with timestamp)
        """
        try:
            # Generate timestamped filename
            saved_filename = self._generate_timestamped_filename(filename, file_type)
            file_path = os.path.join(self.storage_dir, saved_filename)
            
            # Save the file
            mode = 'wb' if isinstance(content, bytes) else 'w'
            encoding = None if isinstance(content, bytes) else 'utf-8'
            
            with open(file_path, mode, encoding=encoding) as f:
                f.write(content)
            
            print(f"Saved generated file: {filename} -> {saved_filename}")
            return saved_filename
            
        except Exception as e:
            print(f"Error saving generated file {filename}: {str(e)}")
            raise
    
    def save_image_from_base64(self, base64_data: str, filename: str = "image.png") -> str:
        """
        Save a base64 image to storage.
        
        Args:
            base64_data: Base64 encoded image data (with or without data URI prefix)
            filename: Desired filename
            
        Returns:
            str: The saved filename (with timestamp)
        """
        try:
            # Handle data URI format (data:image/png;base64,...)
            if base64_data.startswith("data:"):
                header, base64_content = base64_data.split(",", 1)
            else:
                base64_content = base64_data
            
            # Decode base64 to bytes
            image_bytes = base64.b64decode(base64_content)
            
            # Save using save_generated_file
            return self.save_generated_file(filename, image_bytes, "image")
            
        except Exception as e:
            print(f"Error saving base64 image {filename}: {str(e)}")
            raise
    
    def get_file_path(self, filename: str) -> str:
        """
        Get the full path to a file in storage.
        
        Args:
            filename: The filename (with or without timestamp prefix)
            
        Returns:
            str: Full path to the file
        """
        return os.path.join(self.storage_dir, filename)
    
    def file_exists(self, filename: str) -> bool:
        """Check if a file exists in storage"""
        return os.path.exists(self.get_file_path(filename))
    
    def list_files(self, pattern: str = None) -> List[str]:
        """
        List files in storage, optionally filtered by pattern.
        
        Args:
            pattern: Optional pattern to filter filenames
            
        Returns:
            List[str]: List of filenames
        """
        try:
            files = os.listdir(self.storage_dir)
            if pattern:
                files = [f for f in files if pattern in f]
            return sorted(files)
        except Exception as e:
            print(f"Error listing files: {str(e)}")
            return []
    
    def read_file(self, filename: str, mode: str = 'rb') -> Union[bytes, str]:
        """
        Read a file from storage.
        
        Args:
            filename: The filename to read
            mode: File mode ('rb' for binary, 'r' for text)
            
        Returns:
            File content as bytes or string
        """
        try:
            file_path = self.get_file_path(filename)
            encoding = None if 'b' in mode else 'utf-8'
            
            with open(file_path, mode, encoding=encoding) as f:
                return f.read()
                
        except Exception as e:
            print(f"Error reading file {filename}: {str(e)}")
            raise
    
    def convert_file_to_base64(self, filename: str, raw_base64: bool = False) -> str:
        """
        Convert a file to base64 data URI for final output.
        
        Args:
            filename: The filename to convert
            raw_base64: If True, return only the raw base64 data without the data URI prefix
            
        Returns:
            str: Base64 data URI string or raw base64 data
        """
        try:
            file_path = self.get_file_path(filename)
            
            # Read file as bytes
            with open(file_path, 'rb') as f:
                file_bytes = f.read()
            
            # Encode to base64
            base64_data = base64.b64encode(file_bytes).decode('utf-8')
            
            # If raw base64 requested, return just the base64 data
            if raw_base64:
                print(f"Converted {filename} to raw base64 ({len(base64_data)} chars)")
                return base64_data
            
            # Determine MIME type based on extension
            ext = os.path.splitext(filename)[1].lower()
            mime_type_map = {
                '.png': 'image/png',
                '.jpg': 'image/jpeg', 
                '.jpeg': 'image/jpeg',
                '.gif': 'image/gif',
                '.pdf': 'application/pdf',
                '.csv': 'text/csv',
                '.txt': 'text/plain',
                '.json': 'application/json'
            }
            
            mime_type = mime_type_map.get(ext, 'application/octet-stream')
            
            # Create data URI
            data_uri = f"data:{mime_type};base64,{base64_data}"
            
            print(f"Converted {filename} to base64 data URI ({len(data_uri)} chars)")
            return data_uri
            
        except Exception as e:
            print(f"Error converting file {filename} to base64: {str(e)}")
            raise
    
    def get_file_info(self, filename: str) -> Dict[str, Any]:
        """
        Get information about a file.
        
        Args:
            filename: The filename
            
        Returns:
            Dict with file information
        """
        try:
            file_path = self.get_file_path(filename)
            
            if not os.path.exists(file_path):
                return {"exists": False}
            
            stat = os.stat(file_path)
            
            return {
                "exists": True,
                "filename": filename,
                "full_path": file_path,
                "size_bytes": stat.st_size,
                "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "extension": os.path.splitext(filename)[1].lower()
            }
            
        except Exception as e:
            print(f"Error getting file info for {filename}: {str(e)}")
            return {"exists": False, "error": str(e)}
    
    def cleanup_old_files(self, max_age_hours: int = 24):
        """
        Clean up files older than specified hours.
        
        Args:
            max_age_hours: Maximum age in hours before files are deleted
        """
        try:
            import time
            
            current_time = time.time()
            cutoff_time = current_time - (max_age_hours * 3600)
            
            deleted_count = 0
            for filename in os.listdir(self.storage_dir):
                file_path = os.path.join(self.storage_dir, filename)
                if os.path.isfile(file_path):
                    if os.path.getmtime(file_path) < cutoff_time:
                        os.remove(file_path)
                        deleted_count += 1
                        print(f"Deleted old file: {filename}")
            
            print(f"Cleanup completed: {deleted_count} files deleted")
            
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")
    
    def get_files_for_analysis(self, uploaded_files: Dict[str, Any]) -> Dict[str, str]:
        """
        Process uploaded files and return a mapping of original filename to saved filename.
        This is used by the analysis agent to know what files are available.
        
        Args:
            uploaded_files: Dict of filename -> file_obj from the orchestrator
            
        Returns:
            Dict[str, str]: Mapping of original filename to saved filename
        """
        file_mapping = {}
        
        if not uploaded_files:
            return file_mapping
        
        for original_filename, file_obj in uploaded_files.items():
            try:
                saved_filename = self.save_uploaded_file(original_filename, file_obj)
                file_mapping[original_filename] = saved_filename
                print(f"Processed file for analysis: {original_filename} -> {saved_filename}")
            except Exception as e:
                print(f"Error processing file {original_filename}: {str(e)}")
                # Continue with other files
                continue
        
        return file_mapping
    
    def process_analysis_results_files(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process analysis results and save any generated files, returning filenames instead of content.
        This method handles any leftover base64 data that might have been included in results.
        
        Args:
            analysis_results: Results from analysis agent
            
        Returns:
            Dict with file content replaced by filenames
        """
        processed_results = {}
        
        for key, value in analysis_results.items():
            if isinstance(value, str) and value.startswith("data:image/"):
                # This is a base64 image - save it and replace with filename
                # NOTE: This should NOT happen if analysis agent is working correctly
                print(f"WARNING: Found base64 data in analysis results for key '{key}' - this should be a filename!")
                try:
                    # Generate a meaningful filename based on the key
                    image_filename = f"analysis_{key}.png"
                    saved_filename = self.save_image_from_base64(value, image_filename)
                    processed_results[key] = saved_filename
                    print(f"Saved analysis image: {key} -> {saved_filename}")
                except Exception as e:
                    print(f"Error saving analysis image {key}: {str(e)}")
                    # Keep original value if save fails
                    processed_results[key] = value
            elif isinstance(value, str) and self.is_filename(value):
                # This is already a filename - perfect!
                print(f"Found filename in analysis results: {key} -> {value}")
                processed_results[key] = value
            else:
                # Keep other values as-is
                processed_results[key] = value
        
        return processed_results
    
    def is_filename(self, value: str) -> bool:
        """Check if a string looks like a saved filename from our file manager"""
        if not isinstance(value, str):
            return False
        # Check if it looks like a timestamped filename from our file manager
        return (value.endswith(('.png', '.jpg', '.jpeg', '.pdf', '.csv', '.txt', '.json')) and 
                ('_' in value) and 
                len(value) > 10)
    
    def convert_files_in_response(self, data: Dict[str, Any], formatted_response: str) -> str:
        """
        Convert file references in a formatted response to base64 data URIs.
        This is the final step before sending data back to the user.
        
        Args:
            data: Original data containing file references
            formatted_response: The formatted response string
            
        Returns:
            str: Response with file references converted to base64 data URIs
        """
        import re
        
        # Find all file references in the original data
        file_references = []
        
        if isinstance(data, dict):
            # Check regular dict keys
            for key, value in data.items():
                if isinstance(value, str) and self.is_filename(value):
                    file_references.append((key, value))
            
            # Check _list_data if it exists
            if "_list_data" in data:
                for i, item in enumerate(data["_list_data"]):
                    if isinstance(item, str) and self.is_filename(item):
                        file_references.append((f"list_item_{i}", item))
        
        print(f"Converting {len(file_references)} file references to base64 for final output")
        
        # Convert each file to base64 and replace in response
        for key, filename in file_references:
            try:
                # Convert file to base64 data URI
                data_uri = self.convert_file_to_base64(filename)
                
                # Escape quotes in the data URI for JSON safety
                escaped_uri = data_uri.replace('"', '\\"')
                
                # Replace file reference placeholders
                placeholder_patterns = [
                    rf'\[FILE_AVAILABLE: {re.escape(filename)}\]',
                    rf'"{re.escape(filename)}"',
                    rf'{re.escape(filename)}'
                ]
                
                replaced = False
                for pattern in placeholder_patterns:
                    if re.search(pattern, formatted_response):
                        # Always use proper JSON string format
                        formatted_response = re.sub(pattern, f'"{escaped_uri}"', formatted_response)
                        replaced = True
                        print(f"Replaced file {filename} with base64 data URI in final output")
                        break
                
                if not replaced:
                    print(f"Warning: No placeholder found for file {filename}")
                    
            except Exception as e:
                print(f"Error converting file {filename} to base64: {e}")
                # Keep the original filename if conversion fails
                continue
                            
        return formatted_response

    def convert_files_in_response_to_raw_base64(self, data: Dict[str, Any], formatted_response: str) -> str:
        """
        Convert file references in a formatted response to raw base64 data (without data URI prefix).
        This is specifically for API responses where the test framework expects raw base64.
        
        Args:
            data: Original data containing file references
            formatted_response: The formatted response string
            
        Returns:
            str: Response with file references converted to raw base64 data
        """
        import re
        
        # Find all file references in the original data
        file_references = []
        
        if isinstance(data, dict):
            # Check regular dict keys
            for key, value in data.items():
                if isinstance(value, str) and self.is_filename(value):
                    file_references.append((key, value))
            
            # Check _list_data if it exists
            if "_list_data" in data:
                for i, item in enumerate(data["_list_data"]):
                    if isinstance(item, str) and self.is_filename(item):
                        file_references.append((f"list_item_{i}", item))
        
        print(f"Converting {len(file_references)} file references to raw base64 for API output")
        
        # Convert each file to raw base64 and replace in response
        for key, filename in file_references:
            try:
                # Convert file to raw base64 (without data URI prefix)
                raw_base64 = self.convert_file_to_base64(filename, raw_base64=True)
                
                # Escape quotes in the base64 data for JSON safety
                escaped_base64 = raw_base64.replace('"', '\\"')
                
                # Replace file reference placeholders
                placeholder_patterns = [
                    rf'\[FILE_AVAILABLE: {re.escape(filename)}\]',
                    rf'"{re.escape(filename)}"',
                    rf'{re.escape(filename)}'
                ]
                
                replaced = False
                for pattern in placeholder_patterns:
                    if re.search(pattern, formatted_response):
                        # Always use proper JSON string format
                        formatted_response = re.sub(pattern, f'"{escaped_base64}"', formatted_response)
                        replaced = True
                        print(f"Replaced file {filename} with raw base64 data in API output")
                        break
                
                if not replaced:
                    print(f"Warning: No placeholder found for file {filename}")
                    
            except Exception as e:
                print(f"Error converting file {filename} to raw base64: {e}")
                # Keep the original filename if conversion fails
                continue
                            
        return formatted_response
    
    def filter_data_for_llm(self, data: Any) -> Any:
        """
        Filter data for LLM processing - replace filenames with placeholders.
        This prepares data for the LLM by replacing file references with readable placeholders.
        
        Args:
            data: Data that may contain file references
            
        Returns:
            Filtered data with file references replaced by placeholders
        """
        if isinstance(data, dict):
            filtered_data = {}
            for key, value in data.items():
                # Skip internal formatting keys
                if key.startswith("_"):
                    continue
                elif isinstance(value, str) and self.is_filename(value):
                    # This looks like a filename - note it as an available file
                    filtered_data[key] = f"[FILE_AVAILABLE: {value}]"
                elif isinstance(value, str) and len(value) > 1000:
                    # Truncate very long strings
                    filtered_data[key] = value[:500] + f"... [TRUNCATED: {len(value)} total chars]"
                else:
                    filtered_data[key] = self.filter_data_for_llm(value)
            
            # If this was originally a list format, mention that
            if data.get("_original_format") == "list" and "_list_data" in data:
                filtered_data["_note"] = "Original analysis returned as list - converting to structured format"
                
            return filtered_data
        elif isinstance(data, list):
            return [self.filter_data_for_llm(item) for item in data]
        else:
            return data


# Global file manager instance
file_manager = FileManager()