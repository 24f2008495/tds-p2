from bs4 import BeautifulSoup
import json
import re
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import subprocess
import os
from langfuse.openai import OpenAI
from config import LLM_API_KEY
import logging

class ScraperAgent:
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
                    "langfuse_tags": ["scraper-agent"]
                },
            )
            return response.choices[0].message.content
        except Exception as e:
            self.logger.error(f"Error calling OpenAI API: {e}")
            return None

    def _get_browser_version(self, binary_path: str) -> Optional[str]:
        """Get the version of the browser binary"""
        try:
            result = subprocess.run([binary_path, '--version'], 
                                 capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                version_output = result.stdout.strip()
                # Extract version number (e.g., "Chromium 138.0.6971.118" -> "138.0.6971.118")
                import re
                version_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_output)
                if version_match:
                    version = version_match.group(1)
                    self.logger.info(f"Detected browser version: {version}")
                    return version
        except Exception as e:
            self.logger.warning(f"Could not detect browser version: {e}")
            return None

    def _get_compatible_driver(self, browser_version: str, binary_path: str) -> str:
        """Get a compatible ChromeDriver for the browser version"""
        try:
            # Extract major version
            major_version = browser_version.split('.')[0]
            self.logger.info(f"Browser major version: {major_version}")
            
            # Use cached driver if available
            cache_dir = os.path.expanduser("~/.wdm/drivers/chromedriver")
            
            # Try to get the correct driver version
            # Check if we need a specific version for newer Chromium
            if int(major_version) >= 115:
                # For newer versions, try to get latest driver
                try:
                    driver_manager = ChromeDriverManager(driver_version="latest")
                    driver_path = driver_manager.install()
                    self.logger.info(f"Using latest driver: {driver_path}")
                    return driver_path
                except Exception as e:
                    self.logger.warning(f"Could not get latest driver: {e}")
            
            # For Chromium, try to get the appropriate driver
            if 'chromium' in binary_path.lower():
                try:
                    driver_manager = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM)
                    driver_path = driver_manager.install()
                    self.logger.info(f"Using Chromium driver: {driver_path}")
                    return driver_path
                except Exception as e:
                    self.logger.warning(f"Chromium driver failed: {e}")
            
            # Default fallback
            driver_manager = ChromeDriverManager()
            driver_path = driver_manager.install()
            self.logger.info(f"Using default driver: {driver_path}")
            return driver_path
                
        except Exception as e:
            self.logger.error(f"Error getting compatible driver: {e}")
            return ChromeDriverManager().install()

    def _extract_url_from_parameter(self, parameter: str) -> str:
        """Extract URL from parameter string that may contain @ symbol"""
        url = parameter.strip()
        if url.startswith('@'):
            url = url[1:]
        return url

    def _fetch_page_content(self, url: str) -> Optional[str]:
        """Fetch page content using Selenium for maximum reliability"""
        self.logger.info(f"Fetching content from: {url}")
        self.logger.info("Using Selenium for reliable content extraction...")
        return self._fetch_with_selenium(url)

    def _fetch_with_selenium(self, url: str) -> Optional[str]:
        """Fetch page content using Selenium for dynamic content"""
        driver = None
        
        # Try multiple browser configurations
        browser_configs = [
            # Chrome with binary path
            {
                'browser': 'chrome',
                'binary_paths': [
                    '/usr/bin/google-chrome',
                    '/usr/bin/google-chrome-stable', 
                    '/usr/bin/chromium-browser',
                    '/usr/bin/chromium',
                    '/snap/bin/chromium'
                ]
            }
        ]
        
        for config in browser_configs:
            try:
                chrome_options = Options()
                chrome_options.add_argument("--headless")  # Use standard headless mode
                chrome_options.add_argument("--no-sandbox") 
                chrome_options.add_argument("--disable-dev-shm-usage") # important in servers/containers
                chrome_options.add_argument("--disable-gpu")  # harmless on Linux; needed on some setups
                chrome_options.add_argument("--disable-software-rasterizer")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument("--disable-background-timer-throttling")
                chrome_options.add_argument("--disable-backgrounding-occluded-windows")
                chrome_options.add_argument("--disable-renderer-backgrounding")
                chrome_options.add_argument("--disable-features=TranslateUI")
                chrome_options.add_argument("--disable-ipc-flooding-protection")
                chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                chrome_options.add_argument("--disable-extensions")
                chrome_options.add_argument("--disable-plugins")
                chrome_options.add_argument("--disable-default-apps")
                chrome_options.add_argument("--disable-sync")
                chrome_options.add_argument("--disable-translate")
                chrome_options.add_argument("--hide-scrollbars")
                chrome_options.add_argument("--metrics-recording-only")
                chrome_options.add_argument("--mute-audio")
                chrome_options.add_argument("--no-first-run")
                chrome_options.add_argument("--safebrowsing-disable-auto-update")
                chrome_options.add_argument("--ignore-certificate-errors")
                chrome_options.add_argument("--ignore-ssl-errors")
                chrome_options.add_argument("--ignore-certificate-errors-spki-list")
                # Create a unique user data directory to avoid conflicts
                import tempfile
                user_data_dir = tempfile.mkdtemp(prefix="chrome-user-data-")
                chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
                # Critical flags for headless server environments to fix DevToolsActivePort error
                chrome_options.add_argument("--disable-dev-tools")  # Disable DevTools
                chrome_options.add_argument("--disable-web-security")  # Disable web security for scraping
                chrome_options.add_argument("--allow-running-insecure-content")  # Allow insecure content
                chrome_options.add_argument("--disable-features=VizDisplayCompositor")  # Disable display compositor
                chrome_options.add_argument("--remote-debugging-port=0")  # Let Chrome choose the port automatically
                chrome_options.add_argument("--disable-logging")  # Reduce logging
                chrome_options.add_argument("--disable-background-networking")  # Disable background networking
                chrome_options.add_argument("--disable-component-update")  # Disable component updates
                chrome_options.add_argument("--disable-client-side-phishing-detection")  # Disable phishing detection
                chrome_options.add_argument("--disable-hang-monitor")  # Disable hang monitor
                chrome_options.add_argument("--disable-popup-blocking")  # Disable popup blocking
                chrome_options.add_argument("--disable-prompt-on-repost")  # Disable repost prompt
                chrome_options.add_argument("--disable-domain-reliability")  # Disable domain reliability
                chrome_options.add_argument("--disable-features=VizDisplayCompositor,VizServiceDisplay")  # Additional display fixes
                chrome_options.add_argument("--disable-features=AudioServiceOutOfProcess")  # Disable out-of-process audio
                chrome_options.add_argument("--disable-features=MediaRouter")  # Disable media router
                chrome_options.add_argument("--single-process")  # Run in single process mode for stability
                
                # Try to find Chrome binary
                chrome_found = False
                selected_binary = None
                browser_version = None
                
                for binary_path in config['binary_paths']:
                    try:
                        if os.path.exists(binary_path):
                            chrome_options.binary_location = binary_path
                            selected_binary = binary_path
                            chrome_found = True
                            self.logger.info(f"Using Chrome binary at: {binary_path}")
                            
                            # Get browser version
                            browser_version = self._get_browser_version(binary_path)
                            break
                    except:
                        continue
                
                if not chrome_found:
                    self.logger.info("No Chrome binary found, trying default...")
                    selected_binary = None
                
                # Get compatible driver
                if browser_version and selected_binary:
                    driver_path = self._get_compatible_driver(browser_version, selected_binary)
                else:
                    driver_path = ChromeDriverManager().install()
                
                service = Service(driver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
                
                # Remove webdriver property
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                
                # Navigate to URL
                self.logger.info(f"Navigating to: {url}")
                driver.get(url)
                
                # Wait for page to load
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Additional wait for dynamic content
                time.sleep(2)
                
                page_source = driver.page_source
                self.logger.info(f"Successfully fetched page content ({len(page_source)} characters)")
                return page_source
                
            except Exception as e:
                self.logger.error(f"Browser config failed: {e}")
                continue
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = None
                # Clean up temp user data directory if it was created
                try:
                    if 'user_data_dir' in locals() and os.path.exists(user_data_dir):
                        import shutil
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                except:
                    pass
        
        # If all configurations failed
        self.logger.error("\n" + "="*60)
        self.logger.error("BROWSER SETUP FAILED - VERSION MISMATCH DETECTED")
        self.logger.error("="*60)
        self.logger.error("Your Chromium version is too new for available ChromeDrivers.")
        self.logger.error("Let's install Google Chrome which has better driver support:")
        self.logger.error("")
        self.logger.error("Run these commands to install Google Chrome:")
        self.logger.error("wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -")
        self.logger.error("echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | sudo tee /etc/apt/sources.list.d/google-chrome.list")
        self.logger.error("sudo apt-get update && sudo apt-get install google-chrome-stable")
        self.logger.error("")
        self.logger.error("OR try this command to install a compatible Chromium version:")
        self.logger.error("sudo apt-get remove chromium-browser && sudo apt-get install google-chrome-stable")
        self.logger.error("="*60)
        
        return None

    def _analyze_page_structure(self, html_content: str, url: str) -> Dict[str, Any]:
        """Analyze the page structure and identify key elements"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Basic page analysis
        title = soup.find('title')
        title_text = title.get_text() if title else "No title found"
        
        # Find main content areas
        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content') or soup.find('div', id='content')
        
        # Look for tables (common for data lists)
        tables = soup.find_all('table')
        
        # Analyze table details with more specifics
        table_details = []
        for i, table in enumerate(tables[:10]):  # Analyze more tables but limit output
            caption = table.find('caption')
            caption_text = caption.get_text(strip=True) if caption else None
            
            # Get first few rows to understand structure
            rows = table.find_all('tr')[:3]
            sample_data = []
            for row in rows:
                cells = [cell.get_text(strip=True)[:50] for cell in row.find_all(['td', 'th'])]  # Limit cell text
                if cells:
                    sample_data.append(cells)
            
            table_info = {
                'index': i,
                'classes': table.get('class', []),
                'id': table.get('id', ''),
                'caption': caption_text,
                'row_count': len(table.find_all('tr')),
                'column_count': len(table.find_all('tr')[0].find_all(['td', 'th'])) if table.find_all('tr') else 0,
                'sample_data': sample_data
            }
            table_details.append(table_info)
        
        # Look for lists
        lists = soup.find_all(['ul', 'ol'])
        list_details = []
        for i, list_elem in enumerate(lists[:5]):  # Limit to first 5 lists
            items = list_elem.find_all('li')[:3]  # Sample first 3 items
            sample_items = [item.get_text(strip=True)[:100] for item in items]
            list_details.append({
                'index': i,
                'type': list_elem.name,
                'item_count': len(list_elem.find_all('li')),
                'sample_items': sample_items
            })
        
        # Analyze URL structure
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        path = parsed_url.path
        
        structure_analysis = {
            'title': title_text,
            'domain': domain,
            'path': path,
            'has_tables': len(tables) > 0,
            'table_count': len(tables),
            'table_details': table_details,
            'has_lists': len(lists) > 0,
            'list_count': len(lists),
            'list_details': list_details,
            'main_content_found': main_content is not None,
            'page_type': self._identify_page_type(soup, domain, path),
            'content_preview': soup.get_text()[:500] + "..." if len(soup.get_text()) > 500 else soup.get_text()
        }
        
        return structure_analysis

    def _identify_page_type(self, soup: BeautifulSoup, domain: str, path: str) -> str:
        """Identify the type of page based on content and URL"""
        text = soup.get_text().lower()
        
        if 'wikipedia' in domain:
            return 'wikipedia'
        elif any(word in text for word in ['list', 'ranking', 'top', 'best', 'chart']):
            return 'list_page'
        elif any(word in text for word in ['product', 'item', 'buy', 'price']):
            return 'product_page'
        elif any(word in text for word in ['article', 'news', 'story']):
            return 'article'
        elif soup.find_all('table'):
            return 'data_table'
        else:
            return 'general'

    def _generate_custom_scraper(self, structure_analysis: Dict[str, Any], instructions: str, html_content: str) -> str:
        """Generate a custom scraper based on page structure and instructions"""
        
        system_prompt = """You are an expert web scraping developer. Create a robust Python scraper that extracts the requested data.

CRITICAL REQUIREMENTS:
1. Be FLEXIBLE with HTML structure - extract data even if formatting is imperfect
2. Focus on tables/lists that contain the requested information
3. Return clean, structured data that can be easily queried
4. Use .get_text(strip=True) to extract clean text from cells
5. Handle malformed HTML gracefully - extract what you can
6. DO NOT be overly strict with validation - extract data if it's recognizable

RETURN FORMAT: Python function code only, no markdown, no explanations.

The function must be named `custom_scraper` and:
- Take BeautifulSoup object as input
- Return dict with 'data' (list of dicts with consistent field names) and 'debug' (extraction info)
- Be flexible and forgiving with HTML structure
- Extract readable text from cells, ignore HTML tags and references
- Return meaningful field names that can be queried"""

        # Create a more focused user prompt based on page analysis
        relevant_tables = []
        for table in structure_analysis.get('table_details', []):
            if table.get('caption') or table.get('row_count', 0) > 2:
                relevant_tables.append({
                    'index': table['index'],
                    'caption': table.get('caption'),
                    'rows': table.get('row_count'),
                    'columns': table.get('column_count'),
                    'sample': table.get('sample_data', [])[:2]  # Just first 2 rows
                })

        user_prompt = f"""
INSTRUCTIONS: {instructions}

PAGE INFO:
- Title: {structure_analysis['title']}
- Domain: {structure_analysis['domain']}
- Page Type: {structure_analysis['page_type']}

RELEVANT TABLES FOUND: {len(relevant_tables)}
{json.dumps(relevant_tables[:3], indent=2)}

LISTS FOUND: {structure_analysis.get('list_count', 0)}

HTML SAMPLE (first 2000 chars):
{html_content[:2000]}

Create a `custom_scraper(soup)` function that:
1. Finds the table/list most relevant to: {instructions}
2. Extracts ALL data from that table/list (be flexible with HTML structure)
3. Returns data as list of dictionaries with clean field names
4. Uses .get_text(strip=True) to extract clean text from cells
5. Handles any HTML formatting issues gracefully

EXAMPLE OUTPUT FORMAT:
{{
    "data": [
        {{"title": "Item Name", "value": "123", "year": "2024"}},
        {{"title": "Another Item", "value": "456", "year": "2023"}}
    ],
    "debug": {{"table_found": True, "rows_extracted": 2}}
}}

Be LIBERAL in extraction - if it looks like data, extract it. Don't be picky about HTML structure.
"""

        # Generate custom scraper using LLM

        response = self._get_response({
            "system_prompt": system_prompt,
            "user_prompt": user_prompt
        })
        
        # Clean up the response to remove markdown formatting
        if response:
            # Remove markdown code blocks
            response = re.sub(r'```python\s*', '', response)
            response = re.sub(r'```\s*$', '', response)
            response = re.sub(r'^```\s*', '', response)
            response = response.strip()
        
        return response

    def _validate_scraper_code(self, scraper_code: str) -> bool:
        """Validate the generated scraper code for basic syntax and structure"""
        if not scraper_code:
            return False
        
        # Check for required function
        if 'def custom_scraper(' not in scraper_code:
            return False
        
        # Check for basic BeautifulSoup usage
        if 'soup' not in scraper_code:
            return False
        
        # Check for return statement
        if 'return' not in scraper_code:
            return False
        
        # Try to compile the code
        try:
            compile(scraper_code, '<string>', 'exec')
            return True
        except SyntaxError:
            return False

    def _execute_custom_scraper(self, scraper_code: str, html_content: str) -> Dict[str, Any]:
        """Execute the generated custom scraper with enhanced error handling"""
        try:
            self.logger.info(f"Executing scraper code (length: {len(scraper_code)} characters)")
            
            # Validate code first
            if not self._validate_scraper_code(scraper_code):
                return {"error": "Generated scraper code failed validation", "scraper_code": scraper_code[:500]}
            
            # Create a safe namespace for the scraper
            safe_namespace = {
                'BeautifulSoup': BeautifulSoup,
                'json': json,
                're': re,
                'len': len,
                'str': str,
                'int': int,
                'float': float,
                'list': list,
                'dict': dict,
                'enumerate': enumerate,
                'range': range,
                'any': any,
                'all': all,
                'max': max,
                'min': min,
                'sum': sum,
                'sorted': sorted,
                'reversed': reversed,
                'zip': zip,
                'filter': filter,
                'map': map,
                '__builtins__': {
                    'len': len, 'str': str, 'int': int, 'float': float,
                    'list': list, 'dict': dict, 'enumerate': enumerate, 'range': range,
                    'any': any, 'all': all, 'max': max, 'min': min, 'sum': sum,
                    'sorted': sorted, 'reversed': reversed, 'zip': zip, 'filter': filter, 'map': map,
                    'print': print, 'bool': bool, 'type': type, 'isinstance': isinstance,
                    'hasattr': hasattr, 'getattr': getattr, 'setattr': setattr,
                    '__import__': __import__
                }
            }
            
            # Execute the scraper code
            exec(scraper_code, safe_namespace)
            
            # Get the custom_scraper function
            custom_scraper = safe_namespace.get('custom_scraper')
            if not custom_scraper:
                return {"error": "custom_scraper function not found in generated code"}
            
            # Parse HTML and execute scraper
            soup = BeautifulSoup(html_content, 'html.parser')
            result = custom_scraper(soup)
            
            # Validate result structure
            if not isinstance(result, dict):
                return {"error": "Scraper did not return a dictionary"}
            
            if 'data' not in result:
                return {"error": "Scraper result missing 'data' field"}
            
            # Limit data size to prevent excessive output
            if isinstance(result['data'], list) and len(result['data']) > 200:
                result['data'] = result['data'][:200]
                result['debug'] = result.get('debug', {})
                result['debug']['data_truncated'] = f"Results limited to 200 items (original: {len(result['data'])})"
            
            return result
            
        except SyntaxError as e:
            self.logger.error(f"Syntax error in generated scraper code: {e}")
            return {"error": f"Syntax error: {str(e)}", "scraper_code_preview": scraper_code[:500]}
        except Exception as e:
            self.logger.error(f"Error executing custom scraper: {e}")
            return {"error": f"Execution error: {str(e)}", "scraper_code_preview": scraper_code[:500]}

    def _fallback_extraction(self, soup: BeautifulSoup, instructions: str) -> Dict[str, Any]:
        """Generic fallback extraction method for any webpage"""
        self.logger.info("Using fallback extraction method...")
        
        result = {
            "data": [],
            "debug": {
                "method": "fallback",
                "tables_found": 0,
                "lists_found": 0,
                "extraction_attempts": []
            }
        }
        
        instruction_keywords = [word.lower() for word in re.findall(r'\b\w+\b', instructions) if len(word) > 3]
        
        # Strategy 1: Try tables first
        tables = soup.find_all('table')
        result["debug"]["tables_found"] = len(tables)
        
        if tables:
            # Score tables by relevance
            table_scores = []
            for i, table in enumerate(tables):
                score = 0
                
                # Check caption relevance
                caption = table.find('caption')
                if caption:
                    caption_text = caption.get_text(strip=True).lower()
                    score += sum(2 for keyword in instruction_keywords if keyword in caption_text)
                
                # Check content relevance
                table_text = table.get_text().lower()
                score += sum(1 for keyword in instruction_keywords if keyword in table_text)
                
                # Prefer tables with more rows (likely data tables)
                rows = table.find_all('tr')
                score += min(len(rows) / 10, 5)  # Max 5 points for row count
                
                table_scores.append((score, i, table))
            
            # Sort by score and try best tables first
            table_scores.sort(reverse=True, key=lambda x: x[0])
            
            for score, table_idx, table in table_scores[:3]:  # Try top 3 tables
                try:
                    rows = table.find_all('tr')
                    if len(rows) < 2:  # Need header + data
                        continue
                    
                    # Extract headers from first row
                    header_row = rows[0]
                    headers = []
                    for cell in header_row.find_all(['th', 'td']):
                        header_text = cell.get_text(strip=True)
                        headers.append(header_text if header_text else f"column_{len(headers)+1}")
                    
                    if not headers:
                        continue
                    
                    # Extract data rows
                    extracted_count = 0
                    for row in rows[1:51]:  # Limit to 50 rows max
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= len(headers):
                            row_data = {}
                            for j, cell in enumerate(cells[:len(headers)]):
                                if j < len(headers):
                                    # Clean the cell text
                                    cell_text = cell.get_text(strip=True)
                                    # Remove reference numbers and brackets
                                    cell_text = re.sub(r'\[.*?\]', '', cell_text).strip()
                                    row_data[headers[j]] = cell_text
                            
                            # Only add if row has meaningful data
                            if any(value and len(value) > 1 for value in row_data.values()):
                                result["data"].append(row_data)
                                extracted_count += 1
                    
                    result["debug"]["extraction_attempts"].append(
                        f"Table {table_idx} (score: {score:.1f}): Extracted {extracted_count} rows"
                    )
                    
                    if result["data"]:
                        break
                        
                except Exception as e:
                    result["debug"]["extraction_attempts"].append(f"Table {table_idx} failed: {str(e)}")
        
        # Strategy 2: Try lists if no table data found
        if not result["data"]:
            lists = soup.find_all(['ul', 'ol'])
            result["debug"]["lists_found"] = len(lists)
            
            for i, list_elem in enumerate(lists[:3]):  # Try first 3 lists
                try:
                    items = list_elem.find_all('li')
                    if len(items) < 3:  # Need meaningful amount of data
                        continue
                    
                    list_data = []
                    for item in items[:50]:  # Limit to 50 items
                        item_text = item.get_text(strip=True)
                        if item_text and len(item_text) > 2:
                            # Try to extract structured data from list items
                            item_data = {"text": item_text}
                            
                            # Look for links
                            link = item.find('a')
                            if link and link.get('href'):
                                item_data["link"] = link.get('href')
                                if not item_data["text"]:
                                    item_data["text"] = link.get_text(strip=True)
                            
                            list_data.append(item_data)
                    
                    if list_data:
                        result["data"] = list_data
                        result["debug"]["extraction_attempts"].append(f"List {i}: Extracted {len(list_data)} items")
                        break
                        
                except Exception as e:
                    result["debug"]["extraction_attempts"].append(f"List {i} failed: {str(e)}")
        
        # Strategy 3: Extract from main content if nothing else works
        if not result["data"]:
            main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
            if main_content:
                # Look for any structured content
                structured_elements = main_content.find_all(['p', 'div', 'span'], limit=20)
                content_data = []
                
                for elem in structured_elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 10 and any(keyword in text.lower() for keyword in instruction_keywords):
                        content_data.append({"content": text})
                
                if content_data:
                    result["data"] = content_data
                    result["debug"]["extraction_attempts"].append(f"Main content: Extracted {len(content_data)} items")
        
        return result

    def scrape(self, question: str, instructions: str, parameter: str) -> Dict[str, Any]:
        """
        Main scraping method with improved custom scraper generation and fallbacks
        """
        try:
            # Step 1: Extract URL from parameter
            url = self._extract_url_from_parameter(parameter)
            self.logger.info(f"Processing URL: {url}")
            
            # Step 2: Fetch page content
            html_content = self._fetch_page_content(url)
            if not html_content:
                return {"error": "Failed to fetch page content"}
            
            # Step 3: Analyze page structure
            self.logger.info("Analyzing page structure...")
            structure_analysis = self._analyze_page_structure(html_content, url)
            self.logger.info(f"Page type identified: {structure_analysis['page_type']}")
            self.logger.info(f"Tables found: {structure_analysis['table_count']}")
            
            # Step 4: Generate custom scraper
            scraper_code = self._generate_custom_scraper(structure_analysis, instructions, html_content)
            
            if not scraper_code:
                self.logger.warning("Failed to generate custom scraper, using fallback...")
                soup = BeautifulSoup(html_content, 'html.parser')
                return self._fallback_extraction(soup, instructions)
            
            # Step 5: Execute custom scraper
            scraped_data = self._execute_custom_scraper(scraper_code, html_content)
            
            # Step 6: Check if scraper worked, use fallback if needed
            if "error" in scraped_data:
                self.logger.warning(f"Custom scraper failed: {scraped_data['error']}")
                soup = BeautifulSoup(html_content, 'html.parser')
                fallback_result = self._fallback_extraction(soup, instructions)
                fallback_result["debug"]["custom_scraper_error"] = scraped_data["error"]
                return fallback_result
            
            # Step 7: Prepare final result
            result = {
                "data": scraped_data.get("data", []),
                "status": "success",
                "debug": {
                    "url": url,
                    "page_type": structure_analysis['page_type'],
                    "tables_found": structure_analysis['table_count'],
                    "scraper_method": "custom_generated",
                    "data_count": len(scraped_data.get("data", [])),
                    "scraper_debug": scraped_data.get("debug", {})
                }
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in scraping process: {e}")
            return {
                "error": str(e),
                "question": question,
                "instructions": instructions,
                "parameter": parameter,
                "status": "error"
            }