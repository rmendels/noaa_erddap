import json
import requests
import time
from urllib.parse import urlparse
import os

def check_url_availability(url, max_retries=3, retry_delay=2, timeout=10):
    """Test if a URL is reachable with retries."""
    for attempt in range(max_retries):
        try:
            response = requests.head(url, timeout=timeout)
            if 200 <= response.status_code < 300:
                return True
            
            # If status code indicates a problem and we have retries left
            if attempt < max_retries - 1:
                print(f"Attempt {attempt+1} failed for {url}: Status code {response.status_code}. Retrying...")
                time.sleep(retry_delay)
            else:
                print(f"All attempts failed for {url}: Status code {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt+1} failed for {url}: {str(e)}. Retrying...")
                time.sleep(retry_delay)
            else:
                print(f"All attempts failed for {url}: {str(e)}")
                return False
    
    return False

def determine_dataset_type(url):
    """
    Determine if the URL is for a griddap or tabledap dataset.
    
    Args:
        url (str): The base URL to check
        
    Returns:
        str: "griddap" or "tabledap" depending on URL structure
    """
    # Check if the URL contains griddap or tabledap
    if "/griddap/" in url:
        return "griddap"
    elif "/tabledap/" in url:
        return "tabledap"
    else:
        # Default to griddap if can't determine
        print(f"Could not determine dataset type for {url}, defaulting to griddap")
        return "griddap"

def test_url_with_appropriate_endpoint(url):
    """
    Test a URL with the appropriate endpoint based on dataset type.
    
    Args:
        url (str): The base URL to test
        
    Returns:
        tuple: (url, endpoint_url, success) 
    """
    dataset_type = determine_dataset_type(url)
    
    if dataset_type == "griddap":
        endpoint_url = f"{url}.das"
    else:  # tabledap
        endpoint_url = f"{url}.nccsvMetadata"
    
    success = check_url_availability(endpoint_url)
    return (url, endpoint_url, success)

def main():
    file_path = "test.json"
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    try:
        # Read and parse the JSON file
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError as json_err:
                print(f"JSON parsing error: {json_err}")
                return
        
        # Validate data structure
        if not isinstance(data, list):
            print("Error: Expected JSON array at the root level. Found:", type(data).__name__)
            return
            
        # Test each URL
        print("Testing URLs...")
        for i, entry in enumerate(data):
            if not isinstance(entry, dict) or "url" not in entry:
                print(f"Error: Item {i} is invalid or missing 'url' key")
                continue
                
            url = entry["url"]
            base_url, endpoint_url, success = test_url_with_appropriate_endpoint(url)
            result = "Success" if success else "Failure"
            print(f"URL: {base_url}")
            print(f"Tested: {endpoint_url}")
            print(f"Result: {result}")
            print("-" * 50)
    
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()
