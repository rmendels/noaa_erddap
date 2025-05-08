import re
import requests
import concurrent.futures
import time
from urllib.parse import urlparse

def check_url_availability(url, sleep_time=0):
    """
    Test if a URL is reachable
    
    Args:
        url (str): URL to test
        sleep_time (float): Time to sleep after checking URL in seconds
        
    Returns:
        tuple: (url, bool) where bool is True if URL is reachable, False otherwise
    """
    try:
        response = requests.head(url, timeout=20)
        result = (url, response.status_code == 200)
    except requests.RequestException:
        result = (url, False)
    
    # Sleep for the specified time
    if sleep_time > 0:
        time.sleep(sleep_time)
        
    return result

def process_file(input_file_path, output_file_path, max_workers=None, sleep_time=0):
    """
    Process the input file to extract sourceUrls, test them concurrently, and write results to output file
    
    Args:
        input_file_path (str): Path to the input file
        output_file_path (str): Path to the output file
        max_workers (int, optional): Maximum number of worker processes. Default is No
