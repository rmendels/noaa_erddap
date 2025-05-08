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
        max_workers (int, optional): Maximum number of worker processes. Default is None (uses CPU count)
        sleep_time (float, optional): Time to sleep between URL checks in seconds. Default is 0
    """
    # Read the input file
    with open(input_file_path, 'r') as file:
        content = file.read()
    
    # Find all sourceUrl elements using regex
    source_url_pattern = r'<sourceUrl>(.*?)</sourceUrl>'
    source_urls = re.findall(source_url_pattern, content)
    
    # Create list of URLs to test
    test_urls = [url + ".das" for url in source_urls]
    
    # Process URLs concurrently
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and map them to their corresponding futures
        future_to_url = {executor.submit(check_url_availability, url, sleep_time): url for url in test_urls}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
                print(f"Testing: {result[0]} - {'Accessible' if result[1] else 'Not accessible'}")
            except Exception as exc:
                print(f"{url} generated an exception: {exc}")
                results.append((url, False))
    
    # Write results to output file
    with open(output_file_path, 'w') as output_file:
        output_file.write("URL,Accessible\n")  # CSV header
        for url, is_accessible in results:
            output_file.write(f"{url},{is_accessible}\n")

if __name__ == "__main__":
    # File paths
    input_file = "noaa_combined.xml"  # Replace with your input file path
    output_file = "url_test_results.csv"  # Output file path
    
    # Process the file with 10 workers and 0.5 second sleep between checks
    process_file(input_file, output_file, max_workers=10, sleep_time=0.5)
    print(f"Results written to {output_file}")
