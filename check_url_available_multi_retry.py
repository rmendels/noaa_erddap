import re
import requests
import concurrent.futures
import time
from urllib.parse import urlparse
from tenacity import retry, stop_after_attempt, wait_fixed

def create_url_checker(retry_count=3, retry_delay=2):
    """
    Create a URL checking function with retry capabilities
    
    Args:
        retry_count (int): Number of times to retry failed requests
        retry_delay (int): Seconds to wait between retries
        
    Returns:
        function: Decorated check_url_availability function
    """
    @retry(
        stop=stop_after_attempt(retry_count),
        wait=wait_fixed(retry_delay)
    )
    def check_url_availability_with_retry(url):
        """
        Test if a URL is reachable (will be retried if it fails)
        
        Args:
            url (str): URL to test
            
        Returns:
            bool: True if URL is reachable, False otherwise
        """
        response = requests.head(url, timeout=20)
        # Raise an exception if status code is not 200 to trigger retry
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(f"Status code: {response.status_code}")
        return True
    
    return check_url_availability_with_retry

def check_url_availability(url, sleep_time=0, retry_count=3, retry_delay=2):
    """
    Test if a URL is reachable with retries
    
    Args:
        url (str): URL to test
        sleep_time (float): Time to sleep after checking URL in seconds
        retry_count (int): Number of times to retry failed requests
        retry_delay (int): Seconds to wait between retries
        
    Returns:
        tuple: (url, bool) where bool is True if URL is reachable, False otherwise
    """
    # Create a retry-enabled check function with the specified parameters
    check_func = create_url_checker(retry_count, retry_delay)
    
    try:
        # Attempt to check the URL with retries
        check_func(url)
        result = (url, True)
    except Exception as e:
        # If all retries fail, mark as not accessible
        print(f"All retries failed for {url}: {e}")
        result = (url, False)
    
    # Sleep for the specified time
    if sleep_time > 0:
        time.sleep(sleep_time)
        
    return result

def process_file(input_file_path, output_file_path, max_workers=None, sleep_time=0, retry_count=3, retry_delay=2):
    """
    Process the input file to extract sourceUrls, test them concurrently, and write results to output file
    
    Args:
        input_file_path (str): Path to the input file
        output_file_path (str): Path to the output file
        max_workers (int, optional): Maximum number of worker processes. Default is None (uses CPU count)
        sleep_time (float, optional): Time to sleep between URL checks in seconds. Default is 0
        retry_count (int, optional): Number of times to retry failed requests. Default is 3
        retry_delay (int, optional): Seconds to wait between retries. Default is 2
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
        future_to_url = {
            executor.submit(check_url_availability, url, sleep_time, retry_count, retry_delay): url 
            for url in test_urls
        }
        
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
    
    # Process the file with 10 workers, 0.5 second sleep between checks, 
    # 3 retries with 2 seconds between retries
    process_file(
        input_file, 
        output_file, 
        max_workers=10, 
        sleep_time=0.5,
        retry_count=3,
        retry_delay=2
    )
    print(f"Results written to {output_file}")
    