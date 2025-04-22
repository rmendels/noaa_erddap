import re
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

def check_url_availability(url):
    """
    Test if a URL is reachable
    
    Args:
        url (str): URL to test
        
    Returns:
        bool: True if URL is reachable, False otherwise
    """
    try:
        response = requests.get(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException:
        return False

def process_file(input_file_path, output_file_path):
    """
    Process the input file to extract sourceUrls, test them, and write results to output file
    
    Args:
        input_file_path (str): Path to the input file
        output_file_path (str): Path to the output file
    """
    # Read the input file
    with open(input_file_path, 'r') as file:
        content = file.read()
    
    # Find all sourceUrl elements using regex
    # This approach is more robust than using XML parsing if the file isn't well-formed XML
    source_url_pattern = r'<sourceUrl>(.*?)</sourceUrl>'
    source_urls = re.findall(source_url_pattern, content)
    
    # Open output file for writing results
    with open(output_file_path, 'w') as output_file:
        output_file.write("URL,Accessible\n")  # CSV header
        
        # Process each URL
        for url in source_urls:
            # Append .html to the URL
            test_url = url + ".html"
            
            # Test if the URL is reachable
            is_accessible = check_url_availability(test_url)
            
            # Write result to output file
            output_file.write(f"{test_url},{is_accessible}\n")
            
            # Print result to console
            print(f"Testing: {test_url} - {'Accessible' if is_accessible else 'Not accessible'}")

if __name__ == "__main__":
    # File paths
    input_file = "noaa_combined.xml"  # Replace with your input file path
    output_file = "url_test_results.csv"  # Output file path
    
    # Process the file
    process_file(input_file, output_file)
    print(f"Results written to {output_file}")
