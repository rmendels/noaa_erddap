import re
import xml.etree.ElementTree as ET
from pathlib import Path

def process_xml_file(input_file, output_file):
    # Read the file as text to preserve formatting
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Define the regex pattern to match sourceUrl lines
    # This will match URLs starting with http or https, followed by anything, then /erddap
    pattern = r'(<sourceUrl>)https?://[^/]+(/erddap.*?)(</sourceUrl>)'
    
    # Replace with the new domain while preserving the path
    replacement = r'\1https://test.noaa.gov\2\3'
    modified_content = re.sub(pattern, replacement, content)
    
    # Write the modified content to the output file
    with open(output_file, 'w') as f:
        f.write(modified_content)
    
    print(f"Processing complete. Modified file saved to {output_file}")

# Example usage
if __name__ == "__main__":
    input_file = input("Enter the path to your XML file: ")
    output_file = input("Enter the path for the output file (or press Enter to use input_modified.xml): ")
    
    if not output_file:
        # Create default output filename based on input filename
        input_path = Path(input_file)
        output_file = str(input_path.parent / f"{input_path.stem}_modified{input_path.suffix}")
    
    process_xml_file(input_file, output_file)
