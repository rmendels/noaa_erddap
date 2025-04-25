import re
import sys

def update_dataset_status(xml_file_path, source_urls_file_path, output_file_path):
    # Read the list of source URLs from the file
    with open(source_urls_file_path, 'r') as f:
        source_urls = [line.strip() for line in f.readlines() if line.strip()]
    
    # Extract datasetIDs from source URLs
    url_dataset_ids = set()
    for url in source_urls:
        # Extract the last part of the URL which should be the datasetID
        parts = url.rstrip('/').split('/')
        if parts:
            dataset_id = parts[-1]
            url_dataset_ids.add(dataset_id)
    
    print(f"Extracted {len(url_dataset_ids)} dataset IDs from URLs")
    
    # Read the XML content
    with open(xml_file_path, 'r') as f:
        xml_content = f.read()
    
    # Find all dataset entries in the XML
    dataset_pattern = re.compile(r'<dataset [^>]*datasetID="([^"]+)"[^>]*active="(true|false)"[^>]*>')
    alt_pattern = re.compile(r'<dataset [^>]*active="(true|false)"[^>]*datasetID="([^"]+)"[^>]*>')
    
    # Counters for tracking changes
    activated_count = 0
    deactivated_count = 0
    
    # Process each match in the XML content
    # First pattern: datasetID comes before active
    for match in dataset_pattern.finditer(xml_content):
        full_match = match.group(0)
        dataset_id = match.group(1)
        is_active = match.group(2) == "true"
        
        in_url_list = dataset_id in url_dataset_ids
        
        if is_active and in_url_list:
            # Case 2: Dataset is active but in URL list - deactivate it
            new_tag = full_match.replace('active="true"', 'active="false"')
            xml_content = xml_content.replace(full_match, new_tag)
            deactivated_count += 1
            print(f"Deactivated dataset: {dataset_id}")
        elif not is_active and not in_url_list:
            # Case 1: Dataset is inactive and not in URL list - activate it
            new_tag = full_match.replace('active="false"', 'active="true"')
            xml_content = xml_content.replace(full_match, new_tag)
            activated_count += 1
            print(f"Activated dataset: {dataset_id}")
    
    # Second pattern: active comes before datasetID
    for match in alt_pattern.finditer(xml_content):
        full_match = match.group(0)
        is_active = match.group(1) == "true"
        dataset_id = match.group(2)
        
        in_url_list = dataset_id in url_dataset_ids
        
        if is_active and in_url_list:
            # Case 2: Dataset is active but in URL list - deactivate it
            new_tag = full_match.replace('active="true"', 'active="false"')
            xml_content = xml_content.replace(full_match, new_tag)
            deactivated_count += 1
            print(f"Deactivated dataset: {dataset_id}")
        elif not is_active and not in_url_list:
            # Case 1: Dataset is inactive and not in URL list - activate it
            new_tag = full_match.replace('active="false"', 'active="true"')
            xml_content = xml_content.replace(full_match, new_tag)
            activated_count += 1
            print(f"Activated dataset: {dataset_id}")
    
    # Write the updated content to the output file
    with open(output_file_path, 'w') as f:
        f.write(xml_content)
    
    print(f"Processing complete:")
    print(f"- {activated_count} datasets were activated")
    print(f"- {deactivated_count} datasets were deactivated")
    print(f"Updated XML saved to {output_file_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python update_dataset_status.py <xml_file_path> <source_urls_file_path> <output_file_path>")
        sys.exit(1)
    
    xml_file_path = sys.argv[1]
    source_urls_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    
    update_dataset_status(xml_file_path, source_urls_file_path, output_file_path)
