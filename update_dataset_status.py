import re
import sys

def update_dataset_status(xml_file_path, source_urls_file_path, output_file_path):
    # Read the list of source URLs from the file
    with open(source_urls_file_path, 'r') as f:
        source_urls = [line.strip() for line in f.readlines() if line.strip()]
    
    # Extract datasetIDs from source URLs
    # Assuming URLs are in format like https://erddap.aoos.org/erddap/griddap/CBHAR_WRF_SFC_1979_2009
    dataset_ids = []
    for url in source_urls:
        # Extract the last part of the URL which should be the datasetID
        parts = url.rstrip('/').split('/')
        if parts:
            dataset_id = parts[-1]
            dataset_ids.append(dataset_id)
    
    print(f"Extracted {len(dataset_ids)} dataset IDs from {len(source_urls)} URLs")
    
    # Read the XML content
    with open(xml_file_path, 'r') as f:
        xml_content = f.read()
    
    # Create a counter to track how many datasets were updated
    updated_count = 0
    
    # Process each datasetID
    for dataset_id in dataset_ids:
        # Store the original XML content for comparison
        original_xml = xml_content
        
        # Replace with both possible attribute orders
        xml_content = xml_content.replace(
            f'datasetID="{dataset_id}" active="true"', 
            f'datasetID="{dataset_id}" active="false"'
        )
        xml_content = xml_content.replace(
            f'active="true" datasetID="{dataset_id}"', 
            f'active="false" datasetID="{dataset_id}"'
        )
        
        # Check if any replacement happened
        if xml_content != original_xml:
            updated_count += 1
            print(f"Updated dataset: {dataset_id}")
        else:
            print(f"Warning: Dataset {dataset_id} either not found or already inactive")
    
    # Write the updated content to the output file
    with open(output_file_path, 'w') as f:
        f.write(xml_content)
    
    print(f"Processing complete. {updated_count} datasets were updated.")
    print(f"Updated XML saved to {output_file_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python update_datasets.py <xml_file_path> <source_urls_file_path> <output_file_path>")
        sys.exit(1)
    
    xml_file_path = sys.argv[1]
    source_urls_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    
    update_dataset_status(xml_file_path, source_urls_file_path, output_file_path)
    