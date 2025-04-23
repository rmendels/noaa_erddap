import re

def update_dataset_status(xml_file_path, dataset_ids_file_path, output_file_path):
    # Read the list of datasetIDs from the file
    with open(dataset_ids_file_path, 'r') as f:
        dataset_ids = [line.strip() for line in f.readlines()]
    
    # Read the XML content
    with open(xml_file_path, 'r') as f:
        xml_content = f.read()
    
    # Process each datasetID in the list
    for dataset_id in dataset_ids:
        # Create a pattern to match the specific dataset entry
        pattern = rf'<dataset type="[^"]*" datasetID="{re.escape(dataset_id)}" active="true">'
        
        # Replace active="true" with active="false" for this dataset
        replacement = f'<dataset type="EDDGridFromErddap" datasetID="{dataset_id}" active="false">'
        
        # Perform the replacement
        xml_content = re.sub(pattern, replacement, xml_content)
    
    # Write the updated content to the output file
    with open(output_file_path, 'w') as f:
        f.write(xml_content)
    
    print(f"Processing complete. Updated XML saved to {output_file_path}")

# Example usage
if __name__ == "__main__":
    # Replace these paths with your actual file paths
    xml_file_path = "input.xml"
    dataset_ids_file_path = "dataset_ids.txt"
    output_file_path = "updated.xml"
    
    update_dataset_status(xml_file_path, dataset_ids_file_path, output_file_path)
