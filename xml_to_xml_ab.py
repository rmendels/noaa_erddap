import xml.etree.ElementTree as ET
import re

def process_xml_file(input_file, output_file):
    # Parse the XML file
    tree = ET.parse(input_file)
    root = tree.getroot()
    
    # Find all dataset elements
    for dataset in root.findall(".//dataset"):
        dataset_type = dataset.get("type")
        dataset_id = dataset.get("datasetID")
        
        # Handle EDDGridFromDap datasets
        if dataset_type == "EDDGridFromDap":
            # Change type attribute
            dataset.set("type", "EDDGridFromErddap")
            
            # Find and modify sourceUrl
            source_url = dataset.find("sourceUrl")
            if source_url is not None:
                # Create new URL with the datasetID
                new_url = f"http://161.55.17.19:8082/erddap/griddap/{dataset_id}"
                source_url.text = new_url
                
        # Handle EDDTableFromErddap or EDDGridFromErddap datasets
        elif dataset_type in ["EDDTableFromErddap", "EDDGridFromErddap"]:
            # Find and modify sourceUrl
            source_url = dataset.find("sourceUrl")
            if source_url is not None:
                # Replace domain with 161.55.17.19:8081
                current_url = source_url.text
                new_url = re.sub(r'https://[^/]+/erddap', 'https://161.55.17.19:8081/erddap', current_url)
                source_url.text = new_url
    
    # Write the modified XML to the output file
    tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"Processed XML file has been saved to {output_file}")

# Usage example
if __name__ == "__main__":
    input_file = "noaa_combined_new.xml"  # Replace with your input file name
    output_file = "modified_noaa_combined_new.xml" # Replace with your desired output file name
    process_xml_file(input_file, output_file)
