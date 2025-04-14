import re
import sys
import os
from xml.sax.saxutils import unescape

def extract_dataset_ids(file_path):
    """
    Extract all datasetIDs from an ERDDAP XML file.
    
    Args:
        file_path (str): Path to the XML file
        
    Returns:
        set: Set of all datasetIDs found in the file
    """
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Find all datasetIDs
        dataset_pattern = r'<dataset\s+[^>]*?datasetID="([^"]+)"[^>]*?>'
        matches = re.findall(dataset_pattern, content)
        
        # Unescape XML entities (like &amp;)
        dataset_ids = {unescape(match) for match in matches}
        
        print(f"Found {len(dataset_ids)} unique dataset IDs in {file_path}")
        return dataset_ids
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return set()

def compare_files(file1, file2, output_file=None):
    """
    Compare dataset IDs between two XML files and report differences.
    
    Args:
        file1 (str): Path to the first XML file
        file2 (str): Path to the second XML file
        output_file (str, optional): Path to output file for results
    """
    # Extract dataset IDs from both files
    print(f"Reading dataset IDs from {file1}...")
    ids_file1 = extract_dataset_ids(file1)
    
    print(f"Reading dataset IDs from {file2}...")
    ids_file2 = extract_dataset_ids(file2)
    
    # Find datasets in file2 but not in file1
    unique_to_file2 = ids_file2 - ids_file1
    
    # Report results
    print("\n" + "="*60)
    print(f"Datasets in {file2} but not in {file1}:")
    print("="*60)
    
    if not unique_to_file2:
        print("None found. All datasets in the second file are also in the first file.")
    else:
        print(f"Found {len(unique_to_file2)} datasets unique to {file2}:")
        
        # Sort the list for better readability
        sorted_ids = sorted(unique_to_file2)
        for i, dataset_id in enumerate(sorted_ids):
            print(f"{i+1}. {dataset_id}")
    
    # Write results to output file if specified
    if output_file and unique_to_file2:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"Datasets in {file2} but not in {file1}:\n\n")
                for dataset_id in sorted(unique_to_file2):
                    f.write(f"{dataset_id}\n")
            print(f"\nResults written to {output_file}")
        except Exception as e:
            print(f"Error writing to output file: {e}")

def main():
    # Check command line arguments
    if len(sys.argv) < 3:
        print("Usage: python compare-erddap-files.py ioos_sensors.xml cencoos.xml [output_file.txt]")
        print("\nDefaults to ioos_sensors.xml and cencoos.xml if no arguments are provided.")
        
        # Use default file names if not provided
        file1 = "ioos_sensors.xml"
        file2 = "cencoos.xml"
    else:
        file1 = sys.argv[1]
        file2 = sys.argv[2]
    
    # Optional output file
    output_file = None
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    else:
        # Generate a default output file name
        output_file = f"unique_to_{os.path.basename(file2).split('.')[0]}.txt"
    
    # Verify input files exist
    if not os.path.exists(file1):
        print(f"Error: File '{file1}' not found.")
        return
    
    if not os.path.exists(file2):
        print(f"Error: File '{file2}' not found.")
        return
    
    # Compare files
    compare_files(file1, file2, output_file)

if __name__ == "__main__":
    main()
    