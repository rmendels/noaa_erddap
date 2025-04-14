import re
import sys
import os
from xml.sax.saxutils import unescape, escape

def read_xml_file(file_path):
    """
    Read an XML file and extract all dataset entries with their line numbers.
    
    Args:
        file_path (str): Path to the XML file
        
    Returns:
        list: List of tuples (dataset_id, dataset_content, start_line, end_line)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        datasets = []
        in_dataset = False
        current_dataset = []
        current_id = None
        start_line = 0
        
        # Pattern to extract datasetID
        id_pattern = r'<dataset\s+[^>]*?datasetID="([^"]+)"[^>]*?>'
        
        for i, line in enumerate(lines):
            if not in_dataset and '<dataset ' in line:
                # Start of a dataset
                id_match = re.search(id_pattern, line)
                if id_match:
                    current_id = unescape(id_match.group(1))
                    in_dataset = True
                    current_dataset = [line]
                    start_line = i
            elif in_dataset:
                current_dataset.append(line)
                if '</dataset>' in line:
                    # End of a dataset
                    in_dataset = False
                    dataset_content = ''.join(current_dataset)
                    datasets.append((current_id, dataset_content, start_line, i))
                    current_dataset = []
                    current_id = None
        
        return datasets
    
    except Exception as e:
        print(f"Error reading XML file: {e}")
        return []

def read_duplicate_file(file_path):
    """
    Read the duplicate IDs file and extract duplicate dataset information.
    
    Args:
        file_path (str): Path to the duplicate IDs file
        
    Returns:
        dict: Dictionary mapping duplicate IDs to lists of line numbers
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        duplicates = {}
        current_id = None
        
        # Process each block in the file
        blocks = content.split('\n\n')
        for block in blocks:
            if not block.strip():
                continue
                
            lines = block.strip().split('\n')
            
            # Extract dataset ID
            id_match = re.match(r'DatasetID: (.+)', lines[0])
            if id_match:
                current_id = id_match.group(1)
                duplicates[current_id] = []
                
                # Extract line numbers
                for line in lines[2:]:  # Skip the "Appears X times" line
                    line_match = re.match(r'\s+Line (\d+): (.+)', line)
                    if line_match:
                        line_num = int(line_match.group(1))
                        url = line_match.group(2)
                        duplicates[current_id].append((line_num, url))
        
        return duplicates
    
    except Exception as e:
        print(f"Error reading duplicate file: {e}")
        return {}

def process_duplicates(xml_file, duplicate_file, output_file):
    """
    Process the XML file and duplicate file, prompt for which duplicates to keep,
    and write a new XML file without the duplicates.
    
    Args:
        xml_file (str): Path to the XML file
        duplicate_file (str): Path to the duplicate IDs file
        output_file (str): Path to the output XML file
    """
    # Read files
    print(f"Reading XML file: {xml_file}")
    datasets = read_xml_file(xml_file)
    if not datasets:
        print("No datasets found in the XML file.")
        return
    
    print(f"Reading duplicate file: {duplicate_file}")
    duplicates = read_duplicate_file(duplicate_file)
    if not duplicates:
        print("No duplicates found in the duplicate file.")
        print(f"Writing all {len(datasets)} datasets to output file...")
        write_output_file(xml_file, output_file, [])
        return
    
    # Create a lookup from line number to dataset index
    line_to_index = {}
    for i, (_, _, start_line, _) in enumerate(datasets):
        line_to_index[start_line] = i
    
    # Process each duplicate
    datasets_to_remove = set()
    
    for dataset_id, occurrences in duplicates.items():
        print(f"\n{'='*50}")
        print(f"Found duplicate dataset ID: {dataset_id}")
        print(f"This ID appears {len(occurrences)} times:")
        
        # Display options
        options = []
        for i, (line_num, url) in enumerate(occurrences):
            if line_num in line_to_index:
                dataset_idx = line_to_index[line_num]
                dataset_content = datasets[dataset_idx][1]
                
                # Extract sourceUrl for display
                url_match = re.search(r'<sourceUrl>([^<]+)</sourceUrl>', dataset_content)
                url = url_match.group(1) if url_match else "Unknown URL"
                
                print(f"  {i+1}. Line {line_num}: {url}")
                options.append((dataset_idx, line_num))
            else:
                print(f"  {i+1}. Line {line_num}: {url} (WARNING: Dataset not found at this line)")
        
        # Prompt for choice
        while True:
            try:
                choice = input("\nEnter the number of the entry to KEEP (1-{}): ".format(len(options)))
                choice_idx = int(choice) - 1
                
                if 0 <= choice_idx < len(options):
                    keep_idx = options[choice_idx][0]
                    
                    # Mark all other occurrences for removal
                    for i, (dataset_idx, _) in enumerate(options):
                        if i != choice_idx:
                            datasets_to_remove.add(dataset_idx)
                    
                    print(f"Keeping dataset at line {options[choice_idx][1]}, removing {len(options) - 1} duplicate(s).")
                    break
                else:
                    print(f"Invalid choice. Please enter a number between 1 and {len(options)}.")
            except ValueError:
                print("Please enter a valid number.")
    
    # Write output file
    write_output_file(xml_file, output_file, datasets_to_remove)

def write_output_file(input_file, output_file, indices_to_remove):
    """
    Write a new XML file excluding the specified dataset indices.
    
    Args:
        input_file (str): Path to the input XML file
        output_file (str): Path to the output XML file
        indices_to_remove (set): Set of dataset indices to remove
    """
    try:
        # Read original file
        with open(input_file, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Find and remove duplicate datasets
        datasets = read_xml_file(input_file)
        
        # Sort indices in reverse order to avoid position shifts
        indices_to_remove = sorted(indices_to_remove, reverse=True)
        
        lines = content.split('\n')
        for idx in indices_to_remove:
            _, _, start_line, end_line = datasets[idx]
            
            # Replace these lines with empty strings
            for i in range(start_line, end_line + 1):
                if i < len(lines):
                    lines[i] = ""
        
        # Remove empty lines but preserve XML structure
        output_content = '\n'.join(line for line in lines if line != "")
        
        # Write to output file
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(output_content)
        
        print(f"\n{'='*50}")
        print(f"Removed {len(indices_to_remove)} duplicate datasets.")
        print(f"Original file had {len(datasets)} datasets.")
        print(f"New file has {len(datasets) - len(indices_to_remove)} datasets.")
        print(f"Output written to: {output_file}")
    
    except Exception as e:
        print(f"Error writing output file: {e}")

def main():
    # Default file paths
    xml_file = "noaa_erddap.xml"
    duplicate_file = "duplicate_dataset_ids.txt"
    output_file = "noaa_erddap_cleaned.xml"
    
    # Check command line arguments
    if len(sys.argv) > 1:
        xml_file = sys.argv[1]
    if len(sys.argv) > 2:
        duplicate_file = sys.argv[2]
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    
    # Verify input files exist
    if not os.path.exists(xml_file):
        print(f"Error: XML file '{xml_file}' not found.")
        return
    
    if not os.path.exists(duplicate_file):
        print(f"Error: Duplicate file '{duplicate_file}' not found.")
        return
    
    # Process duplicates
    process_duplicates(xml_file, duplicate_file, output_file)

if __name__ == "__main__":
    main()
