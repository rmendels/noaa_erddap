import re
import collections
import sys
from xml.sax.saxutils import unescape

def find_duplicates(file_path):
    """
    Find duplicate datasetIDs and sourceURLs in an ERDDAP XML file.
    
    Args:
        file_path (str): Path to the XML file
    
    Returns:
        tuple: Two dictionaries - one for duplicate datasetIDs and one for duplicate sourceURLs
    """
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            lines = content.split('\n')
        
        # Find all dataset entries with their line numbers
        dataset_pattern = r'<dataset\s+[^>]*?datasetID="([^"]+)"[^>]*?>'
        url_pattern = r'<sourceUrl>([^<]+)</sourceUrl>'
        
        dataset_entries = []
        line_number = 0
        
        while line_number < len(lines):
            line = lines[line_number]
            match = re.search(dataset_pattern, line)
            
            if match:
                dataset_id = unescape(match.group(1))
                
                # Look for the sourceUrl in the next few lines
                source_url = None
                for i in range(1, 5):  # Check next 5 lines
                    if line_number + i < len(lines):
                        url_match = re.search(url_pattern, lines[line_number + i])
                        if url_match:
                            source_url = unescape(url_match.group(1))
                            break
                
                if source_url:
                    dataset_entries.append((dataset_id, source_url, line_number + 1))  # +1 for 1-based line numbers
            
            line_number += 1
        
        print(f"Found {len(dataset_entries)} dataset entries in {file_path}")
        
        if not dataset_entries:
            print("Warning: No datasets found. Check if the file format matches the expected pattern.")
            return {}, {}
        
        # Collect all datasetIDs and sourceURLs
        dataset_ids = [entry[0] for entry in dataset_entries]
        source_urls = [entry[1] for entry in dataset_entries]
        
        # Find duplicates
        duplicate_ids = [item for item, count in collections.Counter(dataset_ids).items() if count > 1]
        duplicate_urls = [item for item, count in collections.Counter(source_urls).items() if count > 1]
        
        # Create dictionaries with line numbers for duplicates
        duplicate_ids_dict = collections.defaultdict(list)
        duplicate_urls_dict = collections.defaultdict(list)
        
        for dataset_id, source_url, line_number in dataset_entries:
            if dataset_id in duplicate_ids:
                duplicate_ids_dict[dataset_id].append((line_number, source_url))
            
            if source_url in duplicate_urls:
                duplicate_urls_dict[source_url].append((line_number, dataset_id))
        
        return dict(duplicate_ids_dict), dict(duplicate_urls_dict)
    
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {}, {}

def main():
    # Check command line arguments
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "noaa_wide.xml"  # Default file name
    
    print(f"Analyzing {file_path} for duplicate datasets...")
    
    # Find duplicates
    duplicate_ids, duplicate_urls = find_duplicates(file_path)
    
    # Report duplicate IDs
    print("\n----- Duplicate datasetIDs -----")
    if duplicate_ids:
        print(f"Found {len(duplicate_ids)} duplicate datasetIDs:")
        for dataset_id, occurrences in duplicate_ids.items():
            print(f"\nDatasetID: {dataset_id}")
            print(f"Appears {len(occurrences)} times:")
            for position, source_url in occurrences:
                print(f"  Line {position}: {source_url}")
    else:
        print("No duplicate datasetIDs found!")
    
    # Report duplicate URLs
    print("\n----- Duplicate sourceURLs -----")
    if duplicate_urls:
        print(f"Found {len(duplicate_urls)} duplicate sourceURLs:")
        for source_url, occurrences in duplicate_urls.items():
            print(f"\nSourceURL: {source_url}")
            print(f"Appears {len(occurrences)} times:")
            for position, dataset_id in occurrences:
                print(f"  Line {position}: {dataset_id}")
    else:
        print("No duplicate sourceURLs found!")
    
    # Write to output files
    if duplicate_ids:
        output_file = "duplicate_dataset_ids.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            for dataset_id, occurrences in duplicate_ids.items():
                f.write(f"DatasetID: {dataset_id}\n")
                f.write(f"Appears {len(occurrences)} times:\n")
                for position, source_url in occurrences:
                    f.write(f"  Line {position}: {source_url}\n")
                f.write("\n")
        print(f"\nDuplicate datasetIDs written to {output_file}")
    
    if duplicate_urls:
        output_file = "duplicate_source_urls.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            for source_url, occurrences in duplicate_urls.items():
                f.write(f"SourceURL: {source_url}\n")
                f.write(f"Appears {len(occurrences)} times:\n")
                for position, dataset_id in occurrences:
                    f.write(f"  Line {position}: {dataset_id}\n")
                f.write("\n")
        print(f"Duplicate sourceURLs written to {output_file}")

if __name__ == "__main__":
    main()
