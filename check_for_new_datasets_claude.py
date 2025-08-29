#!/usr/bin/env python3
"""
ERDDAP Dataset Comparison Tool

This script compares datasets available on ERDDAP servers against a combined datasets.xml file
to identify any missing datasets.

Requirements: pip install requests lxml
"""

import requests
import xml.etree.ElementTree as ET
import json
from urllib.parse import urljoin
import time
from typing import Set, List, Dict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_server_list(filename: str) -> List[str]:
    """Read ERDDAP server URLs from text file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            servers = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Ensure URL ends with /erddap if it doesn't already
                    if not line.endswith('/erddap'):
                        if line.endswith('/'):
                            line += 'erddap'
                        else:
                            line += '/erddap'
                    servers.append(line)
            return servers
    except FileNotFoundError:
        logger.error(f"Server list file '{filename}' not found.")
        return []
    except Exception as e:
        logger.error(f"Error reading server list: {e}")
        return []


def parse_datasets_xml(filename: str) -> Set[str]:
    """Parse datasets.xml file and extract all dataset IDs."""
    try:
        tree = ET.parse(filename)
        root = tree.getroot()
        
        dataset_ids = set()
        
        # Look for dataset elements with datasetID attribute
        for dataset in root.findall('.//dataset'):
            dataset_id = dataset.get('datasetID')
            if dataset_id:
                dataset_ids.add(dataset_id)
        
        logger.info(f"Found {len(dataset_ids)} datasets in XML file")
        return dataset_ids
        
    except FileNotFoundError:
        logger.error(f"Datasets XML file '{filename}' not found.")
        return set()
    except ET.ParseError as e:
        logger.error(f"Error parsing XML file: {e}")
        return set()
    except Exception as e:
        logger.error(f"Unexpected error reading XML: {e}")
        return set()


def get_server_datasets(server_url: str, timeout: int = 30) -> Set[str]:
    """Get list of dataset IDs from an ERDDAP server."""
    dataset_ids = set()
    
    try:
        # Try the info/index.json endpoint first (most efficient)
        info_url = urljoin(server_url, '/info/index.json')
        logger.info(f"Querying {info_url}")
        
        response = requests.get(info_url, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract dataset IDs from the JSON response
        if 'table' in data and 'rows' in data['table']:
            for row in data['table']['rows']:
                if len(row) > 0:
                    dataset_ids.add(row[0])  # First column is typically datasetID
        
        logger.info(f"Found {len(dataset_ids)} datasets on {server_url}")
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to query {server_url} via JSON: {e}")
        
        # Fallback: try the XML info endpoint
        try:
            info_xml_url = urljoin(server_url, '/info/index.xml')
            logger.info(f"Trying XML fallback: {info_xml_url}")
            
            response = requests.get(info_xml_url, timeout=timeout)
            response.raise_for_status()
            
            # Parse the XML response
            root = ET.fromstring(response.content)
            
            # Look for dataset rows in the table
            for row in root.findall('.//row'):
                dataset_id_elem = row.find('dataset_id')
                if dataset_id_elem is not None and dataset_id_elem.text:
                    dataset_ids.add(dataset_id_elem.text)
            
            logger.info(f"Found {len(dataset_ids)} datasets on {server_url} (via XML)")
            
        except Exception as e2:
            logger.error(f"Failed to query {server_url}: {e2}")
    
    except Exception as e:
        logger.error(f"Unexpected error querying {server_url}: {e}")
    
    return dataset_ids


def main():
    """Main function to run the dataset comparison."""
    
    # Configuration
    SERVER_LIST_FILE = "erddap_servers.txt"  # Change this to your server list file
    DATASETS_XML_FILE = "datasets.xml"       # Change this to your datasets XML file
    
    print("ERDDAP Dataset Comparison Tool")
    print("=" * 40)
    
    # Read server list
    servers = read_server_list(SERVER_LIST_FILE)
    if not servers:
        print("No servers found. Please check your server list file.")
        return
    
    print(f"Found {len(servers)} servers to check")
    
    # Parse existing datasets XML
    existing_datasets = parse_datasets_xml(DATASETS_XML_FILE)
    if not existing_datasets:
        print("No existing datasets found. Please check your datasets.xml file.")
        return
    
    # Check each server
    all_missing_datasets = {}
    total_server_datasets = set()
    
    for i, server in enumerate(servers, 1):
        print(f"\nChecking server {i}/{len(servers)}: {server}")
        
        server_datasets = get_server_datasets(server)
        total_server_datasets.update(server_datasets)
        
        # Find missing datasets for this server
        missing = server_datasets - existing_datasets
        
        if missing:
            all_missing_datasets[server] = missing
            print(f"  Missing datasets: {len(missing)}")
            for dataset_id in sorted(missing):
                print(f"    - {dataset_id}")
        else:
            print("  No missing datasets found")
        
        # Small delay to be respectful to servers
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total servers checked: {len(servers)}")
    print(f"Total datasets in XML: {len(existing_datasets)}")
    print(f"Total datasets found on servers: {len(total_server_datasets)}")
    print(f"Servers with missing datasets: {len(all_missing_datasets)}")
    
    total_missing = sum(len(datasets) for datasets in all_missing_datasets.values())
    print(f"Total missing datasets: {total_missing}")
    
    if all_missing_datasets:
        print("\nMISSING DATASETS BY SERVER:")
        print("-" * 30)
        for server, missing_datasets in all_missing_datasets.items():
            print(f"\n{server}:")
            for dataset_id in sorted(missing_datasets):
                print(f"  {dataset_id}")
        
        # Save missing datasets to file
        output_file = "missing_datasets.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("Missing Datasets Report\n")
            f.write("=" * 30 + "\n\n")
            for server, missing_datasets in all_missing_datasets.items():
                f.write(f"Server: {server}\n")
                for dataset_id in sorted(missing_datasets):
                    f.write(f"  {dataset_id}\n")
                f.write("\n")
        
        print(f"\nDetailed report saved to: {output_file}")
    
    else:
        print("\nNo missing datasets found!")


if __name__ == "__main__":
    main()
