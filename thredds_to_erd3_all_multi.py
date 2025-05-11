#!/usr/bin/env python3
"""
THREDDS to ERDDAP Converter (DAS Version - Multithreaded)

This script traverses a THREDDS Data Server (TDS) catalog, extracts datasets and their metadata
using OPeNDAP DAS responses, and formats them for use with ERDDAP's EDDGridFromDap datatype.

Usage:
    python thredds_to_erddap_das.py --url https://coastwatch.noaa.gov/thredds/catalog.xml --output erddap_datasets.xml --threads 20

Requirements:
    - requests
    - beautifulsoup4
    - lxml
"""

import argparse
import re
from urllib.parse import urljoin, urlparse
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import requests
from bs4 import BeautifulSoup

# Thread-safe print lock
print_lock = threading.Lock()

class ThreddsDataset:
    """Class to hold metadata for a THREDDS dataset"""
    def __init__(self, name, url, id=None):
        self.name = name
        self.url = url
        self.id = id or self._generate_id()
        self.metadata = {}  # Global attributes
        self.variables = {}  # Variable attributes
        
    def _generate_id(self):
        """Generate a dataset ID based on the name"""
        # Replace spaces and special characters with underscores
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', self.name)
        # Ensure it starts with a letter
        if not clean_name[0].isalpha():
            clean_name = "ds_" + clean_name
        return clean_name.lower()
    
    def __str__(self):
        return f"Dataset: {self.name}, URL: {self.url}, ID: {self.id}"

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Traverse THREDDS catalog and convert to ERDDAP configuration")
    parser.add_argument("--url", required=True, help="URL to the THREDDS catalog XML")
    parser.add_argument("--output", default="erddap_datasets.xml", help="Output XML file for ERDDAP configuration")
    parser.add_argument("--max-depth", type=int, default=5, help="Maximum depth for traversing catalog references")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--threads", type=int, default=10, 
                       help="Number of threads to use for DAS fetching (default: 10)")
    parser.add_argument("--catalog-threads", type=int, default=5,
                       help="Number of threads to use for catalog traversal (default: 5)")
    parser.add_argument("--filter", action="store_true", 
                       help="Filter out time-specific datasets and catalogs (default: False)")
    return parser.parse_args()

def log(message, verbose=False):
    """Thread-safe logging"""
    if verbose:
        with print_lock:
            print(message)

def fetch_catalog(url, verbose=False):
    """Fetch and parse a THREDDS catalog from a URL"""
    log(f"Fetching catalog: {url}", verbose)
    
    try:
        # If URL ends with .html, change it to .xml
        if url.endswith('.html'):
            url = url.replace('.html', '.xml')
            
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse the XML with BeautifulSoup for more flexibility
        soup = BeautifulSoup(response.content, 'xml')
        return soup
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching catalog: {e}", verbose)
        return None

def is_time_specific_dataset(name):
    """Check if a dataset name indicates it's a time-specific file"""
    # Common patterns for time-specific files
    time_patterns = [
        r'-\d{4}$',          # Ends with -YYYY
        r'-\d{6}$',          # Ends with -YYYYMM
        r'-\d{8}$',          # Ends with -YYYYMMDD
        r'_\d{4}$',          # Ends with _YYYY
        r'_\d{6}$',          # Ends with _YYYYMM
        r'_\d{8}$',          # Ends with _YYYYMMDD
        r'_\d{10}$',         # Ends with _YYYYMMDDHH
        r'\.nc\d{8}',        # Contains .ncYYYYMMDD
        r'\d{4}_\d{2}_\d{2}' # Contains YYYY_MM_DD
    ]
    
    for pattern in time_patterns:
        if re.search(pattern, name):
            return True
    return False

def extract_datasets(catalog, base_url, filter_time_specific=False, verbose=False):
    """Extract datasets from a THREDDS catalog"""
    datasets = []
    
    # Find service definitions
    services = {}
    for service in catalog.find_all('service'):
        if service.has_attr('name') and service.has_attr('serviceType'):
            services[service['name']] = service['serviceType']
    
    # Find all dataset elements
    for dataset_elem in catalog.find_all('dataset'):
        # Skip if it doesn't have a name attribute
        if not dataset_elem.has_attr('name'):
            continue
            
        name = dataset_elem['name']
        dataset_id = dataset_elem.get('ID', None)
        
        # Skip time-specific datasets if filtering is enabled
        if filter_time_specific and is_time_specific_dataset(name):
            log(f"Skipping time-specific dataset: {name}", verbose)
            continue
        
        # Check if this is a catalog reference (not an actual dataset)
        if dataset_elem.find('catalogRef'):
            continue
            
        # Look for OPeNDAP access
        opendap_url = None
        
        # Check for access elements
        for access in dataset_elem.find_all('access'):
            service_name = access.get('serviceName', '')
            service_type = services.get(service_name, '')
            
            # Check if this is an OPeNDAP service
            if service_type.lower() in ['opendap', 'dods']:
                url_path = access.get('urlPath', '')
                if url_path:
                    # Construct the full OPeNDAP URL
                    opendap_url = urljoin(base_url, url_path)
                    break
        
        # If no access element, but dataset has urlPath, check if OPeNDAP service exists
        if not opendap_url and dataset_elem.has_attr('urlPath'):
            url_path = dataset_elem['urlPath']
            # Look for an OPeNDAP service in the services
            for service_name, service_type in services.items():
                if service_type.lower() in ['opendap', 'dods']:
                    # Construct OPeNDAP URL
                    opendap_url = urljoin(base_url.replace('/catalog/', '/dodsC/'), url_path)
                    break
        
        # If we found an OPeNDAP URL, create a dataset object
        if opendap_url:
            dataset = ThreddsDataset(name, opendap_url, dataset_id)
            datasets.append(dataset)
            log(f"Found dataset: {dataset}", verbose)
            
    return datasets

def find_catalog_refs(catalog, base_url, filter_time_specific=False, verbose=False):
    """Find all catalogRef elements in a THREDDS catalog"""
    refs = []
    
    for ref_elem in catalog.find_all('catalogRef'):
        href = None
        name = None
        
        # Check for xlink:href attribute
        if ref_elem.has_attr('xlink:href'):
            href = ref_elem['xlink:href']
            name = ref_elem.get('xlink:title', href)
        
        # Also check for href without namespace
        if not href and ref_elem.has_attr('href'):
            href = ref_elem['href']
            name = ref_elem.get('name', href)
        
        if href and name:
            # Skip catalogs based on filtering options
            if filter_time_specific:
                # Skip time-specific catalog references
                if is_time_specific_dataset(name):
                    log(f"Skipping time-specific catalog reference: {name}", verbose)
                    continue
                    
                # Skip catalog references that contain "files" or "individual" in the name
                if any(word in name.lower() for word in ['files', 'individual', 'single']):
                    log(f"Skipping individual files catalog: {name}", verbose)
                    continue
                
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            refs.append((name, full_url))
            log(f"Found catalog reference: {name} -> {full_url}", verbose)
            
    return refs

def catalog_worker(args):
    """Worker function for parallel catalog traversal"""
    url, max_depth, current_depth, filter_time_specific, verbose = args
    
    if current_depth > max_depth:
        log(f"Reached maximum depth ({max_depth}), stopping traversal", verbose)
        return []
    
    catalog = fetch_catalog(url, verbose)
    if not catalog:
        return []
    
    datasets = extract_datasets(catalog, url, filter_time_specific, verbose)
    refs = find_catalog_refs(catalog, url, filter_time_specific, verbose)
    
    # Return datasets and references for further processing
    return datasets, refs, current_depth

def traverse_catalog_parallel(url, max_depth=5, filter_time_specific=False, verbose=False, max_workers=5):
    """Parallel version of catalog traversal"""
    all_datasets = []
    
    # Use ThreadPoolExecutor for parallel catalog fetching
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Queue for managing catalog URLs to process
        futures = []
        
        # Start with the initial URL
        future = executor.submit(catalog_worker, 
                               (url, max_depth, 0, filter_time_specific, verbose))
        futures.append(future)
        
        while futures:
            # Process completed futures
            new_futures = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        datasets, refs, current_depth = result
                        all_datasets.extend(datasets)
                        
                        # Submit new tasks for catalog references
                        if current_depth < max_depth:
                            for name, ref_url in refs:
                                log(f"Queuing catalog reference: {name} (depth {current_depth+1})", verbose)
                                new_future = executor.submit(catalog_worker,
                                                          (ref_url, max_depth, current_depth + 1, 
                                                           filter_time_specific, verbose))
                                new_futures.append(new_future)
                except Exception as e:
                    log(f"Error in catalog traversal: {e}", verbose)
            
            # Update futures with new ones
            futures = new_futures
    
    return all_datasets

def parse_das_attribute(line):
    """Parse a single DAS attribute line"""
    # Match pattern: Type Name Value;
    # Examples:
    #   String long_name "Sea Surface Temperature";
    #   Float32 _FillValue -999.0;
    
    match = re.match(r'\s*(\w+)\s+(\w+)\s+(.+);', line)
    if match:
        attr_type, attr_name, attr_value = match.groups()
        
        # Clean up the value
        attr_value = attr_value.strip()
        
        # Remove quotes from string values
        if attr_value.startswith('"') and attr_value.endswith('"'):
            attr_value = attr_value[1:-1]
        
        # Convert numeric types
        if attr_type in ['Float32', 'Float64']:
            try:
                attr_value = float(attr_value)
            except ValueError:
                pass
        elif attr_type in ['Int16', 'Int32', 'Int64', 'UInt16', 'UInt32', 'UInt64', 'Byte']:
            try:
                attr_value = int(attr_value)
            except ValueError:
                pass
                
        return attr_name, attr_value
    
    return None, None

def parse_das_response(das_text):
    """Parse a DAS response into global and variable attributes"""
    global_attrs = {}
    var_attrs = {}
    
    lines = das_text.split('\n')
    current_section = None
    current_var = None
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip empty lines
        if not line:
            i += 1
            continue
        
        # Check for global attributes section
        if line == 'Attributes {':
            current_section = 'global'
            i += 1
            continue
        
        # Check for variable attributes section
        var_match = re.match(r'(\w+) {', line)
        if var_match:
            current_var = var_match.group(1)
            if current_var not in var_attrs:
                var_attrs[current_var] = {}
            i += 1
            continue
        
        # Check for end of section
        if line == '}':
            if current_var:
                current_var = None
            else:
                current_section = None
            i += 1
            continue
        
        # Parse attribute
        attr_name, attr_value = parse_das_attribute(line)
        if attr_name:
            if current_var:
                var_attrs[current_var][attr_name] = attr_value
            elif current_section == 'global':
                global_attrs[attr_name] = attr_value
        
        i += 1
    
    return global_attrs, var_attrs

def fetch_das_metadata_worker(dataset, verbose=False):
    """Worker function for parallel DAS fetching"""
    das_url = dataset.url + '.das'
    log(f"Fetching DAS for: {dataset.name} from {das_url}", verbose)
    
    try:
        response = requests.get(das_url, timeout=30)
        response.raise_for_status()
        
        # Parse the DAS response
        global_attrs, var_attrs = parse_das_response(response.text)
        
        dataset.metadata = global_attrs
        dataset.variables = var_attrs
        
        return dataset, True
        
    except Exception as e:
        log(f"Error fetching DAS for {dataset.name}: {e}", verbose)
        return dataset, False

def fetch_das_metadata_parallel(datasets, verbose=False, max_workers=10):
    """Parallel version of DAS metadata fetching"""
    successful = 0
    total = len(datasets)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all DAS fetching tasks
        future_to_dataset = {
            executor.submit(fetch_das_metadata_worker, dataset, verbose): dataset 
            for dataset in datasets
        }
        
        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_dataset)):
            dataset = future_to_dataset[future]
            try:
                _, success = future.result()
                if success:
                    successful += 1
                log(f"Processed dataset {i+1}/{total}: {dataset.name}", verbose)
            except Exception as e:
                log(f"Error processing dataset {dataset.name}: {e}", verbose)
    
    return successful

def create_erddap_xml(datasets, output_file, verbose=False):
    """Create ERDDAP XML configuration from THREDDS datasets"""
    root = ET.Element("erddapDatasets")
    
    for dataset in datasets:
        # Skip datasets without metadata
        if not dataset.metadata and not dataset.variables:
            continue
            
        # Create dataset element
        ds_elem = ET.SubElement(root, "dataset", {
            "type": "EDDGridFromDap",
            "datasetID": dataset.id,
            "active": "true"
        })
        
        # Add source URL
        source = ET.SubElement(ds_elem, "sourceUrl")
        source.text = dataset.url
        
        # Add reloadEveryNMinutes
        reload = ET.SubElement(ds_elem, "reloadEveryNMinutes")
        reload.text = "10080"  # Weekly
        
        # Add attributes
        attrs = ET.SubElement(ds_elem, "addAttributes")
        
        # Global attributes
        if dataset.metadata:
            global_attrs = ET.SubElement(attrs, "att", {"name": "."})
            for attr_name, attr_value in dataset.metadata.items():
                attr_elem = ET.SubElement(global_attrs, "att", {"name": attr_name})
                attr_elem.text = str(attr_value)
        
        # Variable attributes
        for var_name, var_attrs_dict in dataset.variables.items():
            if var_attrs_dict:
                var_attrs = ET.SubElement(attrs, "att", {"name": var_name})
                for attr_name, attr_value in var_attrs_dict.items():
                    attr_elem = ET.SubElement(var_attrs, "att", {"name": attr_name})
                    attr_elem.text = str(attr_value)
    
    # Pretty print the XML
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
    
    # Remove empty lines
    lines = [line for line in xml_str.split('\n') if line.strip()]
    xml_str = '\n'.join(lines[1:])  # Skip XML declaration added by toprettyxml
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8" ?>\n')
        f.write(xml_str)
        
    log(f"Wrote {len(datasets)} datasets to {output_file}", verbose)

def main():
    args = parse_args()
    
    log(f"Starting THREDDS catalog traversal from: {args.url}", args.verbose)
    
    start_time = time.time()
    
    # Parallel catalog traversal
    datasets = traverse_catalog_parallel(args.url, args.max_depth, 
                                      args.filter, args.verbose, 
                                      args.catalog_threads)
    
    catalog_time = time.time() - start_time
    log(f"Found {len(datasets)} datasets with OPeNDAP access in {catalog_time:.2f} seconds", args.verbose)
    
    # Parallel DAS metadata fetching
    das_start_time = time.time()
    successful = fetch_das_metadata_parallel(datasets, 
                                          args.verbose, 
                                          args.threads)
    
    das_time = time.time() - das_start_time
    total_time = time.time() - start_time
    
    log(f"Successfully fetched metadata for {successful}/{len(datasets)} datasets in {das_time:.2f} seconds", args.verbose)
    log(f"Total processing time: {total_time:.2f} seconds", args.verbose)
    
    # Create ERDDAP XML
    if successful > 0:
        create_erddap_xml(datasets, args.output, args.verbose)
        log(f"Created ERDDAP configuration: {args.output}", True)
    else:
        log("No datasets with metadata found", True)

if __name__ == "__main__":
    main()
    