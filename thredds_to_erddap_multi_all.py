#!/usr/bin/env python3
"""
THREDDS to ERDDAP Converter (DAS Version - Multithreaded)

This script traverses a THREDDS Data Server (TDS) catalog, extracts ALL datasets and their metadata
using OPeNDAP DAS responses, and formats them for use with ERDDAP's EDDGridFromDap datatype.

Supports both THREDDS 4 and THREDDS 5 catalog structures.
Uses multithreading for improved performance.
Processes ALL datasets by default (both aggregated and individual time slices).

Usage:
    python thredds_to_erddap_das.py --url https://coastwatch.noaa.gov/thredds/catalog.xml --output erddap_datasets.xml

Requirements:
    - requests
    - beautifulsoup4
    - lxml
"""

import argparse
import re
from urllib.parse import urljoin, urlparse, parse_qs
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
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

def detect_thredds_version(base_url, verbose=False):
    """Detect THREDDS version by checking the server info"""
    try:
        # Try to get server info from the root THREDDS page
        info_url = urljoin(base_url, '/thredds/info/serverInfo.html')
        response = requests.get(info_url, timeout=10)
        
        if response.status_code == 200:
            content = response.text.lower()
            if 'thredds data server version 5' in content or 'tds version 5' in content:
                log("Detected THREDDS version 5", verbose)
                return 5
            elif 'thredds data server version 4' in content or 'tds version 4' in content:
                log("Detected THREDDS version 4", verbose)
                return 4
        
        # Fallback: check catalog structure
        catalog_url = urljoin(base_url, '/thredds/catalog.xml')
        response = requests.get(catalog_url, timeout=10)
        
        if response.status_code == 200:
            if 'xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.2"' in response.text:
                log("Detected THREDDS version 5 from catalog namespace", verbose)
                return 5
                
    except Exception as e:
        log(f"Error detecting THREDDS version: {e}", verbose)
    
    log("Defaulting to THREDDS version 4", verbose)
    return 4

def fetch_catalog(url, thredds_version=4, verbose=False):
    """Fetch and parse a THREDDS catalog from a URL"""
    log(f"Fetching catalog: {url}", verbose)
    
    try:
        if url.endswith('.html'):
            url = url.replace('.html', '.xml')
            
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'xml')
        return soup
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching catalog: {e}", verbose)
        return None

def is_time_specific_dataset(name):
    """Check if a dataset name indicates it's a time-specific file"""
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

def extract_datasets(catalog, base_url, thredds_version=4, filter_time_specific=False, verbose=False):
    """Extract datasets from a THREDDS catalog"""
    datasets = []
    
    services = {}
    for service in catalog.find_all('service'):
        if service.has_attr('name') and service.has_attr('serviceType'):
            services[service['name']] = service['serviceType']
    
    for dataset_elem in catalog.find_all('dataset'):
        if not dataset_elem.has_attr('name'):
            continue
            
        name = dataset_elem['name']
        dataset_id = dataset_elem.get('ID', None)
        
        # Skip time-specific datasets only if filtering is enabled
        if filter_time_specific and is_time_specific_dataset(name):
            log(f"Skipping time-specific dataset: {name}", verbose)
            continue
        
        # Check if this is a catalog reference (not an actual dataset)
        if dataset_elem.find('catalogRef'):
            continue
            
        opendap_url = None
        
        for access in dataset_elem.find_all('access'):
            service_name = access.get('serviceName', '')
            service_type = services.get(service_name, '')
            
            if service_type.lower() in ['opendap', 'dods']:
                url_path = access.get('urlPath', '')
                if url_path:
                    if thredds_version == 5:
                        opendap_url = urljoin(base_url.replace('/catalog/', '/dodsC/'), url_path)
                    else:
                        opendap_url = urljoin(base_url, url_path)
                    break
        
        if not opendap_url and dataset_elem.has_attr('urlPath'):
            url_path = dataset_elem['urlPath']
            for service_name, service_type in services.items():
                if service_type.lower() in ['opendap', 'dods']:
                    opendap_url = urljoin(base_url.replace('/catalog/', '/dodsC/'), url_path)
                    break
        
        if opendap_url:
            dataset = ThreddsDataset(name, opendap_url, dataset_id)
            datasets.append(dataset)
            log(f"Found dataset: {dataset}", verbose)
            
    return datasets

def find_catalog_refs(catalog, base_url, thredds_version=4, filter_time_specific=False, verbose=False):
    """Find all catalogRef elements in a THREDDS catalog"""
    refs = []
    
    for ref_elem in catalog.find_all('catalogRef'):
        href = None
        name = None
        
        if ref_elem.has_attr('xlink:href'):
            href = ref_elem['xlink:href']
            name = ref_elem.get('xlink:title', href)
        
        if not href and ref_elem.has_attr('href'):
            href = ref_elem['href']
            name = ref_elem.get('name', href)
        
        if href and name:
            # Skip only if filtering is enabled
            if filter_time_specific:
                if is_time_specific_dataset(name):
                    log(f"Skipping time-specific catalog reference: {name}", verbose)
                    continue
                    
                if any(word in name.lower() for word in ['files', 'individual', 'single']):
                    log(f"Skipping individual files catalog: {name}", verbose)
                    continue
                
            full_url = urljoin(base_url, href)
            
            if thredds_version == 5:
                if not full_url.endswith('.xml'):
                    if full_url.endswith('/'):
                        full_url += 'catalog.xml'
                    else:
                        full_url += '/catalog.xml'
            
            refs.append((name, full_url))
            log(f"Found catalog reference: {name} -> {full_url}", verbose)
            
    return refs

def traverse_catalog_worker(args):
    """Worker function for parallel catalog traversal"""
    url, max_depth, current_depth, thredds_version, filter_time_specific, verbose = args
    
    if current_depth > max_depth:
        log(f"Reached maximum depth ({max_depth}), stopping traversal", verbose)
        return []
    
    catalog = fetch_catalog(url, thredds_version, verbose)
    if not catalog:
        return []
    
    datasets = extract_datasets(catalog, url, thredds_version, filter_time_specific, verbose)
    refs = find_catalog_refs(catalog, url, thredds_version, filter_time_specific, verbose)
    
    # Return datasets and references for further processing
    return datasets, refs, current_depth

def traverse_catalog_parallel(url, max_depth=5, thredds_version=None, filter_time_specific=False, verbose=False, max_workers=5):
    """Parallel version of catalog traversal"""
    if thredds_version is None:
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        thredds_version = detect_thredds_version(base_url, verbose)
    
    all_datasets = []
    
    # Use ThreadPoolExecutor for parallel catalog fetching
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Queue for managing catalog URLs to process
        futures = []
        
        # Start with the initial URL
        future = executor.submit(traverse_catalog_worker, 
                               (url, max_depth, 0, thredds_version, filter_time_specific, verbose))
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
                                new_future = executor.submit(traverse_catalog_worker,
                                                          (ref_url, max_depth, current_depth + 1, 
                                                           thredds_version, filter_time_specific, verbose))
                                new_futures.append(new_future)
                except Exception as e:
                    log(f"Error in catalog traversal: {e}", verbose)
            
            # Update futures with new ones
            futures = new_futures
    
    return all_datasets

def parse_das_attribute(line):
    """Parse a single DAS attribute line"""
    match = re.match(r'\s*(\w+)\s+(\w+)\s+(.+);', line)
    if match:
        attr_type, attr_name, attr_value = match.groups()
        
        attr_value = attr_value.strip()
        
        if attr_value.startswith('"') and attr_value.endswith('"'):
            attr_value = attr_value[1:-1]
        
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
        
        if not line:
            i += 1
            continue
        
        if line == 'Attributes {':
            current_section = 'global'
            i += 1
            continue
        
        var_match = re.match(r'(\w+) {', line)
        if var_match:
            current_var = var_match.group(1)
            if current_var not in var_attrs:
                var_attrs[current_var] = {}
            i += 1
            continue
        
        if line == '}':
            if current_var:
                current_var = None
            else:
                current_section = None
            i += 1
            continue
        
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
        if not dataset.metadata and not dataset.variables:
            continue
            
        ds_elem = ET.SubElement(root, "dataset", {
            "type": "EDDGridFromDap",
            "datasetID": dataset.id,
            "active": "true"
        })
        
        source = ET.SubElement(ds_elem, "sourceUrl")
        source.text = dataset.url
        
        reload = ET.SubElement(ds_elem, "reloadEveryNMinutes")
        reload.text = "10080"
        
        attrs = ET.SubElement(ds_elem, "addAttributes")
        
        if dataset.metadata:
            global_attrs = ET.SubElement(attrs, "att", {"name": "."})
            for attr_name, attr_value in dataset.metadata.items():
                attr_elem = ET.SubElement(global_attrs, "att", {"name": attr_name})
                attr_elem.text = str(attr_value)
        
        for var_name, var_attrs_dict in dataset.variables.items():
            if var_attrs_dict:
                var_attrs = ET.SubElement(attrs, "att", {"name": var_name})
                for attr_name, attr_value in var_attrs_dict.items():
                    attr_elem = ET.SubElement(var_attrs, "att", {"name": attr_name})
                    attr_elem.text = str(attr_value)
    
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
    
    lines = [line for line in xml_str.split('\n') if line.strip()]
    xml_str = '\n'.join(lines[1:])
    
    with open(output_file, 'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8" ?>\n')
        f.write(xml_str)
        
    log(f"Wrote {len(datasets)} datasets to {output_file}", verbose)

def main():
    args = parse_args()
    
    log(f"Starting THREDDS catalog traversal from: {args.url}", args.verbose)
    
    start_time = time.time()
    
    # Parallel catalog traversal - filter_time_specific is False by default
    datasets = traverse_catalog_parallel(args.url, args.max_depth, 
                                       filter_time_specific=args.filter,
                                       verbose=args.verbose, 
                                       max_workers=args.catalog_threads)
    
    catalog_time = time.time() - start_time
    log(f"Found {len(datasets)} datasets with OPeNDAP access in {catalog_time:.2f} seconds", args.verbose)
    
    # Parallel DAS metadata fetching
    das_start_time = time.time()
    successful = fetch_das_metadata_parallel(datasets, 
                                           verbose=args.verbose, 
                                           max_workers=args.threads)
    
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
