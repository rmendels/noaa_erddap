#!/usr/bin/env python3
"""
THREDDS to ERDDAP Converter (DAS Version)

This script traverses a THREDDS Data Server (TDS) catalog, extracts datasets and their metadata
using OPeNDAP DAS responses, and formats them for use with ERDDAP's EDDGridFromDap datatype.

Supports both THREDDS 4 and THREDDS 5 catalog structures.

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

import requests
from bs4 import BeautifulSoup

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
    parser.add_argument("--aggregated-only", action="store_true", default=True, 
                       help="Only include aggregated datasets, skip individual time files (default: True)")
    parser.add_argument("--include-all", action="store_true", 
                       help="Include all datasets including individual time files")
    return parser.parse_args()

def log(message, verbose=False):
    """Log messages if verbose is enabled"""
    if verbose:
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
        # THREDDS 5 typically has more complex catalog URLs with query parameters
        catalog_url = urljoin(base_url, '/thredds/catalog.xml')
        response = requests.get(catalog_url, timeout=10)
        
        if response.status_code == 200:
            # Simple heuristic: THREDDS 5 catalogs often have different namespace declarations
            if 'xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.2"' in response.text:
                log("Detected THREDDS version 5 from catalog namespace", verbose)
                return 5
                
    except Exception as e:
        log(f"Error detecting THREDDS version: {e}", verbose)
    
    # Default to version 4 if detection fails
    log("Defaulting to THREDDS version 4", verbose)
    return 4

def fetch_catalog(url, thredds_version=4, verbose=False):
    """Fetch and parse a THREDDS catalog from a URL"""
    log(f"Fetching catalog: {url}", verbose)
    
    try:
        # If URL ends with .html, change it to .xml
        if url.endswith('.html'):
            url = url.replace('.html', '.xml')
            
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the XML with BeautifulSoup
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

def extract_datasets(catalog, base_url, thredds_version=4, verbose=False):
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
        
        # Skip time-specific datasets (individual files in aggregations)
        if is_time_specific_dataset(name):
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
                    if thredds_version == 5:
                        # THREDDS 5 typically uses /thredds/dodsC/
                        opendap_url = urljoin(base_url.replace('/catalog/', '/dodsC/'), url_path)
                    else:
                        # THREDDS 4 URL construction
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

def find_catalog_refs(catalog, base_url, thredds_version=4, verbose=False):
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
            # Skip catalog references that appear to be time-specific collections
            if is_time_specific_dataset(name):
                log(f"Skipping time-specific catalog reference: {name}", verbose)
                continue
                
            # Skip catalog references that contain "files" or "individual" in the name
            if any(word in name.lower() for word in ['files', 'individual', 'single']):
                log(f"Skipping individual files catalog: {name}", verbose)
                continue
                
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            
            # For THREDDS 5, we might need to adjust the catalog URL structure
            if thredds_version == 5:
                # Check if the URL needs the catalog.xml suffix
                if not full_url.endswith('.xml'):
                    if full_url.endswith('/'):
                        full_url += 'catalog.xml'
                    else:
                        full_url += '/catalog.xml'
            
            refs.append((name, full_url))
            log(f"Found catalog reference: {name} -> {full_url}", verbose)
            
    return refs

def traverse_catalog(url, max_depth=5, current_depth=0, thredds_version=None, verbose=False):
    """Recursively traverse a THREDDS catalog to find datasets"""
    if current_depth > max_depth:
        log(f"Reached maximum depth ({max_depth}), stopping traversal", verbose)
        return []
    
    # Detect THREDDS version on first call
    if thredds_version is None:
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        thredds_version = detect_thredds_version(base_url, verbose)
    
    catalog = fetch_catalog(url, thredds_version, verbose)
    if not catalog:
        return []
        
    # Extract datasets from the current catalog
    datasets = extract_datasets(catalog, url, thredds_version, verbose)
    
    # Find and process catalog references
    refs = find_catalog_refs(catalog, url, thredds_version, verbose)
    for name, ref_url in refs:
        log(f"Traversing catalog reference: {name} (depth {current_depth+1})", verbose)
        child_datasets = traverse_catalog(ref_url, max_depth, current_depth + 1, thredds_version, verbose)
        datasets.extend(child_datasets)
        
    return datasets

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

def fetch_das_metadata(dataset, verbose=False):
    """Fetch DAS metadata for a dataset"""
    das_url = dataset.url + '.das'
    log(f"Fetching DAS for: {dataset.name} from {das_url}", verbose)
    
    try:
        response = requests.get(das_url, timeout=30)
        response.raise_for_status()
        
        # Parse the DAS response
        global_attrs, var_attrs = parse_das_response(response.text)
        
        dataset.metadata = global_attrs
        dataset.variables = var_attrs
        
        return True
        
    except Exception as e:
        log(f"Error fetching DAS for {dataset.name}: {e}", verbose)
        return False

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
    
    # Traverse the catalog (version will be auto-detected)
    datasets = traverse_catalog(args.url, args.max_depth, verbose=args.verbose)
    
    log(f"Found {len(datasets)} datasets with OPeNDAP access", args.verbose)
    
    # Fetch DAS metadata for each dataset
    successful = 0
    for i, dataset in enumerate(datasets):
        log(f"Processing dataset {i+1}/{len(datasets)}: {dataset.name}", args.verbose)
        if fetch_das_metadata(dataset, args.verbose):
            successful += 1
            
    log(f"Successfully fetched metadata for {successful}/{len(datasets)} datasets", args.verbose)
    
    # Create ERDDAP XML
    if successful > 0:
        create_erddap_xml(datasets, args.output, args.verbose)
        log(f"Created ERDDAP configuration: {args.output}", True)
    else:
        log("No datasets with metadata found", True)

if __name__ == "__main__":
    main()
