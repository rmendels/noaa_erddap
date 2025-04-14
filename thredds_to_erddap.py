#!/usr/bin/env python3
"""
THREDDS to ERDDAP Converter

This script traverses a THREDDS Data Server (TDS) catalog, extracts datasets and their metadata,
and formats them for use with ERDDAP's EDDGridFromDap datatype.

Usage:
    python thredds_to_erddap.py --url https://tds.example.edu/thredds/catalog.xml --output erddap_datasets.xml

Requirements:
    - requests
    - beautifulsoup4
    - netCDF4
    - lxml
"""

import argparse
import os
import re
import uuid
from urllib.parse import urljoin, urlparse
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
import netCDF4 as nc

# THREDDS namespaces
THREDDS_NS = {
    'default': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0',
    'xlink': 'http://www.w3.org/1999/xlink'
}

class ThreddsDataset:
    """Class to hold metadata for a THREDDS dataset"""
    def __init__(self, name, url, id=None):
        self.name = name
        self.url = url
        self.id = id or self._generate_id()
        self.metadata = {}
        self.variables = []
        self.axes = []
        
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
    return parser.parse_args()

def log(message, verbose=False):
    """Log messages if verbose is enabled"""
    if verbose:
        print(message)

def fetch_catalog(url, verbose=False):
    """Fetch and parse a THREDDS catalog from a URL"""
    log(f"Fetching catalog: {url}", verbose)
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the XML with BeautifulSoup for more flexibility
        soup = BeautifulSoup(response.content, 'xml')
        return soup
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching catalog: {e}", verbose)
        return None

def extract_datasets(catalog, base_url, verbose=False):
    """Extract datasets from a THREDDS catalog"""
    datasets = []
    
    # Find all dataset elements with an OPeNDAP access URL
    for dataset_elem in catalog.find_all('dataset'):
        # Skip if it doesn't have a name attribute
        if not dataset_elem.has_attr('name'):
            continue
            
        name = dataset_elem['name']
        
        # Look for OPeNDAP service reference
        dataset_id = dataset_elem.get('ID', None)
        
        # Check for access elements with OPeNDAP service
        opendap_url = None
        
        for access in dataset_elem.find_all('access'):
            if access.get('serviceName', '').lower() == 'opendap':
                opendap_url = urljoin(base_url, access.get('urlPath', ''))
                break
                
        # If we found an OPeNDAP URL, create a dataset object
        if opendap_url:
            dataset = ThreddsDataset(name, opendap_url, dataset_id)
            datasets.append(dataset)
            log(f"Found dataset: {dataset}", verbose)
            
    return datasets

def find_catalog_refs(catalog, base_url, verbose=False):
    """Find all catalogRef elements in a THREDDS catalog"""
    refs = []
    
    for ref_elem in catalog.find_all('catalogRef'):
        if ref_elem.has_attr('xlink:href'):
            href = ref_elem['xlink:href']
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            # Get the name if available
            name = ref_elem.get('xlink:title', href)
            refs.append((name, full_url))
            log(f"Found catalog reference: {name} -> {full_url}", verbose)
            
    return refs

def traverse_catalog(url, max_depth=5, current_depth=0, verbose=False):
    """Recursively traverse a THREDDS catalog to find datasets"""
    if current_depth > max_depth:
        log(f"Reached maximum depth ({max_depth}), stopping traversal", verbose)
        return []
        
    catalog = fetch_catalog(url, verbose)
    if not catalog:
        return []
        
    # Get the base URL for resolving relative URLs
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    # Extract datasets from the current catalog
    datasets = extract_datasets(catalog, base_url, verbose)
    
    # Find and process catalog references
    refs = find_catalog_refs(catalog, base_url, verbose)
    for name, ref_url in refs:
        log(f"Traversing catalog reference: {name} (depth {current_depth+1})", verbose)
        child_datasets = traverse_catalog(ref_url, max_depth, current_depth + 1, verbose)
        datasets.extend(child_datasets)
        
    return datasets

def fetch_dataset_metadata(dataset, verbose=False):
    """Fetch metadata for a dataset by reading its OPeNDAP endpoint"""
    log(f"Fetching metadata for dataset: {dataset.name}", verbose)
    
    try:
        # Open the dataset with netCDF4
        ds = nc.Dataset(dataset.url)
        
        # Extract global attributes
        for attr_name in ds.ncattrs():
            dataset.metadata[attr_name] = ds.getncattr(attr_name)
            
        # Extract axes and variables
        for var_name, var in ds.variables.items():
            # Determine if this is an axis variable
            is_axis = len(var.dimensions) == 1 and var_name in var.dimensions
            
            var_info = {
                'name': var_name,
                'dimensions': var.dimensions,
                'shape': var.shape,
                'attributes': {},
                'dataType': var.dtype.name
            }
            
            # Get variable attributes
            for attr_name in var.ncattrs():
                var_info['attributes'][attr_name] = var.getncattr(attr_name)
                
            if is_axis:
                dataset.axes.append(var_info)
            else:
                dataset.variables.append(var_info)
                
        ds.close()
        log(f"Successfully fetched metadata for: {dataset.name}", verbose)
        
    except Exception as e:
        log(f"Error fetching metadata for {dataset.name}: {e}", verbose)

def generate_erddap_xml(datasets, verbose=False):
    """Generate ERDDAP dataset XML configuration"""
    # Create the root element
    root = ET.Element("erddapDatasets")
    
    for dataset in datasets:
        log(f"Generating ERDDAP XML for: {dataset.name}", verbose)
        
        # Create dataset element
        ds_elem = ET.SubElement(root, "dataset", type="EDDGridFromDap", datasetID=dataset.id)
        
        # Add basic info
        ET.SubElement(ds_elem, "sourceUrl").text = dataset.url
        ET.SubElement(ds_elem, "reloadEveryNMinutes").text = "60"  # Default reload time
        
        # Set a default accessibility value - customize as needed
        ET.SubElement(ds_elem, "accessible").text = "true"
        
        # Add dataset title and summary from metadata
        if 'title' in dataset.metadata:
            ET.SubElement(ds_elem, "title").text = dataset.metadata['title']
        else:
            ET.SubElement(ds_elem, "title").text = dataset.name
            
        if 'summary' in dataset.metadata:
            ET.SubElement(ds_elem, "summary").text = dataset.metadata['summary']
        elif 'description' in dataset.metadata:
            ET.SubElement(ds_elem, "summary").text = dataset.metadata['description']
        else:
            ET.SubElement(ds_elem, "summary").text = f"Data from {dataset.name}"
        
        # Add other global metadata attributes
        for key, value in dataset.metadata.items():
            if key not in ['title', 'summary', 'description']:
                # Skip special attributes already handled
                attr_elem = ET.SubElement(ds_elem, "addAttributes")
                ET.SubElement(attr_elem, key).text = str(value)
        
        # Add axes information
        for axis in dataset.axes:
            axis_elem = ET.SubElement(ds_elem, "axisVariable")
            ET.SubElement(axis_elem, "sourceName").text = axis['name']
            ET.SubElement(axis_elem, "destinationName").text = axis['name']
            
            # Add standard names, units, etc. from attributes
            if axis['attributes']:
                attr_elem = ET.SubElement(axis_elem, "addAttributes")
                for attr_key, attr_value in axis['attributes'].items():
                    ET.SubElement(attr_elem, attr_key).text = str(attr_value)
        
        # Add data variables
        for var in dataset.variables:
            var_elem = ET.SubElement(ds_elem, "dataVariable")
            ET.SubElement(var_elem, "sourceName").text = var['name']
            ET.SubElement(var_elem, "destinationName").text = var['name']
            
            # Add variable attributes
            if var['attributes']:
                attr_elem = ET.SubElement(var_elem, "addAttributes")
                for attr_key, attr_value in var['attributes'].items():
                    ET.SubElement(attr_elem, attr_key).text = str(attr_value)
    
    # Convert to a pretty-printed XML string
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def write_xml_to_file(xml_content, output_file):
    """Write the XML content to a file"""
    with open(output_file, 'w') as f:
        f.write(xml_content)

def main():
    """Main function to run the script"""
    args = parse_args()
    
    print(f"Starting THREDDS catalog traversal from {args.url}")
    datasets = traverse_catalog(args.url, args.max_depth, verbose=args.verbose)
    print(f"Found {len(datasets)} datasets with OPeNDAP access")
    
    # Fetch metadata for each dataset
    for dataset in datasets:
        fetch_dataset_metadata(dataset, verbose=args.verbose)
    
    # Generate ERDDAP XML
    xml_content = generate_erddap_xml(datasets, verbose=args.verbose)
    
    # Write to output file
    write_xml_to_file(xml_content, args.output)
    print(f"ERDDAP configuration written to {args.output}")

if __name__ == "__main__":
    main()
