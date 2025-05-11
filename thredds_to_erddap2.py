#!/usr/bin/env python3
"""
THREDDS to ERDDAP Converter

This script traverses a THREDDS Data Server (TDS) catalog, extracts datasets and their metadata,
and formats them for use with ERDDAP's EDDGridFromDap datatype.

Usage:
    python thredds_to_erddap.py --url https://coastwatch.noaa.gov/thredds/catalog.xml --output erddap_datasets.xml

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
        # If URL ends with .html, change it to .xml
        if url.endswith('.html'):
            url = url.replace('.html', '.xml')
            
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

def find_catalog_refs(catalog, base_url, verbose=False):
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
        
        if href:
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
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
        
    # Extract datasets from the current catalog
    datasets = extract_datasets(catalog, url, verbose)
    
    # Find and process catalog references
    refs = find_catalog_refs(catalog, url, verbose)
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
        return True
        
    except Exception as e:
        log(f"Error fetching metadata for {dataset.name}: {e}", verbose)
        return False

def create_erddap_xml(datasets, output_file, verbose=False):
    """Create ERDDAP XML configuration from THREDDS datasets"""
    root = ET.Element("erddapDatasets")
    
    for dataset in datasets:
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
        global_attrs = ET.SubElement(attrs, "att", {"name": "."})
        for attr_name, attr_value in dataset.metadata.items():
            attr_elem = ET.SubElement(global_attrs, "att", {"name": attr_name})
            attr_elem.text = str(attr_value)
            
        # Variable attributes
        for var in dataset.variables + dataset.axes:
            var_attrs = ET.SubElement(attrs, "att", {"name": var['name']})
            for attr_name, attr_value in var['attributes'].items():
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
    
    # Traverse the catalog
    datasets = traverse_catalog(args.url, args.max_depth, verbose=args.verbose)
    
    log(f"Found {len(datasets)} datasets with OPeNDAP access", args.verbose)
    
    # Fetch metadata for each dataset
    successful = 0
    for i, dataset in enumerate(datasets):
        log(f"Processing dataset {i+1}/{len(datasets)}: {dataset.name}", args.verbose)
        if fetch_dataset_metadata(dataset, args.verbose):
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
