#!/usr/bin/env python3
"""
Hyrax to ERDDAP Converter

This script traverses a Hyrax OPeNDAP server, extracts datasets and their metadata,
and formats them for use with ERDDAP's EDDGridFromDap datatype.

Hyrax servers use a different catalog structure than THREDDS, so this tool
specifically parses Hyrax HTML catalog pages and DAP endpoints.

Usage:
    python hyrax_to_erddap.py --url https://opendap.example.edu/opendap/ --output erddap_datasets.xml

Requirements:
    - requests
    - beautifulsoup4
    - netCDF4
    - lxml
"""

import argparse
import os
import re
import time
import uuid
from urllib.parse import urljoin, urlparse, unquote
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
import netCDF4 as nc


class HyraxDataset:
    """Class to hold metadata for a Hyrax dataset"""
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
    parser = argparse.ArgumentParser(description="Traverse Hyrax OPeNDAP server and convert to ERDDAP configuration")
    parser.add_argument("--url", required=True, help="URL to the Hyrax OPeNDAP server")
    parser.add_argument("--output", default="erddap_datasets.xml", help="Output XML file for ERDDAP configuration")
    parser.add_argument("--max-depth", type=int, default=5, help="Maximum depth for traversing directories")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds")
    return parser.parse_args()


def log(message, verbose=False):
    """Log messages if verbose is enabled"""
    if verbose:
        print(message)


def fetch_hyrax_page(url, verbose=False):
    """Fetch and parse a Hyrax catalog page"""
    log(f"Fetching Hyrax page: {url}", verbose)
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching page: {e}", verbose)
        return None


def is_dataset_link(link, hyrax_extensions):
    """Check if a link points to a dataset based on file extension"""
    href = link.get('href', '')
    return any(href.endswith(ext) for ext in hyrax_extensions)


def is_directory_link(link):
    """Check if a link points to a directory (ends with /)"""
    href = link.get('href', '')
    return href.endswith('/') and not href.startswith('..')


def extract_datasets_from_hyrax_page(soup, base_url, hyrax_extensions, verbose=False):
    """Extract dataset links from a Hyrax page"""
    datasets = []
    
    # Find all links in the page
    for link in soup.find_all('a'):
        href = link.get('href', '')
        
        # Skip parent directory or invalid links
        if not href or href.startswith('..') or href == '/':
            continue
            
        # Is this a dataset link?
        if is_dataset_link(link, hyrax_extensions):
            dataset_name = unquote(href.rstrip('/'))
            
            # Form the OPeNDAP URL
            if '.dods' in href or '.dds' in href or '.das' in href:
                # Strip the extension to get the base URL
                opendap_url = urljoin(base_url, href.rsplit('.', 1)[0])
            else:
                # Use as is for files like .nc, .hdf, etc.
                opendap_url = urljoin(base_url, href)
            
            dataset = HyraxDataset(dataset_name, opendap_url)
            datasets.append(dataset)
            log(f"Found dataset: {dataset}", verbose)
            
    return datasets


def find_directory_links(soup, base_url, verbose=False):
    """Find all directory links in a Hyrax page"""
    directories = []
    
    for link in soup.find_all('a'):
        if is_directory_link(link):
            href = link.get('href', '')
            dir_name = unquote(href.rstrip('/'))
            dir_url = urljoin(base_url, href)
            directories.append((dir_name, dir_url))
            log(f"Found directory: {dir_name} -> {dir_url}", verbose)
            
    return directories


def traverse_hyrax_server(url, max_depth=5, current_depth=0, verbose=False, delay=0.5, 
                         hyrax_extensions=['.nc', '.nc4', '.hdf', '.h5']):
    """Recursively traverse a Hyrax server to find datasets"""
    if current_depth > max_depth:
        log(f"Reached maximum depth ({max_depth}), stopping traversal", verbose)
        return []
        
    soup = fetch_hyrax_page(url, verbose)
    if not soup:
        return []
    
    # Get the base URL
    parsed_url = urlparse(url)
    base_url = url
    
    # Introduce a small delay to avoid overloading the server
    time.sleep(delay)
    
    # Extract datasets from the current page
    datasets = extract_datasets_from_hyrax_page(soup, base_url, hyrax_extensions, verbose)
    
    # Find and process directory links
    dirs = find_directory_links(soup, base_url, verbose)
    for dir_name, dir_url in dirs:
        log(f"Traversing directory: {dir_name} (depth {current_depth+1})", verbose)
        child_datasets = traverse_hyrax_server(
            dir_url, max_depth, current_depth + 1, verbose, delay, hyrax_extensions
        )
        datasets.extend(child_datasets)
        
    return datasets


def extract_das_metadata(dataset_url, verbose=False):
    """Extract metadata from a DAP DAS response"""
    metadata = {}
    
    das_url = f"{dataset_url}.das"
    log(f"Fetching DAS metadata from: {das_url}", verbose)
    
    try:
        response = requests.get(das_url)
        response.raise_for_status()
        
        # Parse the DAS response
        das_text = response.text
        
        # Extract global attributes
        global_attrs_match = re.search(r'Attributes\s*{(.*?)}', das_text, re.DOTALL)
        if global_attrs_match:
            global_attrs_text = global_attrs_match.group(1)
            
            # Parse attributes
            attr_pattern = r'\s*([\w]+)\s*([\w]+)\s*"([^"]*)";'
            for match in re.finditer(attr_pattern, global_attrs_text):
                attr_type, attr_name, attr_value = match.groups()
                metadata[attr_name] = attr_value
        
        log(f"Extracted {len(metadata)} global attributes", verbose)
        return metadata
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching DAS metadata: {e}", verbose)
        return metadata


def fetch_dataset_metadata(dataset, verbose=False, retry_attempts=3):
    """Fetch metadata for a dataset by reading its OPeNDAP endpoint"""
    log(f"Fetching metadata for dataset: {dataset.name}", verbose)
    
    # First try to get metadata from the DAS response
    das_metadata = extract_das_metadata(dataset.url, verbose)
    dataset.metadata.update(das_metadata)
    
    # Then try to open with netCDF4 for more detailed metadata
    attempts = 0
    while attempts < retry_attempts:
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
            break
            
        except Exception as e:
            attempts += 1
            log(f"Error fetching metadata for {dataset.name} (attempt {attempts}/{retry_attempts}): {e}", verbose)
            if attempts < retry_attempts:
                time.sleep(1)  # Wait a bit before retrying
            
    # If we couldn't get variable info but have some metadata, we can still use the dataset
    if not dataset.variables and not dataset.axes and dataset.metadata:
        log(f"Only partial metadata available for {dataset.name}, but continuing", verbose)
        return
        
    # If we couldn't get any metadata at all, log a warning
    if not dataset.metadata and not dataset.variables and not dataset.axes:
        log(f"WARNING: Could not fetch any metadata for {dataset.name}", verbose)


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


def detect_hyrax_dataset_extensions(url, verbose=False):
    """Detect the file extensions used for datasets in this Hyrax server"""
    log(f"Detecting dataset extensions for Hyrax server: {url}", verbose)
    
    # Default extensions to look for
    default_extensions = ['.nc', '.nc4', '.hdf', '.h5', '.grib', '.grb', '.dods', '.dds', '.das']
    
    # Try to detect from the page
    soup = fetch_hyrax_page(url, verbose)
    if not soup:
        log(f"Could not fetch Hyrax page, using default extensions", verbose)
        return default_extensions
        
    # Look for links that might be datasets
    extensions = set()
    for link in soup.find_all('a'):
        href = link.get('href', '')
        
        # Skip parent directory or invalid links
        if not href or href.startswith('..') or href == '/' or href.endswith('/'):
            continue
            
        # Extract extension
        if '.' in href:
            ext = '.' + href.rsplit('.', 1)[1]
            extensions.add(ext)
            
    # If we found any extensions, use them; otherwise use defaults
    detected_extensions = list(extensions) if extensions else default_extensions
    log(f"Detected dataset extensions: {detected_extensions}", verbose)
    return detected_extensions


def write_xml_to_file(xml_content, output_file):
    """Write the XML content to a file"""
    with open(output_file, 'w') as f:
        f.write(xml_content)


def main():
    """Main function to run the script"""
    args = parse_args()
    
    print(f"Starting Hyrax server traversal from {args.url}")
    
    # Detect dataset extensions for this server
    extensions = detect_hyrax_dataset_extensions(args.url, args.verbose)
    
    # Traverse the server
    datasets = traverse_hyrax_server(
        args.url, args.max_depth, verbose=args.verbose, 
        delay=args.delay, hyrax_extensions=extensions
    )
    print(f"Found {len(datasets)} datasets with OPeNDAP access")
    
    # Fetch metadata for each dataset
    for i, dataset in enumerate(datasets):
        print(f"Processing dataset {i+1}/{len(datasets)}: {dataset.name}")
        fetch_dataset_metadata(dataset, verbose=args.verbose)
        # Add a small delay to avoid overloading the server
        time.sleep(args.delay)
    
    # Generate ERDDAP XML
    xml_content = generate_erddap_xml(datasets, verbose=args.verbose)
    
    # Write to output file
    write_xml_to_file(xml_content, args.output)
    print(f"ERDDAP configuration written to {args.output}")


if __name__ == "__main__":
    main()
    