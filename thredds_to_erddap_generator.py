#!/usr/bin/env python3
"""
THREDDS to ERDDAP Script Generator (Multithreaded)

This script traverses a THREDDS Data Server (TDS) catalog, finds all datasets with OPeNDAP access,
and generates a shell script with calls to ERDDAP's GenerateDatasetsXml.sh for each dataset.

Supports both THREDDS 4 and THREDDS 5 catalog structures.
Uses multithreading for improved performance.
Processes ALL datasets by default (both aggregated and individual time slices).

Usage:
    python thredds_to_erddap_generator.py --url https://coastwatch.noaa.gov/thredds/catalog.xml
                                         --output erddap_generator.sh
                                         --erddap-tools /path/to/erddap/tools

Requirements:
    - requests
    - beautifulsoup4
    - lxml
"""

import argparse
import re
import os
from urllib.parse import urljoin, urlparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import requests
from bs4 import BeautifulSoup

# Thread-safe print lock
print_lock = threading.Lock()

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Traverse THREDDS catalog and generate ERDDAP GenerateDatasetsXml.sh calls")
    parser.add_argument("--url", required=True, help="URL to the THREDDS catalog XML")
    parser.add_argument("--output", default="erddap_generator.sh", 
                      help="Output shell script file (default: erddap_generator.sh)")
    parser.add_argument("--erddap-tools", required=True, 
                      help="Path to ERDDAP tools directory containing GenerateDatasetsXml.sh")
    parser.add_argument("--max-depth", type=int, default=5, 
                      help="Maximum depth for traversing catalog references (default: 5)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--threads", type=int, default=5,
                      help="Number of threads to use for catalog traversal (default: 5)")
    parser.add_argument("--filter", action="store_true", 
                      help="Filter out time-specific datasets and catalogs (default: False)")
    parser.add_argument("--datasetid-prefix", default="",
                      help="Prefix to add to dataset IDs (default: none)")
    parser.add_argument("--reloadEveryNMinutes", default="10080",
                      help="reloadEveryNMinutes parameter for ERDDAP (default: 10080 [1 week])")
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

def fetch_catalog(url, verbose=False):
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

def construct_opendap_url(base_url, url_path, thredds_version=4):
    """Safely construct an OPeNDAP URL to avoid path duplication"""
    # Handle PSL and similar sites with URL path duplication issues
    if "psl.noaa.gov" in base_url:
        # Extract the server base URL
        parsed = urlparse(base_url)
        server_base = f"{parsed.scheme}://{parsed.netloc}"
        
        # Create the OPeNDAP URL with the correct path
        return f"{server_base}/thredds/dodsC/{url_path}"
    
    # Standard URL construction
    if '/thredds/catalog/' in base_url:
        # Convert catalog URL to OPeNDAP URL
        opendap_base = base_url.replace('/catalog/', '/dodsC/')
        return urljoin(opendap_base, url_path)
    else:
        # Regular URL join for other cases
        return urljoin(base_url, url_path)

def generate_dataset_id(name, prefix=""):
    """Generate a valid ERDDAP dataset ID from the dataset name"""
    # Replace spaces and special characters with underscores
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    # Ensure it starts with a letter
    if not clean_name[0].isalpha():
        clean_name = "ds_" + clean_name
    # Add prefix if provided
    if prefix:
        clean_name = prefix + clean_name
    # Lowercase for consistency
    return clean_name.lower()

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
                    # Use improved URL construction to avoid duplication
                    opendap_url = construct_opendap_url(base_url, url_path, thredds_version)
                    break
        
        if not opendap_url and dataset_elem.has_attr('urlPath'):
            url_path = dataset_elem['urlPath']
            for service_name, service_type in services.items():
                if service_type.lower() in ['opendap', 'dods']:
                    # Use improved URL construction to avoid duplication
                    opendap_url = construct_opendap_url(base_url, url_path, thredds_version)
                    break
        
        if opendap_url:
            datasets.append((name, opendap_url, dataset_id))
            log(f"Found dataset: {name}, URL: {opendap_url}", verbose)
            
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
    
    catalog = fetch_catalog(url, verbose)
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

def create_erddap_generator_script(datasets, output_file, erddap_tools_path, 
                                 dataset_id_prefix="", reload_minutes="10080", verbose=False):
    """Create a shell script with ERDDAP GenerateDatasetsXml.sh commands for each dataset"""
    log(f"Creating ERDDAP generator script with {len(datasets)} datasets", verbose)
    
    # Use the path as provided without checking existence
    generate_script = os.path.join(erddap_tools_path, "GenerateDatasetsXml.sh")
    
    # Create the shell script with appropriate header
    with open(output_file, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("# ERDDAP Dataset XML Generator Script\n")
        f.write("# Auto-generated from THREDDS catalog\n")
        f.write(f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("# Stop on errors\n")
        f.write("set -e\n\n")
        
        # Write a function to run tasks in parallel
        f.write("# Function to run commands in parallel with a maximum of N jobs\n")
        f.write("run_parallel() {\n")
        f.write("    local max_jobs=$1\n")
        f.write("    shift\n")
        f.write("    local cmds=( \"$@\" )\n")
        f.write("    local running=0\n")
        f.write("    local pids=()\n")
        f.write("    local cmds_index=0\n")
        f.write("    local cmds_len=${#cmds[@]}\n\n")
        
        f.write("    while (( cmds_index < cmds_len || running > 0 )); do\n")
        f.write("        # Start jobs while we have slots and commands\n")
        f.write("        while (( running < max_jobs && cmds_index < cmds_len )); do\n")
        f.write("            eval \"${cmds[cmds_index]}\" &\n")
        f.write("            pids+=($!)\n")
        f.write("            ((running++))\n")
        f.write("            ((cmds_index++))\n")
        f.write("            echo \"Started job $cmds_index/$cmds_len (${running} running)\"\n")
        f.write("        done\n\n")
        
        f.write("        # Wait for any job to finish\n")
        f.write("        if (( running > 0 )); then\n")
        f.write("            wait -n\n")
        f.write("            # Find which pid finished\n")
        f.write("            local alive_pids=()\n")
        f.write("            for pid in \"${pids[@]}\"; do\n")
        f.write("                if kill -0 $pid 2>/dev/null; then\n")
        f.write("                    alive_pids+=($pid)\n")
        f.write("                fi\n")
        f.write("            done\n")
        f.write("            pids=(\"${alive_pids[@]}\")\n")
        f.write("            ((running--))\n")
        f.write("        fi\n")
        f.write("    done\n")
        f.write("}\n\n")
        
        # Write variables section
        f.write("# Variables\n")
        f.write(f"ERDDAP_TOOLS=\"{erddap_tools_path}\"\n")
        f.write("MAX_JOBS=4  # Maximum parallel jobs\n\n")
        
        f.write("# Create array of commands\n")
        f.write("COMMANDS=(\n")
        
        # Add a command for each dataset
        for name, url, dataset_id in datasets:
            # Generate a dataset ID if not provided
            if not dataset_id:
                dataset_id = generate_dataset_id(name, dataset_id_prefix)
            elif dataset_id_prefix:
                dataset_id = dataset_id_prefix + dataset_id
                
            # Create the command with direct command-line arguments
            # Format: GenerateDatasetsXml.sh EDDGridFromDap URL ReloadEveryNMinutes
            command = f"\"$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap '{url}' {reload_minutes}\""
            
            f.write(f"  {command}\n")
        
        f.write(")\n\n")
        
        # Run the commands in parallel
        f.write("# Run commands in parallel\n")
        f.write("run_parallel $MAX_JOBS \"${COMMANDS[@]}\"\n\n")
        
        f.write("echo \"All done! Generated XML for ${#COMMANDS[@]} datasets.\"\n")
    
    # Make the file executable
    os.chmod(output_file, 0o755)
    
    log(f"Created ERDDAP generator script: {output_file}", True)
    log(f"It contains commands for {len(datasets)} datasets", True)

def main():
    args = parse_args()
    
    log(f"Starting THREDDS catalog traversal from: {args.url}", args.verbose)
    
    start_time = time.time()
    
    # Parallel catalog traversal - filter_time_specific is False by default unless --filter is specified
    datasets = traverse_catalog_parallel(args.url, args.max_depth, 
                                      filter_time_specific=args.filter,
                                      verbose=args.verbose, 
                                      max_workers=args.threads)
    
    catalog_time = time.time() - start_time
    log(f"Found {len(datasets)} datasets with OPeNDAP access in {catalog_time:.2f} seconds", args.verbose)
    
    # Create the ERDDAP generator script
    create_erddap_generator_script(
        datasets, 
        args.output, 
        args.erddap_tools,
        dataset_id_prefix=args.datasetid_prefix,
        reload_minutes=args.reloadEveryNMinutes,
        verbose=args.verbose
    )
    
    total_time = time.time() - start_time
    log(f"Total processing time: {total_time:.2f} seconds", args.verbose)
    
    return 0

if __name__ == "__main__":
    exit(main())
    