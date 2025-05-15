#!/usr/bin/env python3
"""
THREDDS to ERDDAP Python Generator (Multithreaded)

This script traverses a THREDDS Data Server (TDS) catalog, finds all datasets with OPeNDAP access,
and generates a Python script that calls ERDDAP's GenerateDatasetsXml.sh for each dataset.

Supports both THREDDS 4 and THREDDS 5 catalog structures.
Uses multithreading for improved performance.
Processes ALL datasets by default (both aggregated and individual time slices).
Generates unique log files for each dataset.

Usage:
    python thredds_to_erddap_generator.py --url https://coastwatch.noaa.gov/thredds/catalog.xml
                                         --output erddap_generator.py
                                         --erddap-tools /path/to/erddap/tools
                                         --logs-dir /path/to/logs/directory

Requirements:
    - requests
    - beautifulsoup4
    - lxml
"""

import argparse
import re
import os
import sys
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
        description="Traverse THREDDS catalog and generate ERDDAP script for calling GenerateDatasetsXml.sh")
    parser.add_argument("--url", required=True, help="URL to the THREDDS catalog XML")
    parser.add_argument("--output", default="erddap_generator.py", 
                      help="Output Python script file (default: erddap_generator.py)")
    parser.add_argument("--erddap-tools", required=True, 
                      help="Path to ERDDAP tools directory containing GenerateDatasetsXml.sh")
    parser.add_argument("--logs-dir", default="./logs", 
                      help="Directory where log files will be saved (default: ./logs)")
    parser.add_argument("--max-depth", type=int, default=5, 
                      help="Maximum depth for traversing catalog references (default: 5)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--threads", type=int, default=5,
                      help="Number of threads to use for catalog traversal (default: 5)")
    parser.add_argument("--process-threads", type=int, default=4,
                      help="Number of threads to use in the generated script (default: 4)")
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

def create_erddap_generator_python(datasets, output_file, erddap_tools_path, logs_dir,
                                 dataset_id_prefix="", reload_minutes="10080", max_workers=4, verbose=False):
    """Create a Python script that calls ERDDAP GenerateDatasetsXml.sh for each dataset"""
    log(f"Creating ERDDAP generator Python script with {len(datasets)} datasets", verbose)
    
    # Make sure the logs directory exists
    logs_dir = os.path.abspath(logs_dir)
    
    with open(output_file, 'w') as f:
        f.write("#!/usr/bin/env python3\n")
        f.write("# ERDDAP Dataset XML Generator Script\n")
        f.write("# Auto-generated from THREDDS catalog\n")
        f.write(f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("import os\n")
        f.write("import sys\n")
        f.write("import subprocess\n")
        f.write("import time\n")
        f.write("import concurrent.futures\n")
        f.write("from multiprocessing import cpu_count\n\n")
        
        f.write("# Configuration\n")
        f.write(f"ERDDAP_TOOLS = \"{erddap_tools_path}\"\n")
        f.write(f"LOGS_DIR = \"{logs_dir}\"\n")
        f.write(f"MAX_WORKERS = {max_workers}  # Maximum parallel processes\n\n")
        
        f.write("# Make sure logs directory exists\n")
        f.write("os.makedirs(LOGS_DIR, exist_ok=True)\n\n")
        
        f.write("# Define processor function\n")
        f.write("def process_dataset(name, url, dataset_id, reload_minutes):\n")
        f.write("    \"\"\"Process a single dataset with GenerateDatasetsXml.sh\"\"\"\n")
        f.write("    # Create safe filename from dataset name\n")
        f.write("    safe_name = ''.join(c if c.isalnum() else '_' for c in name)\n")
        f.write("    safe_name = safe_name[:50]  # Truncate if too long\n")
        f.write("    \n")
        f.write("    # Define log file path\n")
        f.write("    log_file = os.path.join(LOGS_DIR, f\"{safe_name}.log\")\n")
        f.write("    \n")
        f.write("    # Construct command\n")
        f.write("    cmd = [\n")
        f.write("        os.path.join(ERDDAP_TOOLS, \"GenerateDatasetsXml.sh\"),\n")
        f.write("        \"EDDGridFromDap\",\n")
        f.write("        url,\n")
        f.write("        str(reload_minutes)\n")
        f.write("    ]\n")
        f.write("    \n")
        f.write("    # Execute command and redirect output to log file\n")
        f.write("    print(f\"Processing {name} => {log_file}\")\n")
        f.write("    with open(log_file, 'w') as log:\n")
        f.write("        try:\n")
        f.write("            process = subprocess.run(\n")
        f.write("                cmd,\n")
        f.write("                stdout=log,\n")
        f.write("                stderr=subprocess.STDOUT,\n")
        f.write("                text=True,\n")
        f.write("                check=True\n")
        f.write("            )\n")
        f.write("            return (name, True, \"Success\")\n")
        f.write("        except subprocess.CalledProcessError as e:\n")
        f.write("            return (name, False, f\"Error (code {e.returncode})\")\n")
        f.write("        except Exception as e:\n")
        f.write("            return (name, False, str(e))\n\n")
        
        f.write("# Dataset definitions\n")
        f.write("datasets = [\n")
        for name, url, dataset_id in datasets:
            # Generate a dataset ID if not provided
            if not dataset_id:
                dataset_id = generate_dataset_id(name, dataset_id_prefix)
            elif dataset_id_prefix:
                dataset_id = dataset_id_prefix + dataset_id
                
            f.write(f"    (\"{name.replace('\"', '\\\"')}\", \"{url}\", \"{dataset_id}\", {reload_minutes}),\n")
            
        f.write("]\n\n")
        
        f.write("def main():\n")
        f.write("    # Keep track of overall stats\n")
        f.write("    total = len(datasets)\n")
        f.write("    successful = 0\n")
        f.write("    failed = 0\n")
        f.write("    \n")
        f.write("    start_time = time.time()\n")
        f.write("    print(f\"Processing {total} datasets with {MAX_WORKERS} workers...\")\n")
        f.write("    \n")
        f.write("    # Process datasets in parallel\n")
        f.write("    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:\n")
        f.write("        # Submit all tasks\n")
        f.write("        futures = [executor.submit(process_dataset, name, url, dataset_id, reload_mins) \n")
        f.write("                  for name, url, dataset_id, reload_mins in datasets]\n")
        f.write("        \n")
        f.write("        # Process results as they complete\n")
        f.write("        for i, future in enumerate(concurrent.futures.as_completed(futures)):\n")
        f.write("            name, success, message = future.result()\n")
        f.write("            if success:\n")
        f.write("                successful += 1\n")
        f.write("                print(f\"[{i+1}/{total}] ✓ {name}: {message}\")\n")
        f.write("            else:\n")
        f.write("                failed += 1\n")
        f.write("                print(f\"[{i+1}/{total}] ✗ {name}: {message}\")\n")
        f.write("    \n")
        f.write("    elapsed = time.time() - start_time\n")
        f.write("    print(f\"\\nCompleted in {elapsed:.2f} seconds\")\n")
        f.write("    print(f\"Successful: {successful}/{total}\")\n")
        f.write("    print(f\"Failed: {failed}/{total}\")\n")
        f.write("    print(f\"\\nLog files are located in: {LOGS_DIR}\")\n")
        f.write("    \n")
        f.write("    return 0 if failed == 0 else 1\n\n")
        
        f.write("if __name__ == \"__main__\":\n")
        f.write("    sys.exit(main())\n")
    
    # Make the file executable
    os.chmod(output_file, 0o755)
    
    log(f"Created ERDDAP generator Python script: {output_file}", True)
    log(f"It contains commands for {len(datasets)} datasets", True)
    log(f"Log files will be saved to: {logs_dir}", True)

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
    
    # Create the ERDDAP generator Python script
    create_erddap_generator_python(
        datasets, 
        args.output, 
        args.erddap_tools,
        args.logs_dir,
        dataset_id_prefix=args.datasetid_prefix,
        reload_minutes=args.reloadEveryNMinutes,
        max_workers=args.process_threads,
        verbose=args.verbose
    )
    
    total_time = time.time() - start_time
    log(f"Total processing time: {total_time:.2f} seconds", args.verbose)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
