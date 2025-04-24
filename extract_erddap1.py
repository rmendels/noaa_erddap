import requests
import pandas as pd
from io import StringIO  # Import StringIO from io module, not pandas
import os
from xml.sax.saxutils import escape

def extract_erddap_datasets(base_url, debug=False):
    """
    Extract information about all datasets from an ERDDAP server.
    
    Args:
        base_url (str): The base URL of the ERDDAP server, e.g., 'https://coastwatch.pfeg.noaa.gov/erddap'
        debug (bool): Whether to print debug information
        
    Returns:
        list: List of dictionaries containing information about each dataset
    """
    # Ensure the base URL doesn't end with a slash
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    
    # URL for all datasets in CSV format - this endpoint contains all datasets
    all_datasets_url = f"{base_url}/tabledap/allDatasets.csv"
    
    datasets = []
    skipped_datasets = []
    
    try:
        print(f"Fetching all datasets from {all_datasets_url}")
        response = requests.get(all_datasets_url)
        response.raise_for_status()
        
        # Save raw CSV for debugging if needed
        if debug:
            with open("debug_raw_response.csv", "w") as f:
                f.write(response.text)
            print(f"Raw response saved to debug_raw_response.csv")
        
        # Parse CSV data using pandas
        df = pd.read_csv(StringIO(response.text), skiprows=[1])  # Skip the units row
        
        # Save parsed dataframe for debugging if needed
        if debug:
            df.to_csv("debug_parsed_dataframe.csv", index=False)
            print(f"Parsed dataframe saved to debug_parsed_dataframe.csv")
            print(f"Total rows in parsed dataframe: {len(df)}")
        
        # Process each dataset row
        for index, row in df.iterrows():
            try:
                dataset_id = row['datasetID']
                
                # Determine if it's a grid or table based on the dataset's data type
                dataset_type = "table"  # Default to table
                
                # Check dataStructure column if available
                if 'dataStructure' in row:
                    if str(row['dataStructure']).lower() == 'grid':
                        dataset_type = "grid"
                
                # Also check cdm_data_type column if available
                elif 'cdm_data_type' in row:
                    if str(row['cdm_data_type']).lower() == 'grid':
                        dataset_type = "grid"
                
                dataset_info = {
                    'datasetID': dataset_id,
                    'type': dataset_type,
                    'base_url': base_url,
                    'title': row.get('title', '')
                }
                datasets.append(dataset_info)
                
            except Exception as e:
                error_msg = f"Error processing row {index}: {e}"
                print(error_msg)
                if debug:
                    print(f"Problematic row: {row}")
                skipped_datasets.append((index, str(e)))
        
        print(f"Found {len(datasets)} total datasets")
        print(f"Skipped {len(skipped_datasets)} datasets due to errors")
        if skipped_datasets and debug:
            print("Skipped datasets (row index, error):")
            for skip_info in skipped_datasets:
                print(f"  - Row {skip_info[0]}: {skip_info[1]}")
        
        table_count = sum(1 for d in datasets if d['type'] == 'table')
        grid_count = sum(1 for d in datasets if d['type'] == 'grid')
        print(f" - Tables: {table_count}")
        print(f" - Grids: {grid_count}")
        
    except Exception as e:
        print(f"Error processing datasets: {e}")
        raise Exception(f"Failed to retrieve datasets: {e}")
    
    return datasets

def generate_dataset_xml(dataset_id, base_url, dataset_type="grid", reload_minutes=180):
    """
    Generate an XML snippet by substituting placeholders in a template.
    
    Args:
        dataset_id (str): The dataset ID to substitute
        base_url (str): The base URL to substitute
        dataset_type (str): Either "grid" or "table"
        reload_minutes (int): Value for reloadEveryNMinutes
        
    Returns:
        str: XML snippet with substituted values
    """
    # Ensure base_url doesn't end with a slash
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    
    # Escape special characters in dataset_id
    safe_dataset_id = escape(dataset_id)
    
    # Choose the appropriate type based on whether it's a grid or table
    edd_type = "EDDGridFromErddap" if dataset_type.lower() == "grid" else "EDDTableFromErddap"
    
    # Determine the appropriate dap path based on dataset type
    #dap_path = "/erddap/griddap" if dataset_type.lower() == "grid" else "/erddap/tabledap"
    dap_path = "/griddap" if dataset_type.lower() == "grid" else "/tabledap"
    
    # XML template with placeholders
    template = f"""<dataset type="{edd_type}" datasetID="{safe_dataset_id}" active="true">
    <reloadEveryNMinutes>{reload_minutes}</reloadEveryNMinutes>
    <sourceUrl>{base_url}{dap_path}/{safe_dataset_id}</sourceUrl>
</dataset>"""
    
    return template

def write_datasets_to_file(datasets, output_file, reload_minutes=180):
    """
    Write XML snippets for all datasets to a file.
    
    Args:
        datasets (list): List of dataset dictionaries
        output_file (str): Path to output file
        reload_minutes (int): Value for reloadEveryNMinutes
    """
    # Create directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Open file for writing (overwrite if exists)
    with open(output_file, 'w') as f:
        # Write header
        f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        f.write("<erddapDatasets>\n\n")
        
        # Write each dataset
        for dataset in datasets:
            xml = generate_dataset_xml(
                dataset_id=dataset['datasetID'],
                base_url=dataset['base_url'],
                dataset_type=dataset['type'],
                reload_minutes=reload_minutes
            )
            
            f.write(xml + "\n\n")
        
        # Write footer
        f.write("</erddapDatasets>\n")
    
    print(f"Successfully wrote {len(datasets)} XML snippets to {output_file}")

def main():
    # Example usage - replace with your ERDDAP URL
    #erddap_url = "https://coastwatch.pfeg.noaa.gov/erddap"
    #erddap_url = "https://polarwatch.noaa.gov/erddap"
    #erddap_url = "https://coastwatch.noaa.gov/erddap"
    #erddap_url = "https://erddap.cencoos.org/erddap"
    #erddap_url = "https://erddap.sccoos.org/erddap"
    #erddap_url = "https://erddap.sensors.ioos.us/erddap"
    #erddap_url = "https://cwcgom.aoml.noaa.gov/erddap"
    #erddap_url = "https://oceanwatch.pifsc.noaa.gov/erddap"
    #erddap_url = "https://www.ncei.noaa.gov/erddap"
    #erddap_url = "https://osmc.noaa.gov/erddap/"
    #erddap_url = "https://apps.glerl.noaa.gov/erddap"
    #erddap_url = "https://ferret.pmel.noaa.gov/pmel/erddap"
    #erddap_url = "https://erddap.aoml.noaa.gov/gdp/erddap"
    #erddap_url = "https://comet.nefsc.noaa.gov/erddap"
    #erddap_url = "https://ferret.pmel.noaa.gov/alamo/erddap"
    #erddap_url = "https://ferret.pmel.noaa.gov/socat/erddap"
    #erddap_url = "https://pub-data.diver.orr.noaa.gov/erddap"
    #erddap_url = "https://gliders.ioos.us/erddap"
    #erddap_url = "https://erddap.nanoos.org/erddap"
    #erddap_url = "https://www.neracoos.org/erddap"
    #erddap_url = "https://erddap.maracoos.org/erddap"
    #erddap_url = "https://erddap.secoora.org/erddap"
    #erddap_url = "https://seagull-erddap.glos.org/erddap"
    #erddap_url = "https://erddap.gcoos.org/erddap"
    #erddap_url = "https://gcoos5.geos.tamu.edu/erddap"
    #erddap_url = "https://data.neracoos.org/erddap"
    #erddap_url = 'https://oos.soest.hawaii.edu/erddap
    #erddap_url = "https://pae-paha.pacioos.hawaii.edu/erddap"
    #erddap_url = "https://oceanview.pfeg.noaa.gov/erddap"
    # erddap_url = "https://erddap.aoos.org/erddap"
    erddap_url = "https://atn.ioos.us/erddap"
    output_file = "atn_erddap_datasets.xml"
    reload_minutes = 1440
    debug = True  # Enable debug mode
    
    print(f"Extracting datasets from {erddap_url}")
    datasets = extract_erddap_datasets(erddap_url, debug)
    
    # Print summary
    tables_count = sum(1 for d in datasets if d['type'] == 'table')
    grids_count = sum(1 for d in datasets if d['type'] == 'grid')
    
    print(f"Found {len(datasets)} total datasets:")
    print(f" - Tables: {tables_count}")
    print(f" - Grids: {grids_count}")
    
    # Write to file
    write_datasets_to_file(datasets, output_file, reload_minutes)
    
    # Print a few examples for verification
    if datasets:
        print("\nFirst 3 dataset entries:")
        for i, dataset in enumerate(datasets[:3]):
            print(f"{i+1}. {dataset['datasetID']} ({dataset['type']})")
            
    # Try alternate method to count datasets
    try:
        # Directly check the number of datasets from the catalog
        catalog_url = f"{erddap_url}/info/index.json"
        print(f"\nChecking total datasets in catalog at {catalog_url}")
        response = requests.get(catalog_url)
        response.raise_for_status()
        catalog_data = response.json()
        
        if 'table' in catalog_data:
            table_rows = catalog_data['table']['rows']
            print(f"Total datasets in catalog: {len(table_rows)}")
            
            if len(table_rows) > len(datasets):
                print(f"DISCREPANCY: Catalog shows {len(table_rows)} datasets but we processed {len(datasets)}")
                
                if debug:
                    # Save catalog data for comparison
                    import json
                    with open("debug_catalog_data.json", "w") as f:
                        json.dump(catalog_data, f, indent=2)
                    print("Catalog data saved to debug_catalog_data.json")
    except Exception as e:
        print(f"Error checking catalog: {e}")
        # Continue with the datasets we have
if __name__ == "__main__":
    main()
