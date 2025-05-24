#!/usr/bin/env python3
"""
Script to process subscription_list.txt and send HTTP requests to action URLs
"""

import requests
import time
import sys
from urllib.parse import urlparse

def process_subscription_file(filename="subscription_list.txt"):
    """
    Read the subscription file and process pending entries using 'to validate' URLs
    """
    try:
        with open(filename, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    pending_entries = []
    valid_entries = []
    current_dataset = None
    current_status = None
    current_validate_url = None
    
    # Parse the file to extract entries
    for line in lines:
        line = line.strip()
        
        if line.startswith('datasetID:'):
            current_dataset = line.split(':', 1)[1].strip()
        elif line.startswith('status:'):
            current_status = line.split(':', 1)[1].strip()
        elif line.startswith('to validate:'):
            current_validate_url = line.split(':', 1)[1].strip()
            
            # When we have all three pieces of info, categorize the entry
            if current_dataset and current_status and current_validate_url:
                if current_status == 'pending':
                    pending_entries.append((current_dataset, current_validate_url))
                elif current_status == 'valid':
                    valid_entries.append(current_dataset)
                
                # Reset for next entry
                current_dataset = None
                current_status = None
                current_validate_url = None
    
    print(f"Found {len(pending_entries)} pending entries to validate")
    print(f"Found {len(valid_entries)} valid entries (skipping)")
    print()
    
    if len(valid_entries) > 0:
        print("Skipping valid entries:")
        for dataset in valid_entries:
            print(f"  - {dataset}")
        print()
    
    if len(pending_entries) == 0:
        print("No pending entries to process.")
        return
    
    # Process each pending entry
    for i, (dataset_id, validate_url) in enumerate(pending_entries, 1):
        print(f"[{i}/{len(pending_entries)}] Validating dataset: {dataset_id}")
        print(f"Validation URL: {validate_url}")
        
        try:
            # Send GET request to the validation URL
            response = requests.get(validate_url, timeout=30)
            
            # Check if request was successful
            if response.status_code == 200:
                print(f"✓ SUCCESS - Status: {response.status_code}")
                # Print first 200 characters of response content if available
                if response.text:
                    content_preview = response.text[:200].replace('\n', ' ')
                    print(f"  Response: {content_preview}...")
            else:
                print(f"✗ FAILED - Status: {response.status_code}")
                if response.text:
                    error_preview = response.text[:200].replace('\n', ' ')
                    print(f"  Error: {error_preview}...")
                    
        except requests.exceptions.RequestException as e:
            print(f"✗ ERROR - Request failed: {e}")
        except Exception as e:
            print(f"✗ UNEXPECTED ERROR: {e}")
        
        print("-" * 60)
        
        # Add a small delay between requests to be respectful to the server
        if i < len(pending_entries):
            time.sleep(1)
    
    print(f"\nCompleted processing {len(pending_entries)} pending entries")

def main():
    """
    Main function to run the script
    """
    # You can specify a different filename as a command line argument
    filename = sys.argv[1] if len(sys.argv) > 1 else "subscription_list.txt"
    
    print(f"Processing subscription file: {filename}")
    print("=" * 60)
    
    process_subscription_file(filename)

if __name__ == "__main__":
    main()
