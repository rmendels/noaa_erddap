#!/usr/bin/env python3
"""
check_erddap_missing.py

Usage:
  python check_erddap_missing.py servers.txt datasets.xml

servers.txt: one ERDDAP base URL per line (e.g. https://my.host/erddap/ or https://my.host/erddap)
datasets.xml: a large ERDDAP datasets.xml that you want to compare against
"""

import sys
import csv
import json
import time
import argparse
from pathlib import Path
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import requests

TIMEOUT = 30
HEADERS = {"User-Agent": "ERDDAP-datasetid-check/1.0 (+python requests)"}


def normalize_base(url: str) -> str:
    """Ensure the base ends with 'erddap/' and a trailing slash."""
    url = url.strip()
    if not url:
        return url
    # If it already ends with '/erddap' (no slash), add slash
    if url.endswith("/erddap"):
        url = url + "/"
    # If it doesn't contain '/erddap', assume user already provided the correct base;
    # but if they gave a domain root, try to append 'erddap/'.
    if "/erddap/" not in url:
        if url.endswith("/"):
            url = url + "erddap/"
        else:
            url = url + "/erddap/"
    return url


def read_server_list(path: Path) -> list[str]:
    servers = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        servers.append(normalize_base(line))
    return servers


def parse_datasets_xml_ids(xml_path: Path) -> set[str]:
    """
    Collect datasetID attributes from any dataset element under datasets.xml.
    ERDDAP uses many concrete dataset element names (EDDGrid*, EDDTable*, etc.),
    so we just scan for any element with a 'datasetID' attribute.
    """
    ids = set()
    # iterparse to avoid loading huge file fully in memory
    for event, elem in ET.iterparse(xml_path, events=("start", "end")):
        if event == "start":
            if "datasetID" in elem.attrib:
                ids.add(elem.attrib["datasetID"])
        # Clear elements to keep memory low
        if event == "end":
            elem.clear()
    return ids


def _get_json(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200 and r.headers.get("content-type", "").lower().startswith("application/json"):
            return r.json()
        return None
    except requests.RequestException:
        return None


def _get_text(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
        return None
    except requests.RequestException:
        return None


def fetch_dataset_ids(base: str) -> set[str]:
    """
    Try multiple strategies to list datasetIDs on a remote ERDDAP:

    1) /tabledap/allDatasets.csv?datasetID       (fast & light if supported)
    2) /search/index.json?searchFor=&itemsPerPage=100000 (has datasetID column)
    3) /info/index.json?searchFor=&itemsPerPage=100000   (collect distinct datasetID)
    """
    ids: set[str] = set()

    # Strategy 1: allDatasets table (if exposed)
    try:
        url = urljoin(base, "tabledap/allDatasets.csv?datasetID")
        txt = _get_text(url)
        if txt and "datasetID" in txt.splitlines()[0].lower():
            reader = csv.reader(txt.splitlines())
            header = next(reader, [])
            # find datasetID col
            try:
                idx = [h.strip() for h in header].index("datasetID")
            except ValueError:
                idx = 0  # fall back
            for row in reader:
                if not row:
                    continue
                did = row[idx].strip()
                if did and did.lower() != "datasetid":
                    ids.add(did)
            if ids:
                return ids
    except Exception:
        pass

    # Strategy 2: search API (dataset-level results)
    try:
        # Use a blank search to get everything; crank up itemsPerPage
        url = urljoin(base, "search/index.json?searchFor=&itemsPerPage=100000")
        data = _get_json(url)
        if data and isinstance(data, dict) and "table" in data:
            table = data["table"]
            colnames = [c.strip() for c in table.get("columnNames", [])]
            rows = table.get("rows", [])
            if "datasetID" in colnames:
                idx = colnames.index("datasetID")
                for row in rows:
                    if isinstance(row, list) and len(row) > idx:
                        did = str(row[idx]).strip()
                        if did:
                            ids.add(did)
            # Some ERDDAPs use 'datasetId' capitalization — be lenient
            elif "datasetId" in colnames:
                idx = colnames.index("datasetId")
                for row in rows:
                    if isinstance(row, list) and len(row) > idx:
                        did = str(row[idx]).strip()
                        if did:
                            ids.add(did)
            if ids:
                return ids
    except Exception:
        pass

    # Strategy 3: info API (variable/attr-level rows; we deduplicate datasetID)
    try:
        url = urljoin(base, "info/index.json?searchFor=&itemsPerPage=100000")
        data = _get_json(url)
        if data and isinstance(data, dict) and "table" in data:
            table = data["table"]
            colnames = [c.strip() for c in table.get("columnNames", [])]
            rows = table.get("rows", [])
            # Most ERDDAP info tables include 'datasetID'
            if "datasetID" in colnames:
                idx = colnames.index("datasetID")
                for row in rows:
                    if isinstance(row, list) and len(row) > idx:
                        did = str(row[idx]).strip()
                        if did:
                            ids.add(did)
            elif "datasetId" in colnames:
                idx = colnames.index("datasetId")
                for row in rows:
                    if isinstance(row, list) and len(row) > idx:
                        did = str(row[idx]).strip()
                        if did:
                            ids.add(did)
    except Exception:
        pass

    return ids


def main():
    p = argparse.ArgumentParser()
    p.add_argument("servers_txt", type=Path, help="Text file of ERDDAP base URLs, one per line")
    p.add_argument("datasets_xml", type=Path, help="Combined datasets.xml to compare against")
    p.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between servers (optional)")
    p.add_argument("--write-json", type=Path, default=None, help="Optional path to write full JSON results")
    args = p.parse_args()

    servers = read_server_list(args.servers_txt)
    if not servers:
        print("No servers found in servers file.", file=sys.stderr)
        sys.exit(2)

    xml_ids = parse_datasets_xml_ids(args.datasets_xml)
    print(f"Parsed {len(xml_ids)} datasetIDs from {args.datasets_xml}")

    results = []
    any_missing = False

    for base in servers:
        print(f"\n=== {base} ===")
        server_ids = fetch_dataset_ids(base)
        if not server_ids:
            print("  Could not retrieve datasetIDs from this server (skipping).")
            results.append(
                {
                    "server": base,
                    "retrieved": False,
                    "server_dataset_count": 0,
                    "missing_in_xml_count": None,
                    "missing_datasetIDs": [],
                }
            )
        else:
            missing = sorted(server_ids - xml_ids)
            any_missing = any_missing or bool(missing)
            print(f"  Server has {len(server_ids)} datasets.")
            print(f"  Missing from datasets.xml: {len(missing)}")
            for did in missing[:20]:
                print(f"    - {did}")
            if len(missing) > 20:
                print(f"    ... and {len(missing) - 20} more")

            results.append(
                {
                    "server": base,
                    "retrieved": True,
                    "server_dataset_count": len(server_ids),
                    "missing_in_xml_count": len(missing),
                    "missing_datasetIDs": missing,
                }
            )

        if args.sleep:
            time.sleep(args.sleep)

    if args.write_json:
        try:
            args.write_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
            print(f"\nWrote JSON results to: {args.write_json}")
        except Exception as e:
            print(f"\nFailed to write JSON results: {e}", file=sys.stderr)

    print("\nSummary:")
    for r in results:
        if not r["retrieved"]:
            print(f"  {r['server']}: retrieval failed")
        else:
            print(f"  {r['server']}: missing {r['missing_in_xml_count']} of {r['server_dataset_count']}")

    if any_missing:
        sys.exit(1)  # non-zero exit if any missing (useful in CI)
    else:
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(__doc__)
        sys.exit(1)
    main()
