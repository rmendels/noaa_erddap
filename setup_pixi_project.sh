#!/bin/bash
# Script to set up a Pixi project that preserves multiple Python files
# and analysis results (.csv, .xml files)

# Navigate to your project directory
cd noaa_erddap

# Initialize a new Pixi project
pixi init

# Create a pixi.toml configuration file with required dependencies
cat > pixi.toml << EOF
[project]
name = "thredds-to-erddap"
version = "0.1.0"
description = "Convert THREDDS catalogs to ERDDAP configurations"
authors = ["Your Name <your.email@example.com>"]
channels = ["conda-forge"]
# Add Python and required packages
dependencies = [
    "python>=3.8",
    "requests",
    "beautifulsoup4",
    "netCDF4",
    "lxml"
]

[tasks]
# Define a task to run the main converter (update this to match your main script)
convert = "python src/thredds_to_erddap/thredds_to_erddap.py"
# Add more custom tasks as needed for your other scripts

[feature.dev.dependencies]
# Development dependencies
pytest = "*"
black = "*"
flake8 = "*"
EOF

# Create a proper Python package structure while preserving your existing files
mkdir -p src/thredds_to_erddap
mkdir -p tests
mkdir -p data/results  # For analysis results

# Create an __init__.py file to make it a proper package
touch src/thredds_to_erddap/__init__.py

# Copy all Python files to the package directory instead of moving them
# This preserves your original files in case something goes wrong
echo "Copying Python files to package structure..."
for pyfile in *.py; do
    if [ -f "$pyfile" ]; then
        cp "$pyfile" "src/thredds_to_erddap/"
        echo "Copied $pyfile to src/thredds_to_erddap/"
    fi
done

# Move analysis results to the data directory
echo "Organizing analysis results..."
mkdir -p data/csv
mkdir -p data/xml

# Move CSV files to data/csv directory
for csvfile in *.csv; do
    if [ -f "$csvfile" ]; then
        cp "$csvfile" "data/csv/"
        echo "Copied $csvfile to data/csv/"
    fi
done

# Move XML files to data/xml directory
for xmlfile in *.xml; do
    if [ -f "$xmlfile" ]; then
        cp "$xmlfile" "data/xml/"
        echo "Copied $xmlfile to data/xml/"
    fi
done

# Create a main entry point script - update with your actual main script name
MAIN_SCRIPT="thredds_to_erddap.py"  # Change this to your main script name
if [ -f "$MAIN_SCRIPT" ]; then
    cat > run.py << EOF
#!/usr/bin/env python3
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# Import and run the main function from your main script
from thredds_to_erddap.$(basename "$MAIN_SCRIPT" .py) import main

if __name__ == "__main__":
    main()
EOF
    chmod +x run.py
fi

# Create a comprehensive README.md file
cat > README.md << EOF
# THREDDS to ERDDAP Converter

Convert THREDDS Data Server catalogs to ERDDAP EDDGridFromDap configurations.

## Project Structure

\`\`\`
noaa_erddap/
├── pixi.toml            # Pixi configuration and dependencies
├── README.md            # Project documentation
├── run.py               # Main entry point
├── src/                 # Source code
│   └── thredds_to_erddap/ # Python package 
│       ├── __init__.py
│       ├── [your python files]
├── data/                # Data directory
│   ├── csv/             # CSV analysis results
│   └── xml/             # XML files and configurations
└── tests/               # Test directory
\`\`\`

## Installation

This project uses [Pixi](https://prefix.dev/docs/pixi/overview) for dependency management.

To install dependencies:

\`\`\`
pixi install
\`\`\`

## Usage

To run the main converter:

\`\`\`
pixi run convert --url https://tds.example.edu/thredds/catalog.xml --output data/xml/erddap_datasets.xml
\`\`\`

## Project Files

### Python Modules
$(for pyfile in *.py; do
    if [ -f "$pyfile" ]; then
        echo "- $pyfile"
    fi
done)

### Analysis Results
$(for csvfile in *.csv; do
    if [ -f "$csvfile" ]; then
        echo "- $csvfile (CSV)"
    fi
done)
$(for xmlfile in *.xml; do
    if [ -f "$xmlfile" ]; then
        echo "- $xmlfile (XML)"
    fi
done)
EOF

# Create a basic setup.py for traditional installation if needed
cat > setup.py << EOF
from setuptools import setup, find_packages

setup(
    name="thredds_to_erddap",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "beautifulsoup4",
        "netCDF4",
        "lxml",
    ],
    entry_points={
        "console_scripts": [
            "thredds-to-erddap=thredds_to_erddap.$(basename "$MAIN_SCRIPT" .py):main",
        ],
    },
)
EOF

# Create a .gitignore file for Python projects
cat > .gitignore << EOF
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# Distribution / packaging
dist/
build/
*.egg-info/

# Virtual environments
.env/
.venv/
env/
venv/
ENV/

# Pixi specific
.pixi/

# IDE files
.idea/
.vscode/
*.swp
EOF

# Install the dependencies
pixi install

echo "Pixi project setup complete!"
echo "IMPORTANT: This script has copied your files to a new structure."
echo "Your original files are still in their original locations."
echo "Once you've verified everything works, you can remove the originals if desired."
echo ""
echo "To run the main converter, use: pixi run convert [arguments]"
echo "To activate the Pixi environment: pixi shell"
