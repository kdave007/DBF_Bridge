import json
import sys
import os
from pathlib import Path

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from src.dbf_enc_reader.core import DBFReader
from src.dbf_enc_reader.connection import DBFConnection

def main():
    # Set the DLL path first
    dll_path = r"C:\\Program Files (x86)\\Advantage 10.10\\ado.net\\1.0\\Advantage.Data.Provider.dll"
    DBFConnection.set_dll_path(dll_path)

    # Initialize reader
    enc_pass = "my password"
    source_path = r"C:\\Users\\campo\\Documents\\projects\\DBF_encrypted\pospcp"
    table_name = "VENTA"
    limit_rows = 2
    
    print(f"Connecting to DBF at: {source_path}")
    reader = DBFReader(source_path, enc_pass)

    # Example 1: Find notes by date range using memory filtering
    filters = [
        {
            'field': 'F_EMISION',
            'operator': 'range',
            'from_value': '05/05/2025',  # May 5th, 2025
            'to_value': '05/05/2025',    # May 5th, 2025
            'is_date': True
        }
    ]
    print("\nFinding notes for the entire day of 16/12/2020 (newest first):")
    result = reader.to_json(table_name, limit_rows, filters)
    print(result)

if __name__ == "__main__":
    main()