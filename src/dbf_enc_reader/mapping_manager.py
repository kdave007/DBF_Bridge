import json
from pathlib import Path
from typing import Dict, Any, Optional

class MappingManager:
    def __init__(self, mapping_file_path: str):
        """Initialize the mapping manager with the path to mappings.json.
        
        Args:
            mapping_file_path: Path to the mappings.json file
        """
        self.mapping_file_path = Path(mapping_file_path)
        self.mappings: Dict[str, Any] = {}
        self.load_mappings()

    def load_mappings(self) -> None:
        """Load the mappings from the JSON file."""
        try:
            with open(self.mapping_file_path, 'r', encoding='utf-8') as f:
                self.mappings = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Mapping file not found at {self.mapping_file_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in mapping file {self.mapping_file_path}")

    def get_dbf_mappings(self, dbf_name: str) -> Optional[Dict[str, Any]]:
        """Get the mappings for a specific DBF file.
        
        Args:
            dbf_name: Name of the DBF file (e.g., 'CAT_PROD.DBF')
            
        Returns:
            Dictionary containing the mappings for the specified DBF file or None if not found
        """
        return self.mappings.get(dbf_name)

    def get_target_table(self, dbf_name: str) -> Optional[str]:
        """Get the target table name for a DBF file.
        
        Args:
            dbf_name: Name of the DBF file
            
        Returns:
            Target table name or None if not found
        """
        dbf_config = self.get_dbf_mappings(dbf_name)
        return dbf_config.get('target_table') if dbf_config else None

    def get_field_mappings(self, dbf_name: str) -> Dict[str, Dict[str, str]]:
        """Get all field mappings for a DBF file.
        
        Args:
            dbf_name: Name of the DBF file
            
        Returns:
            Dictionary of field mappings directly from the JSON structure
        """
        dbf_config = self.get_dbf_mappings(dbf_name)
        return dbf_config.get('fields', {}) if dbf_config else {}

# Usage example:
if __name__ == "__main__":
    mapper = MappingManager("mappings.json")
    cat_prod_mappings = mapper.get_field_mappings("CAT_PROD.DBF")
    print(f"Target table: {mapper.get_target_table('CAT_PROD.DBF')}")
    for field_name, field_config in cat_prod_mappings.items():
        print(f"{field_name}: DBF={field_config['dbf']} -> Velneo={field_config['velneo_table']} (Type: {field_config['type']})")
