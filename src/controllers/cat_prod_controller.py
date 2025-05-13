from datetime import datetime
from typing import Dict, Any, List, Optional
import json
from ..dbf_enc_reader.core import DBFReader
from ..dbf_enc_reader.connection import DBFConnection
from ..dbf_enc_reader.mapping_manager import MappingManager
from ..config.dbf_config import DBFConfig

class CatProdController:
    def __init__(self, mapping_manager: MappingManager, config: DBFConfig):
        """Initialize the CAT_PROD controller.
        
        Args:
            mapping_manager: Manager for field mappings
            config: DBF configuration
        """
        self.config = config
        self.mapping_manager = mapping_manager
        self.dbf_name = "CAT_PROD.DBF"
        
        # Initialize DBF reader
        DBFConnection.set_dll_path(self.config.dll_path)
        self.reader = DBFReader(self.config.source_directory, self.config.encryption_password)
        
    def get_data_in_range(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Get CAT_PROD data within the specified date range.
        
        Args:
            start_date: Start date for data range
            end_date: End date for data range
            
        Returns:
            List of dictionaries containing the mapped data
        """
        # Get field mappings for CAT_PROD
        field_mappings = self.mapping_manager.get_field_mappings(self.dbf_name)
        
        # No filters, just get last rows
        filters = []  # Empty filter to get all records
        
        # Get data from DBF with filter and parse JSON
        raw_data_str = self.reader.to_json(self.dbf_name, self.config.limit_rows, filters)
        raw_data = json.loads(raw_data_str)

        # Transform the data using mappings
        transformed_data = []
        for record in raw_data:
            transformed_record = self.transform_record(record, field_mappings)
            if transformed_record:  # Only add non-empty records
                transformed_data.append(transformed_record)
        
        return transformed_data
    
    def transform_record(self, record: Dict[str, Any], field_mappings: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a DBF record using the field mappings.
        
        Args:
            record: Raw record from DBF
            field_mappings: Field mapping configuration
            
        Returns:
            Transformed record with mapped field names and types
        """
        transformed = {}
        for target_field, mapping in field_mappings.items():
            dbf_field = mapping['dbf']
            if dbf_field in record:
                value = record[dbf_field]
                if mapping['type'] == 'number':
                    try:
                        value = float(value) if '.' in str(value) else int(value)
                    except (ValueError, TypeError):
                        value = 0
                
                #transformed[target_field] = value
                transformed[mapping['velneo_table']] = value
                
        return transformed



# Usage example:
if __name__ == "__main__":
    from datetime import timedelta
    
    # Initialize configuration
    config = DBFConfig(
        dll_path=r"C:\Program Files (x86)\Advantage 10.10\ado.net\1.0\Advantage.Data.Provider.dll",
        encryption_password="my password",
        source_directory=r"C:\Users\campo\Documents\projects\DBF_encrypted\pospcp",
        limit_rows=2
    )
    
    # Initialize mapping manager
    mapping_manager = MappingManager("mappings.json")
    
    # Create controller
    controller = CatProdController(mapping_manager, config)
    
    # Get data for last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    data = controller.get_data_in_range(start_date, end_date)
