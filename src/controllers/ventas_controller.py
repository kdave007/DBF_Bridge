from datetime import datetime, timedelta
from typing import Dict, Any, List
import json
import time
from ..dbf_enc_reader.core import DBFReader
from ..dbf_enc_reader.connection import DBFConnection
from ..dbf_enc_reader.mapping_manager import MappingManager
from ..config.dbf_config import DBFConfig

class VentasController:
    def __init__(self, mapping_manager: MappingManager, config: DBFConfig):
        """Initialize the CAT_PROD controller.
        
        Args:
            mapping_manager: Manager for field mappings
            config: DBF configuration
        """
        self.config = config
        self.mapping_manager = mapping_manager
        self.venta_dbf = "VENTA.DBF"  # Header table
        self.partvta_dbf = "PARTVTA.DBF"  # Details table
        
        # Initialize DBF reader
        DBFConnection.set_dll_path(self.config.dll_path)
        self.reader = DBFReader(self.config.source_directory, self.config.encryption_password)
    
    def get_sales_in_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get sales data within the specified date range, including details.
        
        Args:
            start_date: Start date for data range
            end_date: End date for data range
            
        Returns:
            List of dictionaries containing the mapped data with nested details
        """
        start_time = time.time()
        
        # First get headers for the date range
        headers_start = time.time()

        headers = self._get_headers_in_range(start_date, end_date)
        headers_time = time.time() - headers_start

        print(f"\nTime to get headers: {headers_time:.2f} seconds")
        
        # Get folios to filter details
        folios = [str(header['Folio']) for header in headers]
        
        # Then get details only for these folios
        details_start = time.time()

        details_by_folio = self._get_details_for_folios(folios) if folios else {}

        details_time = time.time() - details_start
        print(f"Time to get filtered details: {details_time:.2f} seconds")
        
        # Join headers with their details
        join_start = time.time()
        for header in headers:
            folio = header['Folio']  # Using the mapped name from mappings.json
            header['detalles'] = details_by_folio.get(folio, [])
        join_time = time.time() - join_start
        print(f"Time to join headers with details: {join_time:.2f} seconds")
        
        total_time = time.time() - start_time
        print(f"Total processing time: {total_time:.2f} seconds")
        
        return headers
        
    def _get_details_for_folios(self, folios: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Get sales details for specific folios and organize them by folio number.
        
        Args:
            folios: List of folio numbers to get details for
            
        Returns:
            Dictionary mapping folio numbers to lists of detail records
        """
        field_mappings = self.mapping_manager.get_field_mappings(self.partvta_dbf)
        
        # Create filter for specific folios using OR
        filters = []
        for folio in folios:
            # Pad the folio with leading zeros to 6 digits to match DBF format
            filter_dict = {
                'field': 'NO_REFEREN',
                'operator': '=',
                'value': str(folio).zfill(6),  # Pad with leading zeros
                'is_numeric': False  # Treat as string to preserve leading zeros
            }
            filters.append(filter_dict)

        # Get filtered details
        read_start = time.time()

        raw_data_str = self.reader.to_json(self.partvta_dbf, 0, filters)

        read_time = time.time() - read_start
        print(f"Time to read PARTVTA.DBF with filter: {read_time:.2f} seconds")
        
        parse_start = time.time()

        raw_data = json.loads(raw_data_str)
        
        parse_time = time.time() - parse_start
        print(f"Time to parse PARTVTA JSON: {parse_time:.2f} seconds")

        # Organize details by folio
        details_by_folio = {}
        for record in raw_data:
            transformed = self.transform_record(record, field_mappings)
            if transformed:
                folio = transformed['Folio']  # Using the mapped name
                if folio not in details_by_folio:
                    details_by_folio[folio] = []
                details_by_folio[folio].append(transformed)
        
        return details_by_folio
        
    def _get_headers_in_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get sales headers within the specified date range."""
        field_mappings = self.mapping_manager.get_field_mappings(self.venta_dbf)
        
        # Format dates to match DBF format (dd/mm/yyyy)
        # For end_date, we want to include the entire day, so we add one day and subtract 1 second
        end_date_inclusive = end_date + timedelta(days=1) - timedelta(seconds=1)
        
        # Create a single filter for the date range
        filters = [{
            'field': 'F_EMISION',
            'operator': 'range',
            'from_value': start_date.strftime('%d/%m/%Y 12:00:00 a. m.'),  # Format to match DBF
            'to_value': end_date.strftime('%d/%m/%Y 11:59:59 p. m.'),  # End of day
            'is_date': False  # F_EMISION is stored as string
        }]
        print(f"\nSearching for date range: {start_date.strftime('%d/%m/%Y %H:%M:%S')} to {end_date_inclusive.strftime('%d/%m/%Y %H:%M:%S')}")
        
        read_start = time.time()
        raw_data_str = self.reader.to_json(self.venta_dbf, self.config.limit_rows, filters)
        read_time = time.time() - read_start
        print(f"Time to read VENTA.DBF: {read_time:.2f} seconds")
        
        parse_start = time.time()
        raw_data = json.loads(raw_data_str)
        parse_time = time.time() - parse_start
        print(f"Time to parse VENTA JSON: {parse_time:.2f} seconds")

        transformed_data = []
        for record in raw_data:
            transformed = self.transform_record(record, field_mappings)
            if transformed:
                transformed_data.append(transformed)
        
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
                
                transformed[mapping['velneo_table']] = value
                
        # Print first record for debugging
        if not hasattr(VentasController, '_printed_transform'):
            print("\nTransformed record example:", transformed)
            setattr(VentasController, '_printed_transform', True)
                
        return transformed
