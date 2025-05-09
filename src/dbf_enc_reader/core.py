import clr
import json
from typing import List, Dict, Any, Optional
from pathlib import Path

from .connection import DBFConnection
from .converters import DataConverter

class DBFReader:
    def __init__(self, data_source: str, encryption_password: str):
        """
        Initialize DBF reader with connection parameters.
        
        Args:
            data_source: Path to the DBF file
            encryption_password: Password for encrypted DBF
        """
        self.connection = DBFConnection(data_source, encryption_password)
        self.converter = DataConverter()

    def read_table(self, table_name: str, limit: Optional[int] = None, filters: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """Read records from a table with optional filters.
        
        Args:
            table_name: Name of the table to read
            limit: Optional limit on number of records to read
            filters: Optional list of filter conditions
            
        Returns:
            List of records as dictionaries
        """
        results = []
        with self.connection as conn:
            from System.Data import CommandType
            
            # Create command with TableDirect for better performance
            cmd = conn.conn.CreateCommand()
            cmd.CommandType = CommandType.TableDirect
            cmd.CommandText = table_name
            cmd.AdsOptimizedFilters = True  # Enable AOF for better performance
            
            # Get reader
            reader = cmd.ExecuteExtendedReader()
            
            # Apply filters if any
            if filters:
                filter_conditions = []
                for f in filters:
                    if f['field'] == 'F_EMISION':
                        if f['operator'] == 'range':
                            filter_conditions.append(
                                f"F_EMISION >= '{f['from_value']}' AND "
                                f"F_EMISION <= '{f['to_value']}'"
                            )
                        else:
                            filter_conditions.append(
                                f"F_EMISION {f['operator']} '{f['value']}'"
                            )
                
                if filter_conditions:
                    filter_expr = " AND ".join(filter_conditions)
                    print(f"\nApplying AOF filter: {filter_expr}")
                    reader.Filter = filter_expr
            
            # Process results
            while reader.Read():
                record = {}
                for i in range(reader.FieldCount):
                    field_name = reader.GetName(i)
                    value = reader.GetValue(i)
                    record[field_name] = self.converter.convert_value(value)
                    
                results.append(record)
            
            return results
            

    def to_json(self, table_name: str, limit: Optional[int] = None, filters: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Convert table records to JSON string.
        
        Args:
            table_name: Name of the table to convert
            limit: Optional limit on number of records to convert
            filters: Optional list of filter conditions
            
        Returns:
            JSON string representation of the records
        """
        records = self.read_table(table_name, limit, filters)
        return json.dumps(records, indent=4, ensure_ascii=False)

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about table structure.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table metadata
        """
        with self.connection as conn:
            reader = conn.get_reader(table_name)
            return {
                'field_count': reader.FieldCount,
                'columns': [reader.GetName(i) for i in range(reader.FieldCount)]
            }
