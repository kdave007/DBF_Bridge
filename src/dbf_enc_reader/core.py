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
                use_or = len(filters) > 1 and all(f['field'] == filters[0]['field'] for f in filters)
                
                for f in filters:
                    print(f' filter ////// {f}')
                    if f['operator'] == 'range':
                        filter_conditions.append(
                            f"{f['field']} = '{f['from_value']}' OR "
                            f"{f['field']} = '{f['to_value']}'"
                        )
                    else:
                        filter_conditions.append(
                            f"{f['field']}{f['operator']} '{f['value']}'"
                        )

                print(f'HERE ------ {filter_conditions}')        
                
                if filter_conditions:
                    join_op = " OR " if use_or else " AND "
                    filter_expr = join_op.join(filter_conditions)
                    print(f"\nApplying AOF filter: {filter_expr}")
                    try:
                        reader.Filter = filter_expr
                    except Exception as e:
                        print(f"\nFilter error: {str(e)}")
                        print(f"Filter expression: {filter_expr}")
                        raise
            
            # Process results
            count = 0
            while reader.Read():
               
                if limit and count >= limit:
                    break
                    
                record = {}
                for i in range(reader.FieldCount):
                    field_name = reader.GetName(i)
                    value = reader.GetValue(i)
                    record[field_name] = self.converter.convert_value(value)
                    
                results.append(record)
                count += 1
            
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
