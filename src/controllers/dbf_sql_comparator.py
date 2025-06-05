import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from src.config.db_config import PostgresConnection
from src.db.postgres_tracking import PostgresTracking


class DBFSQLComparator:
    """
    Dedicated class for comparing DBF records with SQL database records.
    Handles both day-level batch comparisons and detailed record-by-record comparisons.
    """
    
    def __init__(self, db_config: Any = None):
        """
        Initialize the comparator with database configuration.
        
        Args:
            db_config: Either a PostgresConnection object or a dictionary with database connection parameters.
                       If None, default configuration will be used.
        """
        # Check if db_config is a PostgresConnection object or a dictionary
        if isinstance(db_config, PostgresConnection):
            self.db = db_config
            self.db_config = PostgresConnection.get_db_config()
        elif isinstance(db_config, dict):
            # It's already a config dictionary
            self.db_config = db_config
        else:
            # Use default configuration
            self.db_config = PostgresConnection.get_db_config()
            
        self.tracker = PostgresTracking(self.db_config)

    def add_all(self, dbf_records: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process all DBF records directly without SQL comparison.
        
        Args:
            dbf_records: Dictionary containing DBF records data
            
        Returns:
            Dictionary with records organized for API operations
        """
        # Create dictionaries for easy lookup with full records
        # Store records by folio for quick lookup
        dbf_records_by_folio = {}
        in_dbf_only = []
        
        # Map DBF records by folio
        for record in dbf_records['data']:
            if record.get('Folio') and record.get('md5_hash'):
                # Store the record in the lookup dictionary
                folio = str(record.get('Folio'))
                dbf_records_by_folio[folio] = record
                
                # Add to the list in the same format as compare_records_by_hash
                in_dbf_only.append({
                    "folio": folio,
                    "dbf_record": record,
                    "dbf_hash": record.get('md5_hash')
                })
                
        # Organize data by required API operations (all are creates since we're not comparing)
        api_operations = {
            "create": in_dbf_only,
            "update": [],
            "delete": [],
            "next_check":[]
        }
        
        return {
            "status": "completed",
            "total_dbf_records": len(dbf_records_by_folio),
            "api_operations": api_operations,
            "summary": {
                "create_count": len(in_dbf_only),
                "update_count": 0,
                "delete_count": 0,
                "total_actions_needed": len(in_dbf_only)
            }
        }
        



    
    def compare_batch_by_day(self, dbf_records: Dict[str, Any], sql_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compare DBF records with SQL records by the day batch MD5 hash.
        
        Args:
            dbf_records: Dictionary containing DBF records data
            sql_records: List of SQL records
            
        Returns:
            Dictionary with comparison results
        """
        # Extract date from first DBF record
        if not dbf_records or 'data' not in dbf_records or not dbf_records['data']:
            logging.error("No DBF records to compare")
            return {"matched": False, "error": "No DBF records to compare"}
            
        # Get the first record's date
        first_record = dbf_records['data'][0]
        if 'fecha' not in first_record:
            logging.error("DBF record missing fecha field")
            return {"matched": False, "error": "DBF record missing fecha field"}
            
        # Parse the date from the first record
        try:
            # Handle Spanish format with periods in AM/PM (a. m. / p. m.)
            fecha = first_record['fecha']
            # Try different date formats - DBF uses MM/DD/YYYY format
            date_formats = [
                '%d/%m/%Y %I:%M:%S %p',  # MM/DD/YYYY with AM/PM
                '%d/%m/%Y %I:%M:%S %a. m.',  # MM/DD/YYYY with Spanish AM
                '%d/%m/%Y %I:%M:%S %p. m.',  # MM/DD/YYYY with Spanish PM
                '%d/%m/%Y %H:%M:%S',  # MM/DD/YYYY with 24-hour time
                '%d/%m/%Y'  # MM/DD/YYYY date only
            ]
            
            record_date = None
            for fmt in date_formats:
                try:
                    # Replace Spanish AM/PM format to standard format if needed
                    temp_fecha = fecha.replace('a. m.', 'AM').replace('p. m.', 'PM')
                    record_date = datetime.strptime(temp_fecha, fmt)
                    print(f"Successfully parsed date {fecha} using format {fmt}")
                    break
                except ValueError:
                    continue
                   
            if record_date is None:
                raise ValueError(f"Could not parse date: {fecha} with any known format")
                
            start_date = record_date.date()
        except Exception as e:
            logging.error(f"Error parsing date: {e}")
            return {"matched": False, "error": f"Error parsing date: {e}"}
        
        # Get a single record from lote_diario by start date
        lote_record = self.tracker.get_single_lote_by_date(start_date)
        
        if not lote_record:
            logging.info(f"No lote record found for date {start_date}")
            return {"matched": False, "error": f"No lote record found for date {start_date}"}
        
        # Calculate MD5 hash for the DBF records
        dbf_hash = self._calculate_md5(dbf_records)
    
        # Compare the hashes
        sql_hash = lote_record.get('hash_lote')
        if not sql_hash:
            logging.error("SQL record missing hash_lote field")
            return {"matched": False, "error": "SQL record missing hash_lote field"}
        
        is_match = dbf_hash == sql_hash
        # print(f"DBF Hash: {dbf_hash}")
        # print(f"SQL Hash: {sql_hash}")
        
        # Perform record-by-record comparison directly instead of just checking batch hashes
        # This ensures we always get the same structure as compare_records_by_hash
        print("Performing record-by-record comparison...")
        result = self.compare_records_by_hash(dbf_records, start_date=start_date, end_date=start_date)
            
        return result
    
    def compare_records_by_hash(self, dbf_records: Dict[str, Any], sql_records, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Compare individual DBF records with database records by hash.
        
        Args:
            dbf_records: Dictionary containing DBF records data
            start_date: The start date to query records for
            end_date: The end date to query records for

        Returns:
            Dictionary with detailed comparison results
        """
        # Get all records from database for the given date
       # sql_records = self.tracker.get_records_by_date_range(start_date, end_date)
        
        if not sql_records:
            return {
                "status": "no_sql_records",
                "message": f"No SQL records found for date {start_date}"
            }
            
        # Create dictionaries for easy lookup with full records
        # Store records by folio for quick lookup
        dbf_records_by_folio = {}
        sql_records_by_folio = {}
        
        # Map DBF records by folio
        for record in dbf_records['data']:
            if record.get('Folio') and record.get('md5_hash'):
                dbf_records_by_folio[str(record.get('Folio'))] = record
        
        # Map SQL records by folio
        for record in sql_records:
            if record.get('folio') and record.get('hash'):
                sql_records_by_folio[str(record.get('folio'))] = record
        
        # Compare records
        mismatched = []  # Records to update
        in_dbf_only = []  # Records to add
        in_sql_only = []  # Records to delete
        matching = []    # Records that don't need any changes (hash matches)
        
        # Check each DBF record
        for folio, dbf_record in dbf_records_by_folio.items():
            if folio in sql_records_by_folio:
                sql_record = sql_records_by_folio[folio]
                
                # Compare hashes - if different, it needs to be updated
                print(f'////////-----------DBF { dbf_record.get('md5_hash')}  vs  SQL {sql_record.get('hash')}')
                if dbf_record.get('md5_hash') != sql_record.get('hash'):
                    # Store mismatched records for update
                    mismatched.append({
                        "folio": folio,
                        "id": int(sql_records_by_folio[folio].get('id', 0)),
                        "dbf_record": dbf_record,
                        "sql_record": sql_record,
                        "dbf_hash": dbf_record.get('md5_hash'),
                        "sql_hash": sql_record.get('hash')
                    })
                else:
                    # Store records that match (no changes needed)
                    matching.append({
                        "folio": folio,
                        "id": int(sql_records_by_folio[folio].get('id', 0)),
                        "dbf_record": dbf_record,
                        "sql_record": sql_record,
                        "hash": dbf_record.get('md5_hash')  # Both hashes are the same
                    })
            else:
                # Store complete DBF-only records for creation
                in_dbf_only.append({
                    "folio": folio,
                    "dbf_record": dbf_record,
                    "dbf_hash": dbf_record.get('md5_hash')
                })
                
        # Check for records in SQL only
        for folio, sql_record in sql_records_by_folio.items():
            if folio not in dbf_records_by_folio:
                # Store complete SQL-only records
                in_sql_only.append({
                    "id": int(sql_records_by_folio[folio].get('id', 0)),
                    "folio": folio,
                    "sql_record": sql_record,
                    "sql_hash": sql_record.get('hash')
                })
                
        # Organize data by required API operations
        api_operations = {
            "create": in_dbf_only,
            "update": mismatched,
            "delete": in_sql_only,
            "next_check": matching  # Add the matching records
        }
        
        return {
            "status": "completed",
            "total_dbf_records": len(dbf_records_by_folio),
            "total_sql_records": len(sql_records_by_folio),
            "api_operations": api_operations,
            "summary": {
                "create_count": len(in_dbf_only),
                "update_count": len(mismatched),
                "delete_count": len(in_sql_only),
                "matching_count": len(matching),  # Add count of matching records
                "total_actions_needed": len(in_dbf_only) + len(mismatched) + len(in_sql_only)
            }
        }
    
    def _calculate_md5(self, dbf_records: Dict[str, Any]) -> str:
        """
        Calculate MD5 hash for DBF records.
        
        Args:
            dbf_records: Dictionary containing DBF records data
            
        Returns:
            MD5 hash as string
        """
        import hashlib
        import json
        
        # Generate hash for the entire dataset
        dataset_str = json.dumps(dbf_records['data'], sort_keys=True)
        dataset_hash = hashlib.md5(dataset_str.encode('utf-8')).hexdigest()
        
        return dataset_hash
