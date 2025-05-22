import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any

from src.db.postgres_tracking import PostgresTracking


class DBFSQLComparator:
    """
    Dedicated class for comparing DBF records with SQL database records.
    Handles both day-level batch comparisons and detailed record-by-record comparisons.
    """
    
    def __init__(self, db_config: Dict[str, str] = None):
        """
        Initialize the comparator with database configuration.
        
        Args:
            db_config: Dictionary with database connection parameters.
                       If None, default configuration will be used.
        """
        self.db_config = db_config or {
            'host': 'localhost',
            'database': 'suc_vel',
            'user': 'postgres',
            'password': 'comexcare',
            'port': '5432'
        }
        self.tracker = PostgresTracking(self.db_config)
    
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
                '%m/%d/%Y %I:%M:%S %p',  # MM/DD/YYYY with AM/PM
                '%m/%d/%Y %I:%M:%S %a. m.',  # MM/DD/YYYY with Spanish AM
                '%m/%d/%Y %I:%M:%S %p. m.',  # MM/DD/YYYY with Spanish PM
                '%m/%d/%Y %H:%M:%S',  # MM/DD/YYYY with 24-hour time
                '%m/%d/%Y'  # MM/DD/YYYY date only
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
        print(f"DBF Hash: {dbf_hash}")
        print(f"SQL Hash: {sql_hash}")
        
        result = {
            "matched": is_match,
            "dbf_hash": dbf_hash,
            "sql_hash": sql_hash,
            "lote_id": lote_record.get('lote'),
            "fecha_referencia": lote_record.get('fecha_referencia')
        }
        
        # If batch hashes don't match, perform detailed record-by-record comparison
        if not is_match:
            print("Batch hashes don't match. Performing detailed record comparison...")
            detailed_results = self.compare_records_by_hash(dbf_records, start_date=start_date, end_date=start_date)
            result["detailed_comparison"] = detailed_results
            
        return result
    
    def compare_records_by_hash(self, dbf_records: Dict[str, Any], start_date: date, end_date: date) -> Dict[str, Any]:
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
        sql_records = self.tracker.get_records_by_date_range(start_date, end_date)
        
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
        matching_pendiente = []  # Only track matching records with estado='pendiente'
        mismatched = []
        in_dbf_only = []
        in_sql_only = []
        
        # Check each DBF record
        for folio, dbf_record in dbf_records_by_folio.items():
            if folio in sql_records_by_folio:
                sql_record = sql_records_by_folio[folio]
                
                # Compare hashes
                if dbf_record.get('md5_hash') == sql_record.get('hash'):
                    # Only track if estado is 'pendiente'
                    if sql_record.get('estado') == 'pendiente':
                        matching_pendiente.append({
                            "dbf_record": dbf_record,
                            "sql_record": sql_record
                        })
                else:
                    # Store mismatched records
                    mismatched.append({
                        "folio": folio,
                        "dbf_record": dbf_record,
                        "sql_record": sql_record,
                        "dbf_hash": dbf_record.get('md5_hash'),
                        "sql_hash": sql_record.get('hash')
                    })
            else:
                # Store complete DBF-only records
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
                    "folio": folio,
                    "sql_record": sql_record,
                    "sql_hash": sql_record.get('hash')
                })
                
        # Organize data by required API operations
        api_operations = {
            "create": in_dbf_only,
            "update": mismatched,
            "delete": in_sql_only
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
                "total_actions_needed": len(in_dbf_only) + len(mismatched) + len(in_sql_only)
            },
            "matching_pendiente": matching_pendiente
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
