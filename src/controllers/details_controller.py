import os
import sys
from datetime import date
import hashlib
from turtle import reset
from src.config.db_config import PostgresConnection
from src.db.detail_tracking import DetailTracking



class DetailsController:
    def __init__(self) -> None:
        self.db = PostgresConnection()
        pass

    def process(self, results, start_date, end_date):
        print('CHECKPOINT')

        # Process API results to get combined detail records
        combined = self.process_results(results)

        #inserted = self.insert_records(self.db, combined)

        # Get existing records from database
        sql_records = self.get_sql_records(self.db, start_date, end_date)
        
        # Compare records to find differences
        comparison_result = self.compare_batches(combined, sql_records)
        
        # Print comparison summary
        self.print_comparison_results(comparison_result)
        
        # Process operations based on comparison results
        operations = comparison_result.get('operations', {})
        
        # Process CREATE operations
        if operations.get('create'):
            create_records = [item['combined_record'] for item in operations['create']]
            if create_records:
                inserted = self.insert_records(self.db, create_records)
                print(f"Inserted {inserted} new records")
        
        # Process UPDATE operations - could be implemented if needed
        # if operations.get('update'):
        #     update_records = [item['combined_record'] for item in operations['update']]
        #     if update_records:
        #         updated = self.update_records(self.db, update_records)
        #         print(f"Updated {updated} records")
        
        # Process DELETE operations - could be implemented if needed
        # if operations.get('delete'):
        #     delete_records = [item['sql_record'] for item in operations['delete']]
        #     if delete_records:
        #         deleted = self.delete_records(self.db, delete_records)
        #         print(f"Deleted {deleted} records")
             
            
            #inserted = self.insert_records(self.db, combined)
            #print(f"Inserted/updated {inserted} records")
            


    def process_results(self, results):
        combined_details = []
        
        for operation in ['create', 'update']:
            if operation in results and 'success' in results[operation]:
                for record in results[operation]['success']:
                    print(f'  RECORD BEFORE COMBINED  {record}')
                    if 'details' in record:
                        for detail in record['details']:
                            detail_with_operation = detail.copy()
                            detail_with_operation['operation'] = operation
                            detail_with_operation['folio'] = record.get('folio')
                            
                            # Parse date properly - extract just the date part (remove time)
                            fecha_str = record.get('fecha_emision')
                            if fecha_str:
                                # Extract just the date part (before any space)
                                fecha_parts = fecha_str.split(' ')
                                if fecha_parts:
                                    detail_with_operation['fecha'] = fecha_parts[0]
                            
                            # Generate MD5 hash from detail content
                            detail_str = str(sorted(detail.items()))
                            detail_with_operation['detail_hash'] = hashlib.md5(detail_str.encode()).hexdigest()

                            combined_details.append(detail_with_operation)
        
        return combined_details



    def get_sql_records(self, db_connection, start_date, end_date):
        # Create a DetailTracking instance with the database configuration
        detail_tracker = DetailTracking(db_connection.db_config)
        
        # Use the get_details_by_date_range method from DetailTracking
        records = detail_tracker.get_details_by_date_range(start_date, end_date)
        
        print(f"Found {len(records)} records between {start_date} and {end_date}")
        return records
        
    def insert_records(self, db_connection, records):
        """
        Insert or update records in the database
        
        Args:
            db_connection: PostgresConnection instance
            records: List of records to insert/update
            
        Returns:
            Number of records successfully processed
        """
        if not records:
            return 0
            
        # Create a DetailTracking instance with the database configuration
        detail_tracker = DetailTracking(db_connection.db_config)
        
        # Use batch_insert_details method to insert all records at once
        success = detail_tracker.batch_insert_details(records)
        
        if success:
            return len(records)
        else:
            print("Error inserting records")
            return 0
            
    def compare_batches(self, combined_details, sql_records):
        """
        Compare combined detail records with SQL records to find differences
        
        Args:
            combined_details: List of detail records from DBF/API
            sql_records: List of records from SQL database
            
        Returns:
            Dictionary with comparison results
        """
        if not combined_details:
            return {
                "status": "no_dbf_records",
                "message": "No DBF/combined records provided"
            }
            
        if not sql_records:
            return {
                "status": "no_sql_records",
                "message": "No SQL records found"
            }
        
        print("\n=== DEBUG: COMPARING RECORDS ===")
        print(f"Combined records: {len(combined_details)}")
        print(f"SQL records: {len(sql_records)}")
        
        # Instead of comparing by hash, let's compare by the actual content
        # Create a normalized representation of each record for comparison
        
        # Function to create a normalized key for a record
        def get_record_key(record, is_sql=False):
            # Extract the key fields that identify a unique detail record
            folio = record.get('folio')
            ref = record.get('REF')
            cantidad = record.get('cantidad')
            precio = record.get('precio')
            
            # Create a tuple of values for comparison
            return (str(folio), str(ref), str(cantidad), str(precio))
        
        # Create dictionaries with normalized keys
        combined_by_key = {}
        for record in combined_details:
            key = get_record_key(record)
            combined_by_key[key] = record
            
        sql_by_key = {}
        for record in sql_records:
            key = get_record_key(record, is_sql=True)
            sql_by_key[key] = record
        
        # Debug output
        print(f"Unique combined keys: {len(combined_by_key)}")
        print(f"Unique SQL keys: {len(sql_by_key)}")
        
        # Get sets of keys for comparison
        combined_keys = set(combined_by_key.keys())
        sql_keys = set(sql_by_key.keys())
        
        # Find records to create (in combined but not in SQL)
        create_keys = combined_keys - sql_keys
        # Find records to delete (in SQL but not in combined)
        delete_keys = sql_keys - combined_keys
        
        # Debug output
        print(f"Keys to create: {len(create_keys)}")
        print(f"Keys to delete: {len(delete_keys)}")
        
        # Create result lists
        in_combined_only = []
        in_sql_only = []
        mismatched = []  # Records to update
        
        # Process records to create
        for key in create_keys:
            record = combined_by_key[key]
            folio = record.get('folio')
            hash_val = record.get('detail_hash')
            if folio and hash_val:
                in_combined_only.append({
                    "key": f"{folio}-{hash_val}",
                    "combined_record": record,
                    "combined_hash": hash_val
                })
        
        # Process records to delete
        for key in delete_keys:
            record = sql_by_key[key]
            folio = record.get('folio')
            hash_val = record.get('hash_detalle')
            if folio and hash_val:
                in_sql_only.append({
                    "key": f"{folio}-{hash_val}",
                    "sql_record": record,
                    "sql_hash": hash_val
                })
        # Organize data by required operations
        operations = {
            "create": in_combined_only,
            "update": mismatched,  # Include mismatched list for updates
            "delete": in_sql_only
        }
        
        return {
            "status": "completed",
            "total_combined_records": len(combined_details),
            "total_sql_records": len(sql_records),
            "operations": operations,
            "summary": {
                "create_count": len(in_combined_only),
                "update_count": len(mismatched),
                "delete_count": len(in_sql_only),
                "total_actions_needed": len(in_combined_only) + len(mismatched) + len(in_sql_only)
            }
        }    

    
    def print_comparison_results(self, detailed_comparison):
        print("\n=================================================")
        print("=== DETAIL RECORDS OPERATIONS SUMMARY ===")
        print("=================================================\n")
        
        # Print summary statistics
        summary = detailed_comparison.get('summary', {})
        print(f"Total Combined Records: {detailed_comparison.get('total_combined_records', 0)}")
        print(f"Total SQL Records: {detailed_comparison.get('total_sql_records', 0)}")
        print(f"Status: {detailed_comparison.get('status', 'unknown')}\n")
        
        print("Operations Required:")
        print(f"  CREATE: {summary.get('create_count', 0)} records")
        print(f"  UPDATE: {summary.get('update_count', 0)} records")
        print(f"  DELETE: {summary.get('delete_count', 0)} records")
        print(f"  TOTAL ACTIONS: {summary.get('total_actions_needed', 0)} operations\n")
        
        # Print sample records for each operation type if available
        operations = detailed_comparison.get('operations', {})
        
        if operations.get('create') and summary.get('create_count', 0) > 0:
            print("\nSample CREATE Records:")
            for i, item in enumerate(operations['create'][:3]):  # Show up to 3 samples
                record = item.get('combined_record', {})
                print(f"  {i+1}. Folio: {record.get('folio')}, Hash: {record.get('detail_hash')[:10]}...")
                
        if operations.get('update') and summary.get('update_count', 0) > 0:
            print("\nSample UPDATE Records:")
            for i, item in enumerate(operations['update'][:3]):  # Show up to 3 samples
                record = item.get('combined_record', {})
                print(f"  {i+1}. Folio: {record.get('folio')}, Hash: {record.get('detail_hash')[:10]}...")
                
        if operations.get('delete') and summary.get('delete_count', 0) > 0:
            print("\nSample DELETE Records:")
            for i, item in enumerate(operations['delete'][:3]):  # Show up to 3 samples
                record = item.get('sql_record', {})
                print(f"  {i+1}. Folio: {record.get('folio')}, Hash: {record.get('hash_detalle')[:10]}...")
                
        print("\n=================================================")