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
        comparison_result = self.analyze_sync(combined, sql_records)
        
        # Print comparison summary
        self.print_sync_report(comparison_result)
        
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
                            
                            # Convert REF to lowercase ref if it exists
                            if 'REF' in detail_with_operation:
                                detail_with_operation['ref'] = detail_with_operation.pop('REF')
                                
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
        print(' ---   --- --- --- --- --- ---')
        print(results)
        
        return combined_details



    def get_sql_records(self, db_connection, start_date, end_date):
        # Create a DetailTracking instance with the database configuration
        detail_tracker = DetailTracking(db_connection.db_config)
        
        # Use the get_details_by_date_range method from DetailTracking
        records = detail_tracker.get_details_by_date_range(start_date, end_date)
        
        print(f"Found {(records)} records between {start_date} and {end_date}")
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
            
    def get_record_key(self, record, is_sql=False):
        """
        Extract the key fields that identify a unique detail record
        
        Args:
            record: The record to extract key from
            is_sql: Whether this is a SQL record (different field names)
            
        Returns:
            Tuple of (folio, hash) for unique identification
        """
        # Extract the folio which is our primary filter
        folio = record.get('folio')
        
        # Get the hash value - different field names in combined vs SQL records
        if is_sql:
            hash_val = record.get('hash_detalle')
        else:
            hash_val = record.get('detail_hash')
            
        # Return a tuple of folio and hash for unique identification
        return (str(folio), str(hash_val) if hash_val else "")
    
    def analyze_sync(self, combined_details, sql_records):
        """Core analysis function with accurate duplicate handling"""
        # Create lookup dictionaries and track counts
        for item in combined_details:
            print(f'COMBINED SYNC {item.get("ref")}')
        
        
        for item in sql_records:
            print(f'sql SYNC {item.get("ref")}')

        combined_counts = {}
        for item in combined_details:
            # For combined records, use 'ref' field (now lowercase)
            key = (item['folio'], item.get('ref', ''))
            combined_counts[key] = combined_counts.get(key, 0) + 1
        
        sql_counts = {}
        sql_items = {}
        for item in sql_records:
            # For SQL records, use 'ref' field from the alias in the SQL query
            key = (item['folio'], item.get('ref', ''))
            sql_counts[key] = sql_counts.get(key, 0) + 1
            sql_items.setdefault(key, []).append(item)
       
        # Identify operations
        in_combined_only = []
        to_update = []
        to_delete = []
        unchanged = []

        # Process records only in combined (create)
        for key in set(combined_counts) - set(sql_counts):
            in_combined_only.extend(
                [item for item in combined_details 
                if (item['folio'], item.get('ref', '')) == key]
            )
        
        # Process records only in SQL (delete)
        for key in set(sql_counts) - set(combined_counts):
            to_delete.extend(sql_items[key])
        
        # Process common records
        for key in set(combined_counts) & set(sql_counts):
            combined_count = combined_counts[key]
            sql_count = sql_counts[key]
            
            # Calculate duplicates to delete
            if sql_count > combined_count:
                excess = sql_count - combined_count
                to_delete.extend(sql_items[key][-excess:])  # Delete oldest/newest duplicates

            # Check for updates
            combined_hashes = {i['detail_hash'] for i in combined_details 
                            if (i['folio'], i.get('ref', '')) == key}
            sql_hashes = {i['hash_detalle'] for i in sql_items[key]}
            
            if combined_hashes != sql_hashes:
                to_update.extend([
                    {
                        'sql_id': sql_item['id'],
                        'combined_data': next(c for c in combined_details 
                                            if (c['folio'], c.get('ref', '')) == key 
                                             and c['detail_hash'] != sql_item['hash_detalle']),
                        'sql_data': sql_item
                    }
                    for sql_item in sql_items[key]
                    if sql_item['hash_detalle'] not in combined_hashes
                ])
      
        return {
            "operations": {
                "create": in_combined_only,
                "update": to_update,
                "delete": to_delete
            },
            "metadata": {
                "total_combined": len(combined_details),
                "total_sql": len(sql_records),
                "duplicate_count": sum(max(0, sql_counts[k] - combined_counts.get(k, 0)) 
                                    for k in sql_counts)
            }
        }

    def print_sync_report(self, analysis_result):
        """Updated print function with accurate duplicate counts"""
        ops = analysis_result['operations']
        meta = analysis_result['metadata']
        
        print("=== Synchronization Report ===")
        
        print(f"\nCREATE ({len(ops['create'])} records):")
        for item in ops['create']:
            print(f"  - Folio: {item['folio']}, ref: {item.get('ref', '')}")

        print(f"\nUPDATE ({len(ops['update'])} records):")
        for item in ops['update']:
            print(f"  - Folio: {item['sql_data']['folio']}, ref: {item['sql_data'].get('ref', '')}")
            print(f"    SQL ID: {item['sql_id']}")
            print(f"    Old Hash: {item['sql_data']['hash_detalle']}")
            print(f"    New Hash: {item['combined_data']['detail_hash']}")

        print(f"\nDELETE ({len(ops['delete'])} records):")
        delete_reasons = {}
        for item in ops['delete']:
            key = (item['folio'], item.get('ref', ''))
            if key not in delete_reasons:
                delete_reasons[key] = {
                    'count': 1,
                    'type': 'ORPHANED'  # Default reason
                }
            else:
                delete_reasons[key]['count'] += 1

        for key, reason in delete_reasons.items():
            print(f"  - Folio: {key[0]}, ref: {key[1]} ({reason['type']})")
            print(f"    Count: {reason['count']} record(s) to delete")

        print("\n=== Summary ===")
        print(f"Total Combined Records: {meta['total_combined']}")
        print(f"Total SQL Records: {meta['total_sql']}")
        print(f"Actions Needed: {len(ops['create']) + len(ops['update']) + len(ops['delete'])}")
        print(f"  - Create: {len(ops['create'])}")
        print(f"  - Update: {len(ops['update'])}")
        print(f"  - Delete: {len(ops['delete'])} (includes {meta['duplicate_count']} duplicates)")


    
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