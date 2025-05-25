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
        print(f' {self.db}')

        combined = self.process_results(results)

        for item in combined:
            print(f' combined item {item}')

        # Get existing records from database
        sql_records = self.get_sql_records(self.db, start_date, end_date)
        
        # Insert new records to database
        if combined:
            inserted = self.insert_records(self.db, combined)
            print(f"Inserted/updated {inserted} records")
            


    def process_results(self, results):
        combined_details = []
        
        for operation in ['create', 'update']:
            if operation in results and 'success' in results[operation]:
                for record in results[operation]['success']:
                    if 'details' in record:
                        for detail in record['details']:
                            detail_with_operation = detail.copy()
                            detail_with_operation['operation'] = operation
                            detail_with_operation['folio'] = record.get('folio')
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