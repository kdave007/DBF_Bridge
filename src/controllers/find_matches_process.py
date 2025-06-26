import os
import sys
from turtle import st
from pathlib import Path
from src.config.db_config import PostgresConnection
import hashlib
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from datetime import date, timedelta
from src.controllers.ventas_controller import VentasController
from src.dbf_enc_reader.mapping_manager import MappingManager
from src.config.dbf_config import DBFConfig
from src.models.ventas_model import VentasModel
from src.controllers.dbf_sql_comparator import DBFSQLComparator
from src.controllers.insertion_process import InsertionProcess
from src.db.retries_tracking import RetriesTracking

class MatchesProcess:

    def __init__(self) -> None:
        # Get database configuration
        self.db_config = PostgresConnection.get_db_config()
        
        # Initialize the comparator and insertion processor
        self.comparator = DBFSQLComparator(self.db_config)
        self.insertion_processor = InsertionProcess(self.db_config)

        self.retry_tracker = RetriesTracking(self.db_config)

    def compare_data(self, config, start_date, end_date):
        
        #TO DO : this config may be pass as a parameter and not defined here, but just for testing
       
        # Test date range using a date we know exists in the DBF
        # From the image you shared, we can see a record with date 20/03/2025
        # Let's use exactly that date for our test
        # Note: The date in the DBF is in DD/MM/YYYY format
            
        
        # Print the exact format we're looking for to help debug
        print(f"Looking for records with date exactly matching: {start_date} - {end_date}")
        
        
        #fetch dbf data
        dbf_results = self.get_dbf_data(config, start_date, end_date)

        print(dbf_results)

        # Process DBF data through DataMap for API formatting
        dbf_results = self.db_map_implementations(dbf_results)
        
        # Obtener registros SQL
        sql_records = self.get_sql_data(start_date, end_date)
        
        if not sql_records:
            print(f"No hay registros en SQL entre {start_date} y {end_date}. Insertando nuevos registros")
            # When no SQL records, use add_all to directly process all DBF records
            comparison_result = self.comparator.add_all(dbf_records=dbf_results)
        else:
            # When SQL records exist, compare them with DBF records
            comparison_result = self.comparator.compare_records_by_hash(dbf_records=dbf_results, sql_records=sql_records, start_date=start_date, end_date=end_date)
        
        # Print summary of operations
        self.print_comparison_results(comparison_result)

        self.dischard_by_retries(comparison_result, start_date, end_date)

        # print('STOP')
        # sys.exit()


        
        
        
        # Return the full result for programmatic use
        return comparison_result
                
        
        
        # print(f"\nRegistros SQL encontrados: {len(dbf_results)}")
        # print(f"\nMostrando 2 registros de ejemplo:")
        # for idx, record in enumerate(dbf_results['data'][:2], 1):
        #     print(f"\nRecord #{idx}:")
        #     print(f"Folio: {record.get('Folio')}")
        #     print(f"MD5 Hash: {record.get('md5_hash')}")
        #     print(f"Details: {len(record.get('detalles', []))} items")
        # print(f"\nTotal records: {len(dbf_results['data'])}")

        
        
    def get_dbf_data(self, config, start_date, end_date):
        """Obtiene datos DBF y agrega hashes MD5"""
        import hashlib
        import json
        
        # Initialize mapping manager
        mapping_file = Path(project_root) / "mappings.json"
        mapping_manager = MappingManager(str(mapping_file))
        controller = VentasController(mapping_manager, config)
        
        # Obtener datos originaales
        data = controller.get_sales_in_range(start_date, end_date)
        
        # Agregar hash MD5 a cada registro
        for i, record in enumerate(data):
            record_str = json.dumps(record, sort_keys=True)
            record['md5_hash'] = hashlib.md5(record_str.encode('utf-8')).hexdigest()
        
        # Generar hash para todo el dataset
        dataset_str = json.dumps(data, sort_keys=True)
        dataset_hash = hashlib.md5(dataset_str.encode('utf-8')).hexdigest()
        
        return {
            'data': data,
            'dataset_hash': dataset_hash,
            'record_count': len(data)
        }

    def get_sql_data(self, start_date, end_date):
        """Obtiene datos SQL para comparación"""
        from src.db.postgres_tracking import PostgresTracking
        
        # Get database configuration from PostgresConnection
        db_config = PostgresConnection.get_db_config()
        
        tracker = PostgresTracking(db_config)
        return tracker.get_records_by_date_range(start_date, end_date)


    # The insert_process method has been moved to the InsertionProcess class

    # Comparison methods have been moved to DBFSQLComparator class

    def print_comparison_results(self, detailed_comparison):
        print("\n=================================================")
        print("=== API OPERATIONS SUMMARY ===")
        print("=================================================\n")
        
        # # More debug prints
        # print(f"DEBUG: detailed_comparison type: {type(detailed_comparison)}")
        # print(f"DEBUG: detailed_comparison content: {detailed_comparison}")
        
        # Print summary statistics
        summary = detailed_comparison.get('summary', {})
        print(f"Total DBF Records: {detailed_comparison.get('total_dbf_records', 0)}")
        print(f"Total SQL Records: {detailed_comparison.get('total_sql_records', 0)}")
        print(f"Status: {detailed_comparison.get('status', 'unknown')}\n")
        
        print("API Operations Required:")
        print(f"  CREATE: {summary.get('create_count', 0)} records")
        print(f"  UPDATE: {summary.get('update_count', 0)} records")
        print(f"  DELETE: {summary.get('delete_count', 0)} records")
        print(f"  TOTAL ACTIONS: {summary.get('total_actions_needed', 0)} operations\n")
        
        # Get API operations
        api_ops = detailed_comparison.get('api_operations', {})
        
        # Print sample of records to create (DBF only)
        # create_records = api_ops.get('create', [])
        # if create_records:
        #     print("\n=== SAMPLE RECORDS TO CREATE (DBF only) ===")
        #     for i, record in enumerate(create_records[:3], 1):
        #         print(f"\nRecord #{i}:")
        #         print(f"  Folio: {record.get('folio')}")
        #         if 'md5_hash' in record:
        #             print(f"  Hash: {record.get('md5_hash')}")
        #         if 'fecha' in record:
        #             print(f"  Fecha: {record.get('fecha')}")
        #     if len(create_records) > 3:
        #         print(f"\n... and {len(create_records) - 3} more records to create")
        
        # # Print sample of records to update (mismatched)
        # update_records = api_ops.get('update', [])
        # if update_records:
        #     print("\n=== SAMPLE RECORDS TO UPDATE (hash mismatch) ===")
        #     for i, record in enumerate(update_records[:3], 1):
        #         print(f"\nRecord #{i}:")
        #         print(f"  Folio: {record.get('folio')}")
        #         print(f"  DBF Hash: {record.get('dbf_hash')}")
        #         print(f"  SQL Hash: {record.get('sql_hash')}")
        #     if len(update_records) > 3:
        #         print(f"\n... and {len(update_records) - 3} more records to update")
        
        # # Print sample of records to delete (SQL only)
        # delete_records = api_ops.get('delete', [])
        # if delete_records:
        #     print("\n=== SAMPLE RECORDS TO DELETE (SQL only) ===")
        #     for i, record in enumerate(delete_records[:3], 1):
        #         print(f"\nRecord #{i}:")
        #         print(f"  Folio: {record.get('folio')}")
        #         if 'hash' in record:
        #             print(f"  Hash: {record.get('hash')}")
        #         if 'fecha_emision' in record:
        #             print(f"  Fecha: {record.get('fecha_emision')}")
        #     if len(delete_records) > 3:
        #         print(f"\n... and {len(delete_records) - 3} more records to delete")
        
        print("\n=================================================\n")


    def db_map_implementations(self, dbf_results):
        from src.utils.post_data_map import DataMap
        
        # Initialize the DataMap class
        data_mapper = DataMap()
        
        # Create a deep copy of the original structure to preserve it
        processed_results = dbf_results.copy()
        
        if dbf_results and 'data' in dbf_results and dbf_results['data']:
            for i, record in enumerate(dbf_results['data']):
                # Check if this is a valid invoice record with the expected structure
                if 'Cabecera' in record and record['Cabecera'] == 'FA':
                    # Process the header (factura)
                    header_data = {
                        'Cabecera': record['Cabecera'],
                        'Folio': record['Folio'],
                        'cliente': record.get('cliente'),
                        'empleado': record.get('empleado'),
                        'fecha': record.get('fecha'),
                        'total_bruto': record.get('total_bruto'),
                        'hor': record.get('hor'),
                        'fpg': record.get('fpg'),
                        'md5_hash': record.get('md5_hash')
                    }
                    
                    # Get mapped fields for the header
                    header_mapped = data_mapper.process_record_fac(header_data)
                    
                    # Add mapped fields to the original record
                    for key, value in header_mapped.items():
                        processed_results['data'][i][key] = value
                    
                    # Process the detail records if they exist
                    if 'detalles' in record and isinstance(record['detalles'], list):
                        for j, detail in enumerate(record['detalles']):
                            # Add any missing references from the header that might be needed
                            detail_with_refs = detail.copy()
                            
                            # Generate hash from original detail before adding any mapped fields
                            detail_str = str(sorted(detail.items()))
                            detail_with_refs['detail_hash'] = hashlib.md5(detail_str.encode()).hexdigest()
                            
                            # Add references from header
                            detail_with_refs['metodo_pago'] = record.get('fpg')  # Copy payment method from header
                            detail_with_refs['hor'] = record.get('hor')
                            detail_with_refs['emp'] = record.get('emp')
                            detail_with_refs['emp_div'] = record.get('emp_div')
                            detail_with_refs['ser_vta'] = record.get('ser_vta')
                            detail_with_refs['clt'] = record.get('clt')
                            
                            # Get mapped fields for the detail
                            detail_mapped = data_mapper.process_record_det(detail_with_refs)
                            
                            # Add mapped fields to the original detail record
                            for key, value in detail_mapped.items():
                                processed_results['data'][i]['detalles'][j][key] = value
        
        return processed_results

    def dischard_by_retries(self, records, start_date, end_date):
        """Remove records that have exceeded retry attempts
        
        Args:
            records: Dictionary containing operation lists within 'api_operations' key
            start_date: Start date for retry tracking filter
            end_date: End date for retry tracking filter
        """
        # Get folios to ignore based on retry count and date range
        target_folios = self.retry_tracker.get_ignore_list(start_date, end_date)

        if not target_folios:
            return
            
        print(f"Found {len(target_folios)} folios to discard due to retry limits")
        
        # Make sure api_operations key exists
        if 'api_operations' not in records:
            return

        # Process each operation type if it exists
        operations = ['create', 'update', 'delete', 'next_check']
        
        for operation in operations:
            if operation in records['api_operations'] and records['api_operations'][operation]:
                # Call synch_operations with the correct parameters
                self.synch_operations(
                    records['api_operations'][operation],  # Records list
                    target_folios,       # Folios to remove
                    records['summary'],  # Summary dictionary
                    operation            # Operation type
                )
        
        # Final processing complete - no additional prints needed


    def synch_operations(self, records, target_folios, summary, operation):
        """Remove records with folios that match those in target_folios list
        
        Args:
            records: List of record dictionaries to filter
            target_folios: List of folios to remove
            summary: Dictionary containing summary counts
            operation: Operation type (create, update, delete, next_check)
        """
        if not records or not target_folios:
            return
            
        # Convert target_folios to a set of strings for consistent comparison
        str_folio_set = {str(folio) for folio in target_folios}
        
        # Keep track of indices to remove
        indices_to_remove = []
        
        # Find all records with matching folios
        for i, record in enumerate(records):
            folio = record.get('folio')
            if folio is not None:
                # Convert to string for consistent comparison
                str_folio = str(folio)
                if str_folio in str_folio_set:
                    indices_to_remove.append(i)
                    print(f"Ignoring {operation} record with folio {folio} due to retry limit exceeded")
        
        # Remove records in reverse order to avoid index shifting
        for index in sorted(indices_to_remove, reverse=True):
            records.pop(index)
        
        # Update summary counters
        if indices_to_remove:
            removed_count = len(indices_to_remove)
            
            # Update the specific operation counter
            counter_key = f"{operation}_count"
            
            if counter_key in summary:
                summary[counter_key] -= removed_count
            
            # Update the total actions needed
            if 'total_actions_needed' in summary:
                summary['total_actions_needed'] -= removed_count
