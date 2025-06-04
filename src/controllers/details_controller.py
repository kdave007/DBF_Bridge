import os
import sys
from datetime import date
import hashlib
from turtle import reset

from requests import post
from src.config.db_config import PostgresConnection
from src.db.detail_tracking import DetailTracking
from .send_details import SendDetails



class DetailsController:
    def __init__(self) -> None:
        # Get database configuration as a dictionary
        self.db_config = PostgresConnection.get_db_config()
        # Create a PostgresConnection instance if needed
        self.db = self.db_config
        
    def process(self, results, start_date, end_date):

        send_details = SendDetails()

        # Process API results to get combined detail records, fetch them out of every CA and merge details in one dictionary
        combined = self.process_results(results)
        delete_folios = self.process_delete_results(results)

        # Get existing records from database
        sql_records = self.get_sql_records(self.db, start_date, end_date)

        if not sql_records :
            #post all records found in the dbf data
            print("Inserting all records found, cause no sql data found")
            post_results = send_details.req_post(combined)
            print(f' post results {post_results['records']}')

            if post_results['records']:
                #insert all the records posted that succeeded
                self.insert_records(self.db, post_results['records'])
        else :      
            # Compare records to find differences
            print(f'COMBINED {combined}')
            comparison_result = self.analyze_sync(combined, sql_records)
            # Print comparison summary
            self.print_sync_report(comparison_result)

            # Process operations based on comparison results
            self.process_operations(comparison_result, send_details)
        
            

   
    def process_results(self, results):
        combined_details = []
 
        for operation in ['create', 'update','next_check']:
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
                            
                            detail_with_operation['parent_id'] = record.get('id')#pass the parent ca id to the detail

                            detail_with_operation['folio'] = record.get('folio')

                            detail_with_operation['detail_hash'] = detail.get('detail_hash')
                            
                            # Parse date properly - extract just the date part (remove time)
                            fecha_str = record.get('fecha_emision')
                            if fecha_str:
                                # Extract just the date part (before any space)
                                fecha_parts = fecha_str.split(' ')
                                if fecha_parts:
                                    detail_with_operation['fecha'] = fecha_parts[0]
                            
                            # # Generate MD5 hash from detail content
                            # detail_str = str(sorted(detail.items()))
                            # detail_with_operation['detail_hash'] = hashlib.md5(detail_str.encode()).hexdigest()

                            combined_details.append(detail_with_operation)
                            
        
        # Fetch and append next_check records
        next_check_records = self.fetch_next_check(results)
        if next_check_records:
            print(f' APPENDING {len(next_check_records)} NEXT_CHECK RECORDS')
            combined_details.extend(next_check_records)
        
        print(' ---   --- --- --- --- --- ---')
        print('  COMBINED RESULT:')
        for i, item in enumerate(combined_details):
            print(f'  {i+1}. {item}')

       
        
        return combined_details

    def fetch_next_check(self, results):
        print(f'NEXT_CHECK RECORDS: {results.get("next_check", [])}')
        
        combined = []
        if 'next_check' in results and results['next_check']:
            for i, record in enumerate(results['next_check']):
                #print(f'Processing next_check record {i+1}: {record}')
                
                # Get the dbf_record which contains the details
                dbf_record = record.get('dbf_record', {})
                
                # Extract details from dbf_record
                if 'detalles' in dbf_record:
                    for detail in dbf_record['detalles']:
                        # Create a copy to avoid modifying the original
                        detail_with_operation = detail.copy()
                        
                        # Convert REF to lowercase ref if it exists
                        ref = detail.get('REF') or detail.get('ref', '')
                        
                        # Parse date from dbf_record
                        fecha_str = dbf_record.get('fecha')
                        fecha = ''
                        if fecha_str:
                            # Extract just the date part (before any space)
                            fecha_parts = fecha_str.split(' ')
                            if fecha_parts:
                                fecha = fecha_parts[0]
                        
                        # Generate MD5 hash from detail content
                        # detail_str = str(sorted(detail.items()))
                        # detail_hash = hashlib.md5(detail_str.encode()).hexdigest()
                        
                        combined.append({
                            'ref': ref,
                            'folio': record.get('folio'),
                            'parent_id': record.get('id'),
                            'fecha': fecha,
                            'detail_hash':detail.get('detail_hash'),
                            'operation': 'next_check'  # Mark as next_check operation
                        })
        
        print(f'FETCH NEXT CHECK RESULT: {len(combined)} records processed')
        for i, item in enumerate(combined):
            print(f'  {i+1}. {item}')
                
        return combined

    def get_sql_records(self, db_connection, start_date, end_date):
        # Create a DetailTracking instance with the database configuration
        # db_connection is now a dictionary, not a PostgresConnection object
        detail_tracker = DetailTracking(db_connection)
        
        # Use the get_details_by_date_range method from DetailTracking
        records = detail_tracker.get_details_by_date_range(start_date, end_date)
        
        print(f"Found {(records)} records between {start_date} and {end_date}")
        return records
        
    def insert_records(self, db_connection, records):
        """
        Insert or update records in the database
        
        Args:
            db_connection: Database configuration dictionary
            records: List of records to insert/update
            
        Returns:
            Number of records successfully processed
        """
        if not records:
            return 0
            
        # Create a DetailTracking instance with the database configuration
        # db_connection is now a dictionary, not a PostgresConnection object
        detail_tracker = DetailTracking(db_connection)
        
        # Use batch_insert_details method to insert all records at once
        success = detail_tracker.batch_replace_by_id(records)
        
        if success:
            return len(records)
        else:
            print("Error inserting records")
            return 0

    def process_operations(self, data, send_details): #receive data and send_details object to post data
        operations = data["operations"]
        
        # Process CREATE operations if data exists
        if operations["create"]:  # Checks if list is non-empty
            # Add your create validation/processing logic here
            print(f"Processing CREATE operations:{operations["create"]}")
            create_result = send_details.req_post(operations["create"])
            self.insert_records(self.db, create_result['records'])

        
        # Process UPDATE operations if data exists
        if operations["update"]:  # Checks if list is non-empty
            # Add your update validation/processing logic here
            print(f"Processing UPDATE operations:{operations["update"]}")
            create_result = send_details.req_update(operations["update"])
            self.insert_records(self.db, operations['update'])
          
        
        # Process DELETE operations if data exists
        if operations["delete"]:  # Checks if list is non-empty
            print(f"Processing {len(operations['delete'])} delete operations")
            delete_result = self.delete_by_id(operations["delete"])
            print(f"Delete operations completed: {sum(1 for r in delete_result if r.get('success', False))} successful, {sum(1 for r in delete_result if not r.get('success', False))} failed")
            
    
            
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
        # for item in sql_records:
        #     # For SQL records, use 'ref' field from the alias in the SQL query
        #     key = (item['folio'], item.get('ref', ''))
        #     sql_counts[key] = sql_counts.get(key, 0) + 1
        #     sql_items.setdefault(key, []).append(item)

        for item in sql_records:
            # Convertir 'folio' (Decimal) a str y 'ref' (si existe) a str
            folio_str = str(item['folio'])  # Convertimos Decimal('287734') -> '287734'
            ref_str = str(item.get('ref', ''))  # Por si 'ref' es None o ya es str
            
            key = (folio_str, ref_str)  # Ahora key es (str, str)
            
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
        # 1. Mostrar las claves de ambos diccionarios para comparar
        print("\n=== DEBUG: Comparando sql_counts vs combined_counts ===")
        print("Claves en sql_counts:", set(sql_counts))
        print("Claves en combined_counts:", set(combined_counts))

        # 2. Calcular la diferencia y mostrarla
        difference = set(sql_counts) - set(combined_counts)
        print("\nClaves en SQL que NO están en combined_counts (se eliminarán):", difference)

        # 3. Si hay diferencia, mostrar registros afectados y añadir a to_delete
        if difference:
            print("\nDetalle de registros a eliminar:")
            for key in difference:
                print(f"\n- Clave '{key}' no encontrada en combined_counts.")
                print("  Registros en SQL:", sql_items.get(key, "NO EXISTE"))
                # Add these records to the to_delete list
                if key in sql_items:
                    to_delete.extend(sql_items[key])
        else:
            print("\n✅ No hay diferencias, no se eliminará nada.")
        
        # Process common records
        for key in set(combined_counts) & set(sql_counts):
            combined_count = combined_counts[key]
            sql_count = sql_counts[key]
            
            if sql_count > combined_count:
                excess = sql_count - combined_count
                to_delete.extend(sql_items[key][-excess:])

            # Nueva lógica simplificada para updates
            combined_master = next((c for c in combined_details 
                                if (c['folio'], c['ref']) == key), None)
            
            if combined_master:
                print(f' master {combined_master}')
                for sql_item in sql_items[key]:
                    if sql_item['hash_detalle'] != combined_master['detail_hash']:
                        to_update.append({
                            'sql_id': sql_item['id'],
                            'folio': key[0],
                            'ref': key[1],
                            'fecha': combined_master['fecha'],
                            'old_hash': sql_item['hash_detalle'],
                            'detail_hash': combined_master['detail_hash'],
                            'accion':'modificado',
                            'details': combined_master,
                            'parent_id': combined_master.get('parent_id',None)
                        })
        print(f'PARA BORRAR delete {to_delete}')
        print(f'PARA BORRAR diff {difference}')
      
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
            print(f"  - Folio: {item['folio']}, ref: {item.get('ref', '')}")
            print(f"    SQL ID: {item['sql_id']}")
            print(f"    Old Hash: {item['old_hash']}")
            print(f"    New Hash: {item['detail_hash']}")


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


    # def process_delete_results(self, results):
    #     """
    #     Process API results to extract records from delete.success
        
    #     Args:
    #         results: API response containing delete.success records
            
    #     Returns:
    #         List of processed records from delete.success
    #     """
    #     delete_records = []
        
    #     # Check if delete operation exists and has success records
    #     if 'delete' in results and 'success' in results['delete']:
    #         for record in results['delete']['success']:
    #             print(f'  PROCESSING DELETE RECORD: {record}')
                
    #             # Extract folio from the record
    #             folio = record.get('folio')
    #             if not folio:
    #                 print(f"  WARNING: Record missing folio, skipping: {record}")
    #                 continue
                
    #             # Create a record with the folio and operation type
    #             delete_record = {
    #                 'folio': folio,
    #                 'operation': 'delete'
    #             }
                
                
    #             # Add the record to our list
    #             delete_records.append(delete_record)
    #             print(f'  ADDED DELETE RECORD: {delete_record}')
        
    #     print(f'TOTAL DELETE RECORDS PROCESSED: {len(delete_records)}')
    #     return delete_records

    
    def process_delete_results(self, records):
        """
        Elimina detalles de la base de datos y del API basado en los folios
        
        Args:
            records: Lista de registros con folios a eliminar
            
        Returns:
            Dictionary con resultados de la operación
        """
        # Inicializar el objeto de seguimiento de detalles y el objeto para enviar detalles
        detail_tracking = DetailTracking(self.db_config)
        send_details = SendDetails()
        
        # Resultados para seguimiento
        results = {
            'total_folios': len(records),
            'folios_processed': 0,
            'details_found': 0,
            'details_deleted_api': 0,
            'details_deleted_db': 0,
            'errors': 0
        }
        
        # Procesar cada folio
        print(f'Checking if any records to delete in this proccess : {records}')

        if 'success' not in records['delete'] or not records['delete']['success']:
            print("No delete detail records to process")
            return []

        for record in records['delete']['success']:
            try:
                folio = record.get('folio')
                if not folio:
                    print(f"Registro sin folio, omitiendo: {record}")
                    results['errors'] += 1
                    continue
                
                print(f"\nProcesando eliminación para folio: {folio}")
                
                # Obtener todos los detalles asociados al folio de la base de datos
                db_details = detail_tracking.get_details_by_folio(folio)
                results['details_found'] += len(db_details)
                
                if not db_details:
                    print(f"No se encontraron detalles para el folio {folio} en la base de datos")
                    continue
                
                print(f"Encontrados {len(db_details)} detalles para el folio {folio}")
                
                # Enviar solicitud de eliminación al API para cada detalle
                delete_result = send_details.delete_post(db_details)
                
                # Actualizar contadores basados en el resultado
                results['details_deleted_api'] += delete_result['success']
                
                # Si la eliminación en el API fue exitosa, eliminar de la base de datos
                if delete_result['success'] > 0:
                    # Eliminar de la base de datos por folio
                    db_delete_result = detail_tracking.delete_by_folio(folio)
                    if db_delete_result:
                        print(f"Eliminados registros del folio {folio} de la base de datos")
                        results['details_deleted_db'] += len(db_details)
                    else:
                        print(f"Error al eliminar registros del folio {folio} de la base de datos")
                        results['errors'] += 1
                
                results['folios_processed'] += 1
                
            except Exception as e:
                print(f"Error al procesar folio para eliminación: {str(e)}")
                results['errors'] += 1
        
        # Imprimir resumen
        print("\n=== Resumen de Eliminación ===")
        print(f"Folios procesados: {results['folios_processed']}/{results['total_folios']}")
        print(f"Detalles encontrados: {results['details_found']}")
        print(f"Detalles eliminados del API: {results['details_deleted_api']}")
        print(f"Detalles eliminados de la BD: {results['details_deleted_db']}")
        print(f"Errores: {results['errors']}")
        print("==============================\n")
        
        return results

        
    def post_details_by_batches(self, results, delete_url, post_url, max_batch_size=100):
        """
        Process and post details to URLs by folio, handling delete and post operations.
        For each folio: first sends a delete request for that folio, then posts its records.
        
        Args:
            results: API response containing records to process
            delete_url: URL to post delete requests
            post_url: URL to post data in batches
            max_batch_size: Maximum number of records in a batch (default 100)
            
        Returns:
            Dictionary with counts of processed records by operation type
        """
        import requests
        import json


        """
            delete responses :

            200 
            if posting new, we wont be able to delete id first cause id doesnt exists, this is ok!
                {
                    "return": "No se han encontrado todos los registros a eliminar"
                }

            deleted, now insert the new one (updating records)
                {
                    "return": "Eliminado(s) con éxito"
                }

            one of the reocords couldnt delete 
                {
                    "return": "No se ha podido eliminar el registro 1, se deshace la transacción"
                } 
            """
        
        # Set headers for API requests
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-process-json": "true"
        }

        post_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/mov_alm_g"
        self.api_key = "123456"
        
        # Process the results to get the records
        combined_records = self.process_results(results)
        delete_records = self.process_delete_results(results)
        
        # Track results
        result_counts = {
            'folios_processed': 0,
            'delete_requests': 0,
            'delete_success': 0,
            'post_requests': 0,
            'post_records': 0,
            'post_success': 0,
            'errors': 0
        }
        
        # Collect all folios that need processing
        all_folios = set()
        
        # Add folios from combined records
        for record in combined_records:
            folio = record.get('folio')
            if folio:
                all_folios.add(folio)
                
        # Add folios from delete records
        for record in delete_records:
            folio = record.get('folio')
            if folio:
                all_folios.add(folio)
        
        print(f"Found {len(all_folios)} folios to process")
        
        # Group combined records by folio
        records_by_folio = {}
        for record in combined_records:
            folio = record.get('folio')
            if folio:
                if folio not in records_by_folio:
                    records_by_folio[folio] = []
                records_by_folio[folio].append(record)
        
        # Process each folio one at a time
        for folio in all_folios:
            print(f"\nProcessing folio: {folio}")
            
            try:
                # Step 1: Send delete request with refs in the URL path
                # Collect all ref values for this folio
                ref_values = []
                
                # Get all ref values from the records for this folio
                for record in records_by_folio.get(folio, []):
                    if record.get('ref') and record.get('ref') not in ref_values:
                        ref_values.append(record.get('ref'))
                
                # If no refs found, use the folio as the only value
                if not ref_values:
                    ref_values = [folio]
                
                # Create a comma-separated list of refs
                refs_string = ','.join(ref_values)
                
                # Construct URL with refs in the path
                delete_url_with_refs = f"{delete_url}/{folio},{refs_string}"
                
                print(f"Sending delete request to {delete_url_with_refs}")
                delete_response = requests.delete(delete_url_with_refs, headers=headers)  # Using DELETE method with headers
                result_counts['delete_requests'] += 1
                
                if delete_response.status_code == 200:
                    delete_result = delete_response.json()
                    print(f"Delete response: {delete_result}")
                    result_counts['delete_success'] += 1
                else:
                    print(f"Delete request failed with status code {delete_response.status_code}")
                    print(f"Response: {delete_response.text}")
                    result_counts['errors'] += 1
                    continue  # Skip to next folio if delete fails
                
                # Step 2: Send post request with records for this folio
                folio_records = records_by_folio.get(folio, [])
                
                if not folio_records:
                    print(f"No records to post for folio {folio}, skipping post request")
                    continue
                
                # Split records into batches if needed
                batches = []
                for i in range(0, len(folio_records), max_batch_size):
                    batch = folio_records[i:i + max_batch_size]
                    batches.append(batch)
                
                for i, batch in enumerate(batches):
                    print(f"Posting batch {i+1}/{len(batches)} with {len(batch)} records for folio {folio}")
                    # If batch has only one element, send it as a dictionary instead of an array
                    if len(batch) == 1:
                        print(f"Sending single record as dictionary for folio {folio}")
                        post_response = requests.post(post_url, json=batch[0], headers=headers)
                    else:
                        # Send the batch as an array for multiple records
                        print(f"Sending {len(batch)} records as array for folio {folio}")
                        post_response = requests.post(post_url, json=batch, headers=headers)
                    result_counts['post_requests'] += 1
                    result_counts['post_records'] += len(batch)
                    
                    if post_response.status_code == 200:
                        post_result = post_response.json()
                        print(f"Post response: {post_result}")
                        result_counts['post_success'] += 1
                    else:
                        print(f"Post request failed with status code {post_response.status_code}")
                        print(f"Response: {post_response.text}")
                        result_counts['errors'] += 1
                
                result_counts['folios_processed'] += 1
                print(f"Completed processing folio {folio}")
                
            except Exception as e:
                print(f"Error processing folio {folio}: {e}")
                result_counts['errors'] += 1
        
        # Print summary
        print("\n=== Processing Summary ===")
        print(f"Total folios processed: {result_counts['folios_processed']}")
        print(f"Delete requests: {result_counts['delete_requests']} (successful: {result_counts['delete_success']})")
        print(f"Post requests: {result_counts['post_requests']} (successful: {result_counts['post_success']})")
        print(f"Total records posted: {result_counts['post_records']}")
        print(f"Errors encountered: {result_counts['errors']}")
        print("==========================\n")
        
        return result_counts


    def delete_by_id(self, records):
        """
        Delete records from both the API and the database by their IDs
        
        Args:
            records: List of records to delete, each containing at least an 'id' field
            
        Returns:
            List of dictionaries with results of the delete operations
        """
        if not records:
            print("No records to delete")
            return []
            
        send_details = SendDetails()
        detail_tracking = DetailTracking(self.db_config)
        results = []
        
        print(f"Deleting {len(records)} records by ID")
        for record in records:
            # Ensure the record has an ID
            record_id = record.get('id')
            if not record_id:
                print(f"Warning: Record missing ID, cannot delete: {record}")
                results.append({
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    'status_code': 400,
                    'fecha': record.get('fecha'),
                    'success': False,
                    'error': "Missing record ID for deletion",
                    'db_deleted': False
                })
                continue
                
            # Step 1: Call the delete_post method from SendDetails for this record
            delete_result = send_details.delete_post([record])
            
            # Extract the API result for this record
            api_success = False
            result_record = None
            
            if delete_result and 'records' in delete_result and delete_result['records']:
                result_record = delete_result['records'][0]
                api_success = result_record.get('success', False)
            else:
                result_record = {
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    'status_code': 500,
                    'fecha': record.get('fecha'),
                    'success': False,
                    'id': record_id,
                    'error': "Failed to get response from delete operation"
                }
            
            # Step 2: Delete from database if API deletion was successful
            db_deleted = False
            if api_success:
                try:
                    print(f"Deleting record ID {record_id} from database")
                    db_deleted = detail_tracking.delete_by_id(record_id)
                    if db_deleted:
                        print(f"Successfully deleted record ID {record_id} from database")
                    else:
                        print(f"Failed to delete record ID {record_id} from database")
                except Exception as e:
                    print(f"Error deleting record ID {record_id} from database: {e}")
            
            # Add database deletion result to the record
            result_record['db_deleted'] = db_deleted
            results.append(result_record)
                
        # Print summary
        api_success_count = sum(1 for r in results if r.get('success', False))
        db_success_count = sum(1 for r in results if r.get('db_deleted', False))
        print(f"API deletions: {api_success_count}/{len(records)} successful")
        print(f"Database deletions: {db_success_count}/{len(records)} successful")
        
        return results