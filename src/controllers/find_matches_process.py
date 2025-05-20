import os
import sys
from turtle import st
from pathlib import Path

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from datetime import datetime, timedelta
from src.controllers.ventas_controller import VentasController
from src.dbf_enc_reader.mapping_manager import MappingManager
from src.config.dbf_config import DBFConfig
from src.models.ventas_model import VentasModel
from src.controllers.dbf_sql_comparator import DBFSQLComparator

class MatchesProcess:

    def __init__(self) -> None:
        # Database configuration
        self.db_config = {
            'host': 'localhost',
            'database': 'suc_vel',
            'user': 'postgres',
            'password': 'comexcare',
            'port': '5432'
        }
        
        # Initialize the comparator
        self.comparator = DBFSQLComparator(self.db_config)

    def compare_batches(self, config):
        
        #TO DO : this config may be pass as a parameter and not defined here, but just for testing
       
        # Test date range (April 19-20, 2025)
        start_date = datetime(2025, 5, 5)# yyyy, dd, mm
        end_date = datetime(2025, 5, 5)
        
        
        #fetch dbf data
        dbf_results = self.get_dbf_data(config, start_date, end_date)
        
        # Obtener registros SQL
        sql_records = self.get_sql_data(start_date, end_date)
        
        if not sql_records:
            print(sql_records)
            print(f"No hay registros en SQL entre {start_date} y {end_date}. insertando nuevos registros")
            self.insert_process(dbf_results)
      
        comparison_result = self.comparator.compare_batch_by_day(dbf_records=dbf_results, sql_records=sql_records)

        print(f' ----- ')
        
        if  comparison_result.get('detailed_comparison'):
            self.print_comparison_results(comparison_result['detailed_comparison'])
        
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
        
        db_config = {
            'host': 'localhost',
            'database': 'suc_vel',
            'user': 'postgres',
            'password': 'comexcare',
            'port': '5432'
        }
        
        tracker = PostgresTracking(db_config)
        return tracker.get_records_by_date_range(start_date, end_date)


    def insert_process(self, dbf_result):
        """
        Proceso completo de inserción usando transacción atómica
        Nota: La tabla lotes ya no usa campo sublote
        """
        from src.db.postgres_tracking import PostgresTracking
        from datetime import datetime
        import logging
        
        db_config = {
            'host': 'localhost',
            'database': 'suc_vel',
            'user': 'postgres',
            'password': 'comexcare',
            'port': '5432'
        }
        
        tracker = PostgresTracking(db_config)
        lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Preparar datos para inserción (todos los registros)
        batch_data = []
        valid_records = 0

        print(f"Processing {len(dbf_result['data'])} records...")
        
        for i, record in enumerate(dbf_result['data'], 1):  # Procesar todos los registros
            # Validar campos obligatorios (usando las mayúsculas exactas del DBF)
            folio = record.get('Folio') # Mayúscula según el DBF
            fecha = record.get('fecha')  # Minúscula según el DBF
            
            if not folio or not fecha:
                logging.warning(f"Registro inválido - Folio: {folio}, Fecha: {fecha}. Datos completos: {record}")
                continue
                
            try:
                # Procesar fecha (formato: 'mm/dd/yyyy HH:MM:SS a. m./p. m.')
                fecha_str = fecha.split()[0]  # Tomar solo la parte de la fecha
                fecha_emision = datetime.strptime(fecha_str, '%m/%d/%Y').date()
                print(f"Parsed date {fecha_str} as {fecha_emision} (MM/DD/YYYY format)")
                
                # Validar hash
                md5_hash = record.get('md5_hash')
                if not md5_hash or len(md5_hash) != 32:
                    md5_hash = hashlib.md5(str(record).encode()).hexdigest()
                    logging.warning(f"Hash inválido para folio {folio}. Generado nuevo hash")
                
                batch_data.append({
                    'folio': folio,
                    'total_partidas': len(record.get('detalles', [])) if record.get('detalles') is not None else 0,
                    'descripcion': f"empleado : {record.get('empleado')}",
                    'hash': md5_hash,
                    'fecha_emision': fecha_emision
                })
                valid_records += 1
                
            except ValueError as e:
                logging.warning(f"Error procesando registro {folio}: {str(e)}. Fecha: {fecha}")
                continue
        
        if not valid_records:
            logging.error("No hay registros válidos para insertar")
            return False
        
        # Obtener fecha referencia del primer registro válido
        fecha_referencia = batch_data[0]['fecha_emision']
        
        # Ejecutar transacción completa
        success = tracker.insert_full_batch_transaction(
            batch_data=batch_data,
            lote_id=lote_id,
            batch_hash=dbf_result['dataset_hash'],
            fecha_referencia=fecha_referencia
        )
        
        if success:
            print(f"Proceso completado. Insertados {len(batch_data)} registros en lote {lote_id}")
        else:
            print("Error en el proceso de inserción")
        
        return success

    # Comparison methods have been moved to DBFSQLComparator class

    def print_comparison_results(self, detailed_comparison):
        print("\n=================================================")
        print("=== API OPERATIONS SUMMARY ===")
        print("=================================================\n")
        
        # More debug prints
        print(f"DEBUG: detailed_comparison type: {type(detailed_comparison)}")
        print(f"DEBUG: detailed_comparison content: {detailed_comparison}")
        
        # Print summary statistics
        summary = detailed_comparison.get('summary', {})
        print(f"Total DBF Records: {detailed_comparison.get('total_dbf_records', 0)}")
        print(f"Total SQL Records: {detailed_comparison.get('total_sql_records', 0)}")
        print(f"Status: {detailed_comparison.get('status', 'unknown')}\n")
        
        print("API Operations Required:")
        print(f"  CREATE: {summary.get('create_count', 0)} records")
        print(f"  UPDATE: {summary.get('update_count', 0)} records")
        print(f"  DELETE: {summary.get('delete_count', 0)} records")
        print(f"  NO ACTION: {summary.get('matching_count', 0)} records")
        print(f"  TOTAL ACTIONS: {summary.get('total_actions_needed', 0)} operations\n")
        
        # Get API operations
        api_ops = detailed_comparison.get('api_operations', {})
        
        # Print sample of records to create (DBF only)
        create_records = api_ops.get('create', [])
        if create_records:
            print("\n=== SAMPLE RECORDS TO CREATE (DBF only) ===")
            for i, record in enumerate(create_records[:3], 1):
                print(f"\nRecord #{i}:")
                print(f"  Folio: {record.get('Folio')}")
                if 'md5_hash' in record:
                    print(f"  Hash: {record.get('md5_hash')}")
                if 'fecha' in record:
                    print(f"  Fecha: {record.get('fecha')}")
            if len(create_records) > 3:
                print(f"\n... and {len(create_records) - 3} more records to create")
        
        # Print sample of records to update (mismatched)
        update_records = api_ops.get('update', [])
        if update_records:
            print("\n=== SAMPLE RECORDS TO UPDATE (hash mismatch) ===")
            for i, record in enumerate(update_records[:3], 1):
                print(f"\nRecord #{i}:")
                print(f"  Folio: {record.get('folio')}")
                print(f"  DBF Hash: {record.get('dbf_hash')}")
                print(f"  SQL Hash: {record.get('sql_hash')}")
            if len(update_records) > 3:
                print(f"\n... and {len(update_records) - 3} more records to update")
        
        # Print sample of records to delete (SQL only)
        delete_records = api_ops.get('delete', [])
        if delete_records:
            print("\n=== SAMPLE RECORDS TO DELETE (SQL only) ===")
            for i, record in enumerate(delete_records[:3], 1):
                print(f"\nRecord #{i}:")
                print(f"  Folio: {record.get('folio')}")
                if 'hash' in record:
                    print(f"  Hash: {record.get('hash')}")
                if 'fecha_emision' in record:
                    print(f"  Fecha: {record.get('fecha_emision')}")
            if len(delete_records) > 3:
                print(f"\n... and {len(delete_records) - 3} more records to delete")
        
        print("\n=================================================\n")
