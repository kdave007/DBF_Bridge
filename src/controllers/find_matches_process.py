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

class MatchesProcess:

    def __init__(self) -> None:
        pass

    def compare_batches(self):
        
        #TO DO : this config may be pass as a parameter and not defined here, but just for testing
        config = DBFConfig(
            dll_path=r"C:\Program Files (x86)\Advantage 10.10\ado.net\1.0\Advantage.Data.Provider.dll",
            encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
            source_directory=r"C:\Users\campo\Documents\projects\DBF_encrypted\pospcp",
            limit_rows=500  # Limit to 3 sales for testing
        )
        # Test date range (April 19-20, 2025)
        start_date = datetime(2025, 5, 4)# yyyy, dd, mm
        end_date = datetime(2025, 5, 4)
        
        
        #fetch dbf data
        dbf_results = self.get_dbf_data(config, start_date, end_date)
        
        # Obtener registros SQL
        sql_records = self.get_sql_data(start_date, end_date)
        
        if not sql_records:
            print(f"No hay registros en SQL entre {start_date} y {end_date}. insertando nuevos registros")
            #self.insert_process(dbf_results)
        else:
            result = 0
        
        
        
        print(f"\nRegistros SQL encontrados: {len(dbf_results)}")
        print(f"\nMostrando 2 registros de ejemplo:")
        for idx, record in enumerate(dbf_results['data'][:2], 1):
            print(f"\nRecord #{idx}:")
            print(f"Folio: {record.get('Folio')}")
            print(f"MD5 Hash: {record.get('md5_hash')}")
            print(f"Details: {len(record.get('detalles', []))} items")
        print(f"\nTotal records: {len(dbf_results['data'])}")

        
        
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
        for record in data:
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
        
        # Preparar datos para inserción (solo primeros 4 registros para debug)
        batch_data = []
        valid_records = 0

        print(dbf_result['data'][:1])
        
        for record in dbf_result['data'][:4]:  # Solo primeros 4 registros
            # Validar campos obligatorios (usando las mayúsculas exactas del DBF)
            folio = record.get('Folio')  # Mayúscula según el DBF
            fecha = record.get('fecha')  # Minúscula según el DBF
            
            if not folio or not fecha:
                logging.warning(f"Registro inválido - Folio: {folio}, Fecha: {fecha}. Datos completos: {record}")
                continue
                
            try:
                # Procesar fecha (formato: 'dd/mm/yyyy HH:MM:SS a. m./p. m.')
                fecha_str = fecha.split()[0]  # Tomar solo la parte de la fecha
                fecha_emision = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                
                # Validar hash
                md5_hash = record.get('md5_hash')
                if not md5_hash or len(md5_hash) != 32:
                    md5_hash = hashlib.md5(str(record).encode()).hexdigest()
                    logging.warning(f"Hash inválido para folio {folio}. Generado nuevo hash")
                
                batch_data.append({
                    'folio': folio,
                    'total_partidas': len(record.get('detalles', [])) if record.get('detalles') is not None else 0,
                    'descripcion': f"Factura {folio}",
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

    def dbf_sql_comparison(dbf_records, sql_records):
        pass
