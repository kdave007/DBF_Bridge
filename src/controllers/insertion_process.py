import logging
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime
from src.db.postgres_tracking import PostgresTracking
from src.config.db_config import PostgresConnection

class InsertionProcess:
    """
    Class responsible for handling data insertion processes.
    Provides methods for processing and inserting DBF records into the database.
    """
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the InsertionProcess with database configuration.
        
        Args:
            db_config: Optional database configuration dictionary. If not provided, config from PostgresConnection will be used.
        """
        # Use the provided config or get it from PostgresConnection
        self.db_config = db_config or PostgresConnection.get_db_config()
        
        self.tracker = PostgresTracking(self.db_config)
    
    def process_dbf_records(self, dbf_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process DBF records, validating and transforming them for insertion.
        
        Args:
            dbf_result: Dictionary containing DBF records data
            
        Returns:
            Dictionary with processed batch data and metadata
        """
        lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_data = []
        valid_records = 0

        print(f"Processing {len(dbf_result['data'])} records...")
        
        for i, record in enumerate(dbf_result['data'], 1):
            # Validate required fields (using exact case from DBF)
            folio = record.get('Folio')  # Uppercase as in DBF
            fecha = record.get('fecha')  # Lowercase as in DBF
            
            if not folio or not fecha:
                logging.warning(f"Invalid record - Folio: {folio}, Fecha: {fecha}. Full data: {record}")
                continue
                
            try:
                # Process date (format: 'mm/dd/yyyy HH:MM:SS a. m./p. m.')
                fecha_str = fecha.split()[0]  # Take only the date part
                fecha_emision = datetime.strptime(fecha_str, '%m/%d/%Y').date()
                
                # Validate hash
                md5_hash = record.get('md5_hash')
                if not md5_hash or len(md5_hash) != 32:
                    md5_hash = hashlib.md5(str(record).encode()).hexdigest()
                    logging.warning(f"Invalid hash for folio {folio}. Generated new hash")
                
                batch_data.append({
                    'folio': folio,
                    'total_partidas': len(record.get('detalles', [])) if record.get('detalles') is not None else 0,
                    'descripcion': f"empleado : {record.get('empleado')}",
                    'hash': md5_hash,
                    'fecha_emision': fecha_emision
                })
                valid_records += 1
                
            except ValueError as e:
                logging.warning(f"Error processing record {folio}: {str(e)}. Date: {fecha}")
                continue
        
        if not valid_records:
            logging.error("No valid records to insert")
            return {
                'success': False,
                'batch_data': [],
                'lote_id': lote_id,
                'valid_records': 0
            }
        
        # Get reference date from first valid record
        fecha_referencia = batch_data[0]['fecha_emision']
        
        return {
            'success': True,
            'batch_data': batch_data,
            'lote_id': lote_id,
            'batch_hash': dbf_result.get('dataset_hash'),
            'fecha_referencia': fecha_referencia,
            'valid_records': valid_records
        }
    
    def insert_batch(self, dbf_result: Dict[str, Any]) -> bool:
        """
        Process and insert a batch of DBF records using an atomic transaction.
        
        Args:
            dbf_result: Dictionary containing DBF records data
            
        Returns:
            Boolean indicating success or failure
        """
        # Process the records
        processed_data = self.process_dbf_records(dbf_result)
        
        if not processed_data['success'] or not processed_data['batch_data']:
            return False
        
        # Execute the full batch transaction
        success = self.tracker.insert_full_batch_transaction(
            batch_data=processed_data['batch_data'],
            lote_id=processed_data['lote_id'],
            batch_hash=processed_data['batch_hash'],
            fecha_referencia=processed_data['fecha_referencia']
        )
        
        if success:
            print(f"Process completed. Inserted {len(processed_data['batch_data'])} records in batch {processed_data['lote_id']}")
        else:
            print("Error in insertion process")
        
        return success
