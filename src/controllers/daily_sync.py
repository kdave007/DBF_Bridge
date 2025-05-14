import os
import sys
import time

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from datetime import datetime, timedelta
from src.controllers.ventas_controller import VentasController
from src.dbf_enc_reader.mapping_manager import MappingManager
from src.config.dbf_config import DBFConfig
from src.models.ventas_model import VentasModel
from src.models.ventas_detalle_model import VentasDetalleModel
from src.models.ventas_tracking_model import VentasTrackingModel
import logging  # For logging what happens
import json    # For handling JSON data
import os
from dotenv import load_dotenv
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),
        logging.StreamHandler()
    ]
)

class ProcessingError(Exception):
    """Custom error for processing failures"""
    pass

def handle_batch_error(error: Exception, record: dict, error_type: str) -> None:
    """Handle errors during batch processing"""
    folio = record.get('Folio', 'Unknown')
    logging.error(f"{error_type} error for folio {folio}: {str(error)}")
    if isinstance(error, psycopg2.Error):
        logging.error(f"Database error code: {error.pgcode}")
        if error.pgerror:
            logging.error(f"Database error details: {error.pgerror}")

def process_daily_data():
    try:
        # Initialize DBF configuration (it will load from .env automatically)
        config = DBFConfig()
        
        logging.info(f"Using DLL: {config.dll_path}")
        logging.info(f"Using DBF directory: {config.source_directory}")
        
        # Get project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        logging.info(f"Project root: {project_root}")
        
        # Load mappings from project root
        mappings_path = os.path.join(project_root, 'mappings.json')
        mapping_manager = MappingManager(mappings_path)
        ventas_controller = VentasController(mapping_manager, config)
        
        # Debug: Use specific dates
        end_date = datetime(2025, 5, 4)    # Year, Month, Day
        start_date = datetime(2025, 5, 4)   # Previous day
        
        logging.info(f"Getting data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # This will get all sales records in that date range
        records = ventas_controller.get_sales_in_range(start_date, end_date)
        logging.info(f"Found {len(records)} records")
        
        # Initialize models
        ventas_model = VentasModel()
        detalle_model = VentasDetalleModel()
        tracking_model = VentasTrackingModel()
        
        # Split into batches of 100
        batch_size = 100
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        # Process each batch
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = start_idx + batch_size
            batch = records[start_idx:end_idx]
            
            logging.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch)} records)")
            
            if batch:
                # Add retry logic for transient failures
                from tenacity import retry, stop_after_attempt
                
                @retry(stop=stop_after_attempt(3))
                def process_batch(batch, connection):
                    # Prepare and save header records
                    header_records = [ventas_model.prepare_record(record) for record in batch]
                    logging.info(f"Saving {len(header_records)} headers to PostgreSQL")
                    ventas_model.insert_records(header_records, connection)
                    
                    # Process each record's details and tracking
                    for record in batch:
                        # Save details if they exist
                        detail_records = detalle_model.prepare_batch(record)
                        if detail_records:
                            logging.info(f"Saving {len(detail_records)} details for folio {record['Folio']}")
                            detalle_model.insert_records(detail_records, connection)
                            
                            # Create tracking record
                            tracking_record = tracking_model.prepare_record(
                                header_record=record,
                                total_partidas=len(detail_records)
                            )
                            logging.info(f"Creating tracking record for folio {record['Folio']}")
                            tracking_model.insert_records([tracking_record], connection)
                
                try:
                    # Start transaction for this batch
                    connection = ventas_model.db.begin_transaction()
                    batch_start_time = time.time()
                    
                    try:
                        # Process batch with retry logic
                        process_batch(batch, connection)
                        
                        # If we get here, everything succeeded, commit the transaction
                        ventas_model.db.commit_transaction(connection)
                        
                        # Log performance metrics
                        batch_time = time.time() - batch_start_time
                        logging.info(f"Batch processed in {batch_time:.2f} seconds")
                        logging.info(f"Successfully processed batch of {len(batch)} records")
                        
                    except Exception as e:
                        # Something failed, rollback the entire batch
                        ventas_model.db.rollback_transaction(connection)
                        handle_batch_error(e, batch[0], "Batch Processing")
                        continue  # Skip to next batch
                        
                except Exception as e:
                    handle_batch_error(e, batch[0], "Transaction")
                    continue  # Skip to next batch
        
    except Exception as e:
        logging.error(f"Critical error in process_daily_data: {str(e)}")
        if isinstance(e, psycopg2.Error):
            logging.error(f"Database error code: {e.pgcode}")
            logging.error(f"Database error details: {e.pgerror if e.pgerror else 'No additional details'}")
        raise ProcessingError(f"Failed to process daily data: {str(e)}") from e

if __name__ == "__main__":
    process_daily_data()