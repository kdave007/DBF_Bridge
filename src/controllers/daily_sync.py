import os
import sys
import time
import random
from tenacity import retry, stop_after_attempt, RetryError

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
    
    # Handle RetryError specifically
    if isinstance(error, RetryError):
        inner_error = error.last_attempt.exception()
        logging.error(f"Retry failed after 3 attempts. Inner error: {str(inner_error)}")
        if isinstance(inner_error, psycopg2.Error):
            logging.error(f"Database error code: {inner_error.pgcode}")
            logging.error(f"Database error details: {inner_error.pgerror if inner_error.pgerror else 'No details'}")
            logging.error(f"Database diagnostic: {inner_error.diag.message_detail if inner_error.diag else 'No diagnostic'}")
    
    # Handle direct database errors
    elif isinstance(error, psycopg2.Error):
        logging.error(f"Database error code: {error.pgcode}")
        logging.error(f"Database error details: {error.pgerror if error.pgerror else 'No details'}")
        logging.error(f"Database diagnostic: {error.diag.message_detail if error.diag else 'No diagnostic'}")

class DailySyncProcessor:
    def __init__(self):
        self.config = DBFConfig()
        self.project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.ventas_model = VentasModel()
        self.detalle_model = VentasDetalleModel()
        self.tracking_model = VentasTrackingModel()
        self.batch_size = 100

    def initialize_controller(self):
        """Initialize the sales controller with mappings"""
        logging.info(f"Using DLL: {self.config.dll_path}")
        logging.info(f"Using DBF directory: {self.config.source_directory}")
        logging.info(f"Project root: {self.project_root}")
        
        mappings_path = os.path.join(self.project_root, 'mappings.json')
        mapping_manager = MappingManager(mappings_path)
        return VentasController(mapping_manager, self.config)

    def get_date_range(self) -> tuple[datetime, datetime]:
        """Get the date range for processing"""
        # Hardcoded dates for debugging
        start_date = datetime(2025, 5, 4)  # Year, Month, Day
        end_date = datetime(2025, 5, 4)    # Same day for testing
            
        logging.info(f"Getting data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return start_date, end_date

    def process_single_batch(self, batch: list, connection, batch_id: str) -> None:
        """Process a single batch of records
        
        Args:
            batch: List of records to process
            connection: Database connection
            batch_id: Unique identifier for this batch
        """
        # Prepare and save header records
        header_records = [self.ventas_model.prepare_record(record) for record in batch]
        logging.info(f"Saving {len(header_records)} headers to PostgreSQL")
        self.ventas_model.insert_records(header_records, connection)
        
        # Process each record's details and tracking
        for record in batch:
            # Save details if they exist
            detail_records = self.detalle_model.prepare_batch(record)
            if detail_records:
                logging.info(f"Saving {len(detail_records)} details for folio {record['Folio']}")
                self.detalle_model.insert_records(detail_records, connection)
                print("checkpoint ---------------------------------------------------")
                # Create tracking record with batch ID
                logging.info(f"Creating tracking record for folio {record['Folio']}")
                tracking_record = self.tracking_model.prepare_record(
                    header_record=record,
                    total_partidas=len(detail_records),
                    batch_id=batch_id
                )
                logging.info(f"Tracking record prepared: {tracking_record}")
                logging.info(f"About to insert tracking record for folio {record['Folio']}")
                self.tracking_model.insert_records([tracking_record], connection)

    @retry(stop=stop_after_attempt(3))
    def process_batch_with_retry(self, batch: list, connection, batch_id: str) -> None:
        """Process a batch with retry logic
        
        Args:
            batch: List of records to process
            connection: Database connection
            batch_id: Unique identifier for this batch
        """
        self.process_single_batch(batch, connection, batch_id)

    def process_batch(self, batch: list) -> None:
        """Process a batch of records within a transaction"""
        try:
            connection = self.ventas_model.db.begin_transaction()
            batch_start_time = time.time()
            
            # Generate unique batch ID
            batch_id = self.generate_batch_id()
            logging.info(f"Processing batch with ID: {batch_id}")
            
            try:
                self.process_batch_with_retry(batch, connection, batch_id)
                self.ventas_model.db.commit_transaction(connection)
                
                # Log performance metrics
                batch_time = time.time() - batch_start_time
                logging.info(f"Batch processed in {batch_time:.2f} seconds")
                logging.info(f"Successfully processed batch of {len(batch)} records")
                
            except Exception as e:
                self.ventas_model.db.rollback_transaction(connection)
                handle_batch_error(e, batch[0], "Batch Processing")
                
        except Exception as e:
            handle_batch_error(e, batch[0], "Transaction")

    def process_records(self, records: list) -> None:
        """Process all records in batches"""
        total_batches = (len(records) + self.batch_size - 1) // self.batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = start_idx + self.batch_size
            batch = records[start_idx:end_idx]
            
            if batch:
                logging.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch)} records)")
                self.process_batch(batch)

    @classmethod
    def process_daily_data(cls) -> None:
        """Main function to process daily sales data"""
        try:
            processor = cls()
            ventas_controller = processor.initialize_controller()
            
            start_date, end_date = processor.get_date_range()
            records = ventas_controller.get_sales_in_range(start_date, end_date)
            logging.info(f"Found {len(records)} records")
            
            processor.process_records(records)
            
        except Exception as e:
            logging.error(f"Critical error in process_daily_data: {str(e)}")
            if isinstance(e, psycopg2.Error):
                logging.error(f"Database error code: {e.pgcode}")
                if e.pgerror:
                    logging.error(f"Database error details: {e.pgerror}")
            raise ProcessingError(f"Failed to process daily data: {str(e)}") from e

    def generate_batch_id(self) -> str:
        """Generate a unique batch ID using timestamp and random number.
        Format: YYYYMMDD_HHMMSS_XXXXX where X is random digit"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        random_suffix = ''.join(str(random.randint(0, 9)) for _ in range(5))
        return f"{timestamp}_{random_suffix}"

if __name__ == "__main__":
    DailySyncProcessor.process_daily_data()