from typing import Dict, Any, Optional, List, Iterator
from src.config.db_config import PostgresConnection
from datetime import datetime
import psycopg2
import logging

class BaseModel:
    def __init__(self):
        self.db = PostgresConnection()
        self.table_name = ""  # Will be set by child classes
        self.chunk_size = 100  # Default chunk size for processing

    class DatabaseError(Exception):
        """Custom error for database operations"""
        pass

    def _chunk_records(self, records: List[Dict[str, Any]]) -> Iterator[List[Dict[str, Any]]]:
        """Split records into chunks of specified size"""
        for i in range(0, len(records), self.chunk_size):
            yield records[i:i + self.chunk_size]

    def insert_records(self, records: List[Dict[str, Any]], connection: Any = None) -> None:
        """Insert multiple records into the database"""
        if not records:
            return
        
        try:
            # Get field names from first record
            fields = list(records[0].keys())
            placeholders = [f'%({field})s' for field in fields]
            
            # Build the query
            query = f"""
            INSERT INTO {self.table_name} ({', '.join(fields)})
            VALUES ({', '.join(placeholders)})
            """
            
            # Execute batch insert with optional transaction
            self.db.execute_batch_update(query, records, connection)
            
        except Exception as e:
            error_msg = f"Failed to insert records into {self.table_name}"
            if isinstance(e, psycopg2.Error):
                error_msg += f": {e.pgerror if e.pgerror else str(e)} (Code: {e.pgcode})"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e

    def update_batch_status(self, record_ids: List[str], status: str, 
                          api_response: Dict[str, Any], 
                          error_message: Optional[str] = None) -> List[Dict[str, Any]]:
        """Update status for multiple records in chunks"""
        if not record_ids:
            return []
            
        try:
            query = f"""
            UPDATE {self.table_name}
            SET 
                status = %(status)s,
                error_message = %(error_message)s,
                api_response = %(api_response)s,
                updated_at = NOW(),
                retry_count = CASE 
                    WHEN status = 'failed' THEN retry_count + 1
                    ELSE retry_count
                END
            WHERE record_id = ANY(%(record_ids)s)
            RETURNING record_id, status
            """
            
            results = []
            # Process record_ids in chunks
            for chunk_ids in self._chunk_records(record_ids):
                params = {
                    'status': status,
                    'error_message': error_message,
                    'api_response': api_response,
                    'record_ids': chunk_ids
                }
                
                chunk_result = self.db.execute_query(query, params)
                if not chunk_result:
                    logging.warning(f"No records updated in chunk for {self.table_name}")
                results.append(chunk_result)
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to update batch status in {self.table_name}"
            if isinstance(e, psycopg2.Error):
                error_msg += f": {e.pgerror if e.pgerror else str(e)} (Code: {e.pgcode})"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e

    def get_pending_records(self, limit: int = 300) -> List[Dict[str, Any]]:
        """Get pending records up to the specified limit"""
        try:
            query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE status = 'pending'
            AND (retry_count < 3 OR retry_count IS NULL)
            ORDER BY created_at ASC
            LIMIT %(limit)s
            """
            
            result = self.db.execute_query(query, {'limit': limit})
            if not result:
                logging.info(f"No pending records found in {self.table_name}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get pending records from {self.table_name}"
            if isinstance(e, psycopg2.Error):
                error_msg += f": {e.pgerror if e.pgerror else str(e)} (Code: {e.pgcode})"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e

    def get_failed_records(self, min_retries: int = 3) -> List[Dict[str, Any]]:
        """Get records that have failed multiple times"""
        try:
            query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE status = 'failed'
            AND retry_count >= %(min_retries)s
            ORDER BY updated_at DESC
            """
            
            result = self.db.execute_query(query, {'min_retries': min_retries})
            if not result:
                logging.info(f"No failed records found in {self.table_name} with {min_retries}+ retries")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get failed records from {self.table_name}"
            if isinstance(e, psycopg2.Error):
                error_msg += f": {e.pgerror if e.pgerror else str(e)} (Code: {e.pgcode})"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e