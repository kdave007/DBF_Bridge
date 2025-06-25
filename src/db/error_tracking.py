import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz

class ErrorTracking:
    """Sistema de seguimiento para detalles de facturas"""
    
    def __init__(self, db_config: dict):
        self.config = db_config

    
    def insert(self, desc: str, class_name: str) -> bool:
        """Insert a new error record into the error_tracking table
        
        Args:
            desc: Description of the error
            class_name: Name of the class where the error occurred
            
        Returns:
            bool: True if the operation was successful, False otherwise
        """
        try:
            # Connect with explicit parameters
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                with conn.cursor() as cursor:
                    # Insert error record
                    query = sql.SQL("""
                        INSERT INTO errores (
                            fecha, descripcion, clase
                        ) VALUES (
                            CURRENT_TIMESTAMP, %s, %s
                        )
                        RETURNING id
                    """)
                    
                    params = (desc, class_name)
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    
                    if result:
                        error_id = result[0]
                        print(f"Successfully logged error with ID {error_id}")
                        return True
                    else:
                        print("Failed to log error")
                        return False
                        
        except Exception as e:
            logging.error(f"Error logging to error_tracking: {e}")
            return False
