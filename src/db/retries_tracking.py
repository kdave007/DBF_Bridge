import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz

class RetriesTracking:
    """Sistema de seguimiento para detalles de facturas"""
    
    def __init__(self, db_config: dict):
        self.config = db_config

    def insert_or_update_fac(self, folio: int, completado: bool = False, fecha_registro: date = None) -> bool:
        """Insert a new record or update an existing one in the retries_tracking table
        
        Args:
            folio: The invoice folio (primary key)
            intentos: Number of retry attempts
            completado: Whether the process was completed successfully
            
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
                    # Insert or update if exists
                    query = sql.SQL("""
                        INSERT INTO reintentos_fac_venta (
                            folio, intentos, completado, fecha_del_registro
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (folio) DO UPDATE SET
                            intentos = reintentos_fac_venta.intentos + 1,
                            completado = EXCLUDED.completado,
                            fecha_del_registro = EXCLUDED.fecha_del_registro
                        RETURNING folio
                    """)
                    
                    # Always start with intentos = 1 for new records
                    # Use current date if fecha_registro is not provided
                    if fecha_registro is None:
                        fecha_registro = date.today()
                    
                    params = (folio, 1, completado, fecha_registro)
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    
                    if result:
                        print(f"Successfully inserted/updated retry tracking for folio {folio}")
                        return True
                    else:
                        print(f"Failed to insert/update retry tracking for folio {folio}")
                        return False
                        
        except Exception as e:
            logging.error(f"Error in retry tracking for folio {folio}: {e}")
            return False

    def get_ignore_list(self, start_date, end_date):
        """Get a list of folios to ignore based on date range and retry criteria
        
        Args:
            start_date: The start date for the range (inclusive)
            end_date: The end date for the range (inclusive)
            
        Returns:
            list: List of folios that meet the criteria
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
                    # Select folios within date range
                    query = sql.SQL("""
                        SELECT folio 
                        FROM reintentos_fac_venta
                        WHERE fecha_del_registro BETWEEN %s AND %s
                        AND intentos >= 3
                        AND completado = false
                        ORDER BY folio
                    """)
                    
                    params = (start_date, end_date)
                    cursor.execute(query, params)
                    
                    # Fetch all results and extract folios
                    results = cursor.fetchall()
                    folios = [row[0] for row in results]
                    
                    print(f"Found {len(folios)} folios to ignore in date range {start_date} to {end_date}")
                    return folios
                        
        except Exception as e:
            logging.error(f"Error getting ignore list: {e}")
            return []

    def completed(self, folio):
        """Update a record to mark it as completed
        
        Args:
            folio: The folio of the record to update
            
        Returns:
            bool: True if the update was successful, False otherwise
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
                    # Update completado to True for the specified folio
                    query = sql.SQL("""
                        UPDATE reintentos_fac_venta
                        SET completado = true
                        WHERE folio = %s
                        RETURNING folio
                    """)
                    
                    cursor.execute(query, (folio,))
                    result = cursor.fetchone()
                    conn.commit()
                    
                    # Even if no record was found, we consider this a success
                    # since we're calling this method after successful processing
                    if result:
                        print(f"Successfully marked folio {folio} as completed")
                    else:
                        print(f"No retry record found for folio {folio} - this is normal if it was processed on first try")
                    
                    return True
                        
        except Exception as e:
            logging.error(f"Error marking folio {folio} as completed: {e}")
            return False