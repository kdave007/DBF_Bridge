import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz

class ResponseTracking:
    def __init__(self, db_config: dict):
        self.config = db_config 

    def delete_by_id(self, id) -> bool:
        """Delete a record from estado_factura_venta by ID"""
        try:
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                with conn.cursor() as cursor:
                    # Delete record by ID
                    query = sql.SQL("""
                        DELETE FROM estado_factura_venta
                        WHERE id = %s
                        RETURNING id
                    """)
                    
                    cursor.execute(query, (id,))
                    deleted_id = cursor.fetchone()
                    conn.commit()
                    
                    if deleted_id:
                        print(f"Successfully deleted record with ID {id}")
                        return True
                    else:
                        print(f"No record found with ID {id}")
                        return False
                        
        except Exception as e:
            print(f"Error deleting record with ID {id}: {e}")
            return False

    def update_status(self, 
                        id,
                        folio: str, 
                        total_partidas: int,
                        hash: str,
                        estado: str,
                        accion: str,
                        fecha_emision: date) -> bool:
        """Actualiza o inserta estado de factura"""
        try:
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                with conn.cursor() as cursor:
                    # Insert o update si existe
                    query = sql.SQL("""
                        INSERT INTO estado_factura_venta (
                            id,folio, total_partidas, hash,
                            fecha_procesamiento, estado, fecha_emision, accion
                        ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            estado = EXCLUDED.estado,
                            hash = EXCLUDED.hash,
                            accion = EXCLUDED.accion,
                            fecha_procesamiento = %s,
                            total_partidas = EXCLUDED.total_partidas,
                            fecha_emision = EXCLUDED.fecha_emision
                        RETURNING id
                    """)
                    
                    current_date = datetime.now().date()
                    params = (
                        id,
                        folio, 
                        total_partidas, 
                        hash, 
                        current_date, 
                        estado, 
                        fecha_emision, 
                        accion,
                        current_date  # For the update
                    )
                    #print(f"\nSQL Operation for folio: {folio}")
                    #print(f"Parameters: {params}")
                    
                    cursor.execute(query, params)
                    
                    # Si se insertó, retornará el id
                    result = cursor.fetchone()
                    #print(f"SQL Result: {result}")
                    
                    if result:
                        print(f"Operation successful for folio {folio} - hash: {hash}")
                        conn.commit()
                        return True
                    
                    print(f"Operation failed for folio {folio}")
                    return False
        except Exception as e:
            logging.error(f"Error insertando estado: {e}")
            return False
            
    def update_record_status(self, id: int, estado: str, accion: str) -> bool:
        """Update only the estado and accion fields for a record by its ID
        
        Args:
            id: The ID of the record to update
            estado: The new estado value
            accion: The new accion value
            
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
                    # Update only estado and accion fields
                    query = sql.SQL("""
                        UPDATE estado_factura_venta 
                        SET estado = %s, 
                            accion = %s,
                            fecha_procesamiento = %s
                        WHERE id = %s
                        RETURNING id
                    """)
                    
                    # Execute the query with current timestamp
                    current_date = datetime.now()
                    cursor.execute(query, (estado, accion, current_date, id))
                    updated_id = cursor.fetchone()
                    conn.commit()
                    
                    if updated_id:
                        print(f"Successfully updated status for record with ID {id}")
                        return True
                    else:
                        print(f"No record found with ID {id}")
                        return False
                        
        except Exception as e:
            logging.error(f"Error updating status for record with ID {id}: {e}")
            return False