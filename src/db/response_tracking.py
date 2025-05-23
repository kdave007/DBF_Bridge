import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz

class ResponseTracking:
    def __init__(self, db_config: dict):
        self.config = db_config 

    def update_status(self, 
                        folio: str, 
                        total_partidas: int,
                        hash: str,
                        estado: str,
                        accion: str,
                        fecha_emision: date) -> bool:
        """Actualiza o inserta estado de factura"""
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cursor:
                    # Insert o update si existe
                    query = sql.SQL("""
                        INSERT INTO estado_factura_venta (
                            folio, total_partidas, hash,
                            fecha_procesamiento, estado, fecha_emision, accion
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (folio) DO UPDATE SET
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
                       # print(f"Operation successful for folio {folio} - hash: {hash}")
                        conn.commit()
                        return True
                    
                    print(f"Operation failed for folio {folio}")
                    return False
        except Exception as e:
            logging.error(f"Error insertando estado: {e}")
            return False