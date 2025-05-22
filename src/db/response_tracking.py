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
                        descripcion: str,
                        hash: str,
                        id_lote: str,
                        estado: str,
                        fecha_emision: date) -> bool:
        """Actualiza o inserta estado de factura"""
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cursor:
                    # Insert o update si existe
                    query = sql.SQL("""
                        INSERT INTO estado_factura_venta (
                            folio, total_partidas, descripcion,
                            hash, fecha_procesamiento, id_lote, estado, fecha_emision
                        ) VALUES (%s, %s, %s, %s, %s::date, %s, %s, %s)
                        ON CONFLICT (folio) DO UPDATE SET
                            estado = EXCLUDED.estado,
                            fecha_procesamiento = %s::date
                        RETURNING id
                    """)
                    
                    params = (folio, total_partidas, descripcion, hash, datetime.now().date(), id_lote, estado, fecha_emision, datetime.now().date())
                    cursor.execute(query, params)
                    
                    # Si se insertó, retornará el id
                    if cursor.fetchone():
                        conn.commit()
                        return True
                    return False
        except Exception as e:
            logging.error(f"Error insertando estado: {e}")
            return False