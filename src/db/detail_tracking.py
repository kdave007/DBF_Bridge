import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz

class DetailTracking:
    """Sistema de seguimiento para detalles de facturas"""
    
    def __init__(self, db_config: dict):
        self.config = db_config
    
    def insert_or_update_detail(self, 
                               folio: str, 
                               hash_detalle: str,
                               fecha: date,
                               estado: str = 'pendiente',
                               accion: str = 'create') -> bool:
        """
        Inserta un nuevo registro de detalle o actualiza uno existente
        
        Args:
            folio: Número de folio
            hash_detalle: Hash MD5 del detalle
            fecha: Fecha del detalle
            estado: Estado del detalle (pendiente, procesado, error)
            accion: Tipo de operación (create, update, delete)
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        INSERT INTO detalle_estado (
                            folio, hash_detalle, fecha, estado, accion
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (folio, hash_detalle) 
                        DO UPDATE SET 
                            estado = EXCLUDED.estado,
                            accion = EXCLUDED.accion,
                            hash_detalle = EXCLUDED.hash_detalle
                        RETURNING id
                    """)
                    
                    params = (folio, hash_detalle, fecha, estado, accion)
                    cursor.execute(query, params)
                    
                    result = cursor.fetchone()
                    conn.commit()
                    return result is not None
                    
        except Exception as e:
            logging.error(f"Error al insertar/actualizar detalle: {e}")
            return False
    
    def get_details_by_date_range(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Obtiene todos los detalles en un rango de fechas
        
        Args:
            start_date: Fecha inicial del rango
            end_date: Fecha final del rango
            
        Returns:
            Lista de diccionarios con los detalles encontrados
        """
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        SELECT id, folio, hash_detalle, fecha, estado, accion
                        FROM detalle_estado
                        WHERE fecha BETWEEN %s AND %s
                        ORDER BY fecha DESC, folio ASC
                    """)
                    
                    cursor.execute(query, (start_date, end_date))
                    
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                    
        except Exception as e:
            logging.error(f"Error al obtener detalles por rango de fechas: {e}")
            return []
    
    def batch_insert_details(self, details: List[Dict]) -> bool:
        """
        Inserta múltiples detalles en una sola transacción
        
        Args:
            details: Lista de diccionarios con los detalles a insertar
                Cada diccionario debe contener: folio, hash_detalle, fecha, estado, accion
                
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        if not details:
            return True  # Nothing to insert
            
        try:
            with psycopg2.connect(**self.config) as conn:
                # First, get existing folios to determine starting counters
                folio_counters = {}
                
                try:
                    with conn.cursor() as cursor:
                        # Query to get max index for each folio
                        count_query = """
                            SELECT folio, MAX(CAST(SPLIT_PART(id, '-', 2) AS INTEGER)) as max_index
                            FROM detalle_estado
                            GROUP BY folio
                        """
                        cursor.execute(count_query)
                        
                        # Initialize counters based on existing data
                        for row in cursor.fetchall():
                            folio, max_index = row
                            folio_counters[folio] = max_index
                except Exception as e:
                    logging.warning(f"Could not retrieve existing counters: {e}")
                
                # Continue with inserts
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        INSERT INTO detalle_estado (
                            id, folio, hash_detalle, fecha, estado, accion
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            estado = EXCLUDED.estado,
                            accion = EXCLUDED.accion,
                            hash_detalle = EXCLUDED.hash_detalle
                    """)
                    
                    # Track successful inserts
                    success_count = 0
                    
                    for detail in details:
                        folio = detail.get('folio')
                        
                        # Initialize counter for this folio if not exists
                        if folio not in folio_counters:
                            folio_counters[folio] = 0
                        
                        # Increment counter for this folio
                        folio_counters[folio] += 1
                        
                        # Create composite ID from folio and index
                        composite_id = f"{folio}-{folio_counters[folio]}"
                        
                        # Get current date if fecha is not provided
                        fecha = detail.get('fecha')
                        if not fecha:
                            fecha = date.today()
                        
                        params = (
                            composite_id,
                            folio,
                            detail.get('detail_hash') or detail.get('hash_detalle'),
                            fecha,
                            detail.get('estado', 'pendiente'),
                            detail.get('operation') or detail.get('accion', 'create')
                        )
                        
                        try:
                            cursor.execute(query, params)
                            success_count += 1
                        except Exception as e:
                            # Log the error but continue with other records
                            logging.error(f"Error inserting record {composite_id}: {e}")
                            conn.rollback()
                            continue
                    
                    conn.commit()
                    return success_count > 0
                    
        except Exception as e:
            logging.error(f"Error al insertar detalles en lote: {e}")
            return False