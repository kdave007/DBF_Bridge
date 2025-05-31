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
                               id,
                               folio: str, 
                               hash_detalle: str,
                               fecha: date,
                               estado,
                               accion,
                               ref: str = '') -> bool:
        """
        Inserta un nuevo registro de detalle o actualiza uno existente
        
        Args:
            folio: Número de folio
            hash_detalle: Hash MD5 del detalle
            fecha: Fecha del detalle
            estado: Estado del detalle (pendiente, procesado, error)
            accion: Tipo de operación (create, update, delete)
            ref: Referencia del detalle
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
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
                    query = sql.SQL("""
                        INSERT INTO detalle_estado (
                            id,folio, hash_detalle, fecha, estado, accion, ref
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) 
                        DO UPDATE SET 
                            estado = EXCLUDED.estado,
                            accion = EXCLUDED.accion,
                            hash_detalle = EXCLUDED.hash_detalle,
                            ref = EXCLUDED.ref
                        RETURNING id
                    """)
                    
                    params = (folio, hash_detalle, fecha, estado, accion, ref)
                    
                    cursor.execute(query, params)
                    
                    result = cursor.fetchone()
                    conn.commit()
                    return result is not None
                    
        except Exception as e:
            logging.error(f"Error al insertar/actualizar detalle: {e}")
            return False
    
    def get_details_by_folio(self, folio: str) -> List[Dict]:
        """
        Obtiene todos los detalles asociados a un folio específico
        
        Args:
            folio: Número de folio a consultar
            
        Returns:
            Lista de diccionarios con los detalles encontrados
        """
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
                    query = sql.SQL("""
                        SELECT id, folio, hash_detalle, fecha, estado, accion, ref
                        FROM detalle_estado
                        WHERE folio = %s
                        ORDER BY id ASC
                    """)
                    
                    cursor.execute(query, (folio,))
                    
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                    
        except Exception as e:
            logging.error(f"Error al obtener detalles por folio: {e}")
            return []
    
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
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        SELECT id, folio, hash_detalle, fecha, estado, accion, ref
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
    
    def batch_replace_by_id(self, details: List[Dict]) -> bool:
        """
        Procesa múltiples detalles en una sola transacción, utilizando el ID como referencia
        principal en lugar del folio.
        
        Args:
            details: Lista de diccionarios con los detalles a insertar
                Cada diccionario debe contener: id, folio, hash_detalle, fecha, estado, accion
                
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        if not details:
            return True  # Nothing to process
            
        try:
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                # Group details by ID
                print(f'DETAILS {details}')

                details_by_id = {}
                for detail in details:
                    print(f'batch_replace_by_id {detail}')
                    detail_id = detail.get('detail_id') or  detail.get('sql_id')#here goes the id not parent id
                    if detail_id:
                        if detail_id not in details_by_id:
                            details_by_id[detail_id] = []
                        details_by_id[detail_id].append(detail)
                
                # Track successful operations
                deleted_count = 0
                inserted_count = 0
                
                # Process each ID in a separate transaction
                for detail_id, id_details in details_by_id.items():
                    try:
                        # First delete all existing records for this ID
                        with conn.cursor() as cursor:
                            delete_query = "DELETE FROM detalle_estado WHERE id = %s"
                            cursor.execute(delete_query, (detail_id,))
                            deleted_count += cursor.rowcount
                            print(f"Deleted {cursor.rowcount} existing records for ID {detail_id}")
                            
                        # Then insert all new records for this ID
                        with conn.cursor() as cursor:
                            # Insert query
                            insert_query = """
                                INSERT INTO detalle_estado (
                                    id, folio, hash_detalle, fecha, estado, accion, ref
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            # Insert each detail (should be just one per ID)
                            for detail in id_details:
                                # Get the folio from the detail
                                folio = detail.get('folio', '')
                                
                                # Get current date if fecha is not provided
                                fecha = detail.get('fecha')
                                if not fecha:
                                    fecha = date.today()
                                
                                # Get the REF value
                                ref_value = ''
                                if 'REF' in detail:
                                    ref_value = detail['REF']
                                elif 'ref' in detail:
                                    ref_value = detail['ref']
                                
                                # Extract values
                                detail_hash = detail.get('hash_detail') or detail.get('hash_detalle') or detail.get('detail_hash')
                                estado = 'pa_completado'
                                operation = detail.get('accion', 'creado')
                                
                                params = (
                                    detail_id,  # Use the actual ID from the API
                                    folio,
                                    detail_hash,
                                    fecha,
                                    estado,
                                    operation,
                                    ref_value
                                )
                                
                                # Debug print
                                print(f'REPLACE: ID={detail_id}, FOLIO={folio}, HASH={detail_hash}, '
                                      f'FECHA={fecha}, ESTADO={estado}, ACCION={operation}, REF={ref_value}')
                                
                                cursor.execute(insert_query, params)
                                inserted_count += 1
                        
                        # Commit the transaction for this ID
                        conn.commit()
                        print(f"Successfully processed ID {detail_id}: deleted {deleted_count}, inserted {inserted_count}")
                        
                    except Exception as e:
                        # If anything goes wrong, rollback this ID's transaction
                        conn.rollback()
                        logging.error(f"Error processing ID {detail_id}: {e}")
                        # Continue with the next ID
                        
                return inserted_count > 0
                
        except Exception as e:
            logging.error(f"Error in batch_replace_by_id: {e}")
            return False
    
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
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
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
                            id, folio, hash_detalle, fecha, estado, accion, ref
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (folio, ref) DO UPDATE SET
                            estado = EXCLUDED.estado,
                            accion = EXCLUDED.accion,
                            hash_detalle = EXCLUDED.hash_detalle,
                            ref = EXCLUDED.ref
                    """)
                    
                    # Track successful inserts
                    success_count = 0
                    
                    for detail in details:
                        print(f' $$$$ inserting record detail : {detail}')
                        folio = detail.get('folio')
                        print(f"checkpoint______________________________")
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
                        
                        # Get the REF value - check both 'REF' and 'ref' keys to handle case sensitivity
                        ref_value = ''
                        if 'REF' in detail:
                            ref_value = detail['REF']
                        elif 'ref' in detail:
                            ref_value = detail['ref']
                        
                        # Extract values for better debugging
                        detail_hash = detail.get('detail_hash') or detail.get('hash_detalle')
                        estado = detail.get('estado', 'pendiente')
                        operation = detail.get('operation') or detail.get('accion', 'create')
                        
                        params = (
                            composite_id,
                            folio,
                            detail_hash,
                            fecha,
                            estado,
                            operation,
                            ref_value
                        )
                        
                        # Detailed debug print to identify null values
                        print(f'DEBUG INSERT: ID={composite_id}, FOLIO={folio}, HASH={detail_hash}, '
                              f'FECHA={fecha}, ESTADO={estado}, ACCION={operation}, REF={ref_value}')
                        print(f'ORIGINAL DETAIL: {detail}')
                        
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


    def delete_by_folio(self, folio) -> bool:
        """
        Elimina todos los registros asociados a un folio específico
        
        Args:
            folio: Número de folio a eliminar
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
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
                    query = sql.SQL("""
                        DELETE FROM detalle_estado
                        WHERE folio = %s
                    """)
                    
                    cursor.execute(query, (folio,))
                    
                    # Get number of rows affected
                    rows_deleted = cursor.rowcount
                    conn.commit()
                    
                    return rows_deleted > 0
                    
        except Exception as e:
            logging.error(f"Error al eliminar registros por folio: {e}")
            return False

    def delete_by_id(self, id) -> bool:
        """
        Elimina un registro específico por su ID
        
        Args:
            id: Identificador único del registro a eliminar
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
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
                    query = sql.SQL("""
                        DELETE FROM detalle_estado
                        WHERE id = %s
                    """)
                    
                    cursor.execute(query, (id,))
                    
                    # Get number of rows affected
                    rows_deleted = cursor.rowcount
                    conn.commit()
                    
                    return rows_deleted > 0
                    
        except Exception as e:
            logging.error(f"Error al eliminar registro por ID: {e}")
            return False