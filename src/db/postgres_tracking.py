import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz

class PostgresTracking:
    """Sistema de seguimiento para estado_factura_venta"""
    
    def __init__(self, db_config: dict):
        self.config = db_config
    
    def get_by_lote(self, id_lote: str = None, limit: int = 100) -> List[Dict]:
        """Obtiene estados de facturas"""
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
                    base_query = sql.SQL("""
                        SELECT id, folio, total_partidas, descripcion, 
                               hash, fecha_procesamiento, id_lote, estado, fecha_emision, accion
                        FROM estado_factura_venta
                    """)
                    
                    if id_lote:
                        query = base_query + sql.SQL(" WHERE id_lote = %s ORDER BY fecha_procesamiento DESC")
                        cursor.execute(query, (id_lote,))
                    else:
                        query = base_query + sql.SQL(" ORDER BY fecha_procesamiento DESC LIMIT %s")
                        cursor.execute(query, (limit,))
                    
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error obteniendo estados: {e}")
            return []
    
    def update_invoice_status(self, 
                            folio: str, 
                            total_partidas: int,
                            descripcion: str,
                            hash: str,
                            id_lote: str,
                            estado: str = 'pendiente',
                            fecha_emision: date = None) -> bool:
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
                    # Solo insert si no existe
                    query = sql.SQL("""
                        INSERT INTO estado_factura_venta (
                            folio, total_partidas, descripcion,
                            hash, fecha_procesamiento, id_lote, estado, fecha_emision
                        ) VALUES (%s, %s, %s, %s, %s::date, %s, %s, %s)
                        ON CONFLICT (folio) DO NOTHING
                        RETURNING id
                    """)
                    
                    params = (folio, total_partidas, descripcion, hash, datetime.now().date(), id_lote, estado, fecha_emision)
                    cursor.execute(query, params)
                    
                    # Si se insertó, retornará el id
                    if cursor.fetchone():
                        conn.commit()
                        return True
                    return False
        except Exception as e:
            logging.error(f"Error insertando estado: {e}")
            return False
            
    def update_existing_invoice(self,
                              folio: str,
                              new_status: str,
                              new_hash: str = None) -> bool:
        """Actualiza solo estado y hash de factura existente"""
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
                    if new_hash:
                        query = sql.SQL("""
                            UPDATE estado_factura_venta
                            SET estado = %s,
                                hash = %s,
                                fecha_procesamiento = %s
                            WHERE folio = %s
                        """)
                        cursor.execute(query, (new_status, new_hash, datetime.now(pytz.utc), folio))
                    else:
                        query = sql.SQL("""
                            UPDATE estado_factura_venta
                            SET estado = %s,
                                fecha_procesamiento = %s
                            WHERE folio = %s
                        """)
                        cursor.execute(query, (new_status, datetime.now(pytz.utc), folio))
                    
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error actualizando estado: {e}")
            return False

    def get_records_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Obtiene todos los registros en el rango de fechas
        
        Args:
            start_date: Fecha inicial
            end_date: Fecha final
            
        Returns:
            Lista de registros completos en el rango
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
                    query = """
                        SELECT id, folio, total_partidas,
                               hash, fecha_procesamiento,estado, fecha_emision
                        FROM estado_factura_venta
                        WHERE fecha_emision BETWEEN %s AND %s
                        ORDER BY fecha_emision
                    """
                    
                    # Format dates for better debugging output
                    print(f"\nExecuting SQL query with dates: {start_date} to {end_date}")
                    
                    # Convert datetime objects to date objects if needed
                    # This ensures we're only comparing the date part in the query
                    start_date_param = start_date.date() if hasattr(start_date, 'date') else start_date
                    end_date_param = end_date.date() if hasattr(end_date, 'date') else end_date
                    
                    cursor.execute(query, (start_date_param, end_date_param))
                    
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                        
                        print(f"Query found {len(results)} records")
                        
                        # Print sample of results (first 2)
                        if results:
                            print("\nSample records:")
                            for i, record in enumerate(results[:2], 1):
                                print(f"\nRecord #{i}:")
                                print(f"Folio: {record.get('folio')}")
                                print(f"Hash: {record.get('hash')}")
                                print(f"Fecha: {record.get('fecha_emision')}")
                        else:
                            print("No records found in date range")
                            
                        return results
                    return []
        except Exception as e:
            logging.error(f"Error obteniendo registros: {e}")
            return []

    def insert_batch_record(self, lote_id: str, hash_lote: str, fecha_referencia: date) -> bool:
        """
        Inserta un único registro en tabla lote_diario que representa todo el batch
        
        Args:
            lote_id: ID único del lote
            hash_lote: Hash MD5 del dataset completo
            fecha_referencia: Fecha de referencia del batch (no puede ser null)
        """
        if not fecha_referencia:
            fecha_referencia = datetime.now().date()
            logging.warning("Fecha referencia no proporcionada, usando fecha actual")
            
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
                        INSERT INTO lote_diario (
                            lote, fecha_insercion, 
                            fecha_referencia, hash_lote
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (lote) DO NOTHING
                    """)
                    cursor.execute(query, (
                        lote_id,
                        datetime.now(pytz.utc),
                        fecha_referencia,
                        hash_lote
                    ))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error insertando registro de lote: {e}")
            return False

    def insert_full_batch_transaction(self, 
                                   batch_data: List[Dict], 
                                   lote_id: str, 
                                   batch_hash: str,
                                   fecha_referencia: date) -> bool:
        """
        Inserta en una sola transacción:
        1. Registro en tabla lote_diario
        2. Todos los registros en estado_factura_venta
        
        Args:
            batch_data: Lista de diccionarios con datos de facturas
            lote_id: ID del lote
            batch_hash: Hash del dataset completo
            fecha_referencia: Fecha de referencia
        """
        # Obtener fecha referencia válida
        if not fecha_referencia:
            fecha_referencia = datetime.now().date()
        
        try:
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                try:
                    cursor = conn.cursor()
                    
                    # 1. Insertar registro en tabla lotes
                    lote_query = """
                        INSERT INTO lote_diario (
                            lote, fecha_insercion,
                            fecha_referencia, hash_lote
                        ) VALUES (%s, %s, %s, %s)
                    """
                    lote_params = (
                        lote_id,
                        datetime.now(pytz.utc),
                        fecha_referencia,
                        batch_hash
                    )
                    logging.debug(f"Query lote:\n{lote_query}\nParams: {lote_params}")
                    cursor.execute(lote_query, lote_params)
                    
                    # 2. Insertar todas las facturas
                    factura_query = """
                        INSERT INTO estado_factura_venta (
                            folio, total_partidas, descripcion,
                            hash, fecha_procesamiento, id_lote, estado, fecha_emision
                        ) VALUES (%s, %s, %s, %s, %s::date, %s, %s, %s)
                    """
                    for record in batch_data:
                        # Validar campos requeridos
                        if not record.get('folio'):
                            logging.error("Intento de insertar registro sin folio")
                            continue
                        
                        if not record.get('fecha_emision'):
                            logging.warning(f"Folio {record['folio']} sin fecha, usando fecha actual")
                            record['fecha_emision'] = datetime.now().date()
                        
                        factura_params = (
                            record['folio'],
                            record['total_partidas'],
                            record['descripcion'],
                            record['hash'],
                            datetime.now().date(),
                            lote_id,
                            'pendiente',
                            record['fecha_emision']
                        )
                        logging.debug(f"Query factura:\n{factura_query}\nParams: {factura_params}")
                        cursor.execute(factura_query, factura_params)
                    
                    conn.commit()
                    return True
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error en transacción: {e}")
                    return False
        except Exception as e:
            logging.error(f"Error de conexión: {e}")
            return False

    def _ensure_indexes(self):
        """Create required indexes if missing"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_lote_diario_fecha_ref 
                    ON lote_diario (fecha_referencia);
                """)
                self.conn.commit()
        except Exception as e:
            self.logger.error(f"Index creation error: {str(e)}")
            self.conn.rollback()

    def get_lotes_by_fecha_referencia(self, fecha: str) -> List[Dict]:
        """
        Retrieve lotes by reference date
        Returns: List of dictionaries with lote data
        """
        query = """
            SELECT id_lote, rango_fechas, hash_lote, fecha_procesamiento
            FROM lote_diario
            WHERE fecha_referencia = %s
            ORDER BY fecha_procesamiento DESC;
        """
        
        try:
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (fecha,))
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching lotes by fecha: {str(e)}")
            return []
            
    def get_single_lote_by_date(self, start_date: date) -> Optional[Dict]:
        """
        Retrieve a single record from lote_diario by start date
        
        Args:
            start_date: The reference date to search for
            
        Returns:
            A dictionary with the lote data or None if not found
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
                    query = """
                        SELECT lote, fecha_insercion, fecha_referencia, hash_lote
                        FROM lote_diario
                        WHERE fecha_referencia = %s
                        ORDER BY fecha_insercion DESC
                        LIMIT 1
                    """
                    
                    cursor.execute(query, (start_date,))
                    row = cursor.fetchone()
                    
                    if row:
                        columns = [desc[0] for desc in cursor.description]
                        return dict(zip(columns, row))
                    return None
        except Exception as e:
            logging.error(f"Error retrieving single lote by date: {e}")
            return None
