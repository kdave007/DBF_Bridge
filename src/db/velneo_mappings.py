import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz




class VelneoMappings:
    """Seleccion de bases de datos"""
    
    def __init__(self, db_config: dict):
        self.config = db_config
    

    def get_cliente(self):
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM clientes 
            WHERE pvsi_clave = 'VTPUB'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving almacen Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_from_general_alm(self):
        """Get the Velneo ID for an almacen (warehouse) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'almacen'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving almacen Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_from_general_serie(self):
        """Get the Velneo ID for a serie from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'serie'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving serie Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_from_general_emp(self):
        """Get the Velneo ID for an empresa (company) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'empresa'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving empresa Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def get_from_general_div(self):
        """Get the Velneo ID for an division (company) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'division'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving empresa Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_metodo_pago(self, reference):
        """Get the Velneo ID for a payment method from metodo_pago table
        
        Args:
            reference: The reference to look for in the pvsi column
            
        Returns:
            int: The velneo value if found, None otherwise
        """
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM metodo_pago 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving payment method Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_vendedor(self, reference):
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            # Convert reference to string to match the character varying column
            str_reference = str(reference) if reference is not None else None
            
            query = """
            SELECT velneo FROM vendedores 
            WHERE pvsi_clave = %s
            LIMIT 1
            """
            
            cursor.execute(query, (str_reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving vendedor Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_pais(self, reference):
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT id FROM pais 
            WHERE description = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving payment method Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_tipo_mov(self, reference):
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM tipo_movimiento 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving payment method Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_articulo(self, reference):
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT velneo_id FROM articulos 
            WHERE pvsi_clave = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving payment method Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    
    def get_tipo_iva(self, reference):
        try:
            conn = psycopg2.connect(**self.config)
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM iva 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving payment method Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    