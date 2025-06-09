


from typing import Dict, Any, Optional
import logging
from src.db.velneo_mappings import VelneoMappings
from src.config.db_config import PostgresConnection

class DataMap:
    """Class for mapping DBF data to API format using database lookups
    
    This class handles the process of converting references from DBF files
    to the appropriate IDs needed for the API by querying the database.
    """

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """Initialize the DataMap with database configuration
        
        Args:
            db_config: Optional database configuration dictionary. If not provided, 
                       config from PostgresConnection will be used.
        """
        self.db_config = db_config or PostgresConnection.get_db_config()
        self.velneo_mappings = VelneoMappings(self.db_config)
    
    def apply_map_serie(self) -> Optional[int]:
        """Get the Velneo ID for serie from the database
        
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        try:
            return self.velneo_mappings.get_from_general_serie()
        except Exception as e:
            logging.error(f"Error mapping serie: {e}")
            return None
    
    def apply_map_cliente(self) -> Optional[int]:
        """Get the Velneo ID for cliente from the database
        
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        try:
            return self.velneo_mappings.get_cliente()
        except Exception as e:
            logging.error(f"Error mapping cliente: {e}")
            return None

    def apply_map_metodo_pago(self, ref: str) -> Optional[int]:
        """Get the Velneo ID for metodo_pago from the database
        
        Args:
            ref: The reference code from the DBF record
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        if not ref:
            return None
            
        try:
            return self.velneo_mappings.get_metodo_pago(ref)
        except Exception as e:
            logging.error(f"Error mapping metodo_pago with ref {ref}: {e}")
            return None

    def apply_map_vendedor(self, ref: str) -> Optional[int]:
        """Get the Velneo ID for vendedor from the database
        
        Args:
            ref: The reference code from the DBF record
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        if not ref:
            return None
            
        try:
            return self.velneo_mappings.get_vendedor(ref)
        except Exception as e:
            logging.error(f"Error mapping vendedor with ref {ref}: {e}")
            return None

    def apply_map_pais(self, ref: str) -> Optional[int]:
        """Get the Velneo ID for pais from the database
        
        Args:
            ref: The reference code from the DBF record
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        if not ref:
            return None
            
        try:
            return self.velneo_mappings.get_pais(ref)
        except Exception as e:
            logging.error(f"Error mapping pais with ref {ref}: {e}")
            return None

    def apply_map_alm(self) -> Optional[int]:
        """Get the Velneo ID for almacen from the database
        
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        try:
            return self.velneo_mappings.get_from_general_alm()
        except Exception as e:
            logging.error(f"Error mapping almacen: {e}")
            return None

    def apply_map_emp(self) -> Optional[int]:
        """Get the Velneo ID for empresa from the database
        
        Args:
            ref: The reference code from the DBF record (not used in current implementation)
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        try:
            # Note: ref parameter is kept for consistency but not used in the current implementation
            return self.velneo_mappings.get_from_general_emp()
        except Exception as e:
            logging.error(f"Error mapping empresa: {e}")
            return None

    def apply_map_div(self) -> Optional[int]:
        """Get the Velneo ID for division from the database
        
        Args:
            ref: The reference code from the DBF record (not used in current implementation)
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        try:
            # Note: ref parameter is kept for consistency but not used in the current implementation
            return self.velneo_mappings.get_from_general_div()
        except Exception as e:
            logging.error(f"Error mapping division: {e}")
            return None
    
    def apply_map_tipo_mov(self, ref: str) -> Optional[int]:
        """Get the Velneo ID for tipo_movimiento from the database
        
        Args:
            ref: The reference code from the DBF record
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        if not ref:
            return None
            
        try:
            return self.velneo_mappings.get_tipo_mov(ref)
        except Exception as e:
            logging.error(f"Error mapping tipo_movimiento with ref {ref}: {e}")
            return None
    
    def apply_map_articulo(self, ref: str) -> Optional[int]:
        """Get the Velneo ID for articulo from the database
        
        Args:
            ref: The reference code from the DBF record
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        if not ref:
            return None
            
        try:
            return self.velneo_mappings.get_articulo(ref)
        except Exception as e:
            logging.error(f"Error mapping articulo with ref {ref}: {e}")
            return None

    def apply_map_tipo_iva(self, ref: str) -> Optional[int]:
        """Get the Velneo ID for iva from the database
        
        Args:
            ref: The reference code from the DBF record
            
        Returns:
            int: The mapped Velneo ID or None if not found
        """
        if not ref:
            return None
            
        try:
            return self.velneo_mappings.get_tipo_iva(ref)
        except Exception as e:
            logging.error(f"Error mapping articulo with ref {ref}: {e}")
            return None
    
    def process_record_fac(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process a complete record by applying all relevant mappings
        
        Args:
            record: Dictionary containing the DBF record data
            
        Returns:
            Dict[str, Any]: The processed record with mapped values
        """
        result = record.copy()
        # print(f' MAP FAC BEFORE {record}')
        # Apply mappings based on available fields in the record

        result['ser'] = self.apply_map_serie()
            
        result['clt'] = self.apply_map_cliente()
            
        result['fpg'] = self.apply_map_metodo_pago(record['fpg'])
            
        result['cmr'] = self.apply_map_vendedor(1)
            
        result['pai'] = self.apply_map_pais('México')

        result['emp_div'] = self.apply_map_div()

        result['emp'] = self.apply_map_emp()

        result['alm'] = self.apply_map_alm()

        # print(f' MAP FAC AFTER {result}')
      
        return result

    def process_record_det(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process a complete record by applying all relevant mappings
        
        Args:
            record: Dictionary containing the DBF record data
            
        Returns:
            Dict[str, Any]: The processed record with mapped values
        """
        result = record.copy()
        # print(f' MAP DETAIL BEFORE {record}')
        
        # Apply mappings based on available fields in the record
        result['alm'] = self.apply_map_alm()

        result['emp_div'] = self.apply_map_div()

        result['emp'] = self.apply_map_emp()

        result['art'] = self.apply_map_articulo(record['REF'])
     
            
        result['ser_vta'] = self.apply_map_serie()
     
        #result['mov_tip'] = self.apply_map_tipo_mov(record['tipo_mov'])
        result['mov_tip'] = 'V'

        result['reg_iva_vta'] = self.apply_map_tipo_iva(record['iva_vta'])

        result['clt'] = self.apply_map_cliente()

        # print(f' MAP DETAIL AFTER {result}')
   
        return result


