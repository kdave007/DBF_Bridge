from typing import Dict, Any, List, Optional
from datetime import datetime
from psycopg2.extensions import AsIs


class VentasTrackingModel():
    def __init__(self):
    
        self.table_name = "estado_factura_venta"  # Tracking table
        self.chunk_size = 100
        # Reset the printed flag on init
        if hasattr(self, '_printed_estado_factura_venta'):
            delattr(self, '_printed_estado_factura_venta')
    
    def prepare_record(self, header_record: Dict[str, Any], total_partidas: int, batch_id: str) -> Dict[str, Any]:
        """Prepare a tracking record
        
        Args:
            header_record: The header record containing sale information
            total_partidas: Total number of items in the sale
            batch_id: Unique batch identifier for grouping records
            
        Returns:
            Dict containing the tracking record data
        """
        return {
            'id': int(header_record['Folio']),  # Using folio as numeric ID
            'folio': str(header_record['Folio']),  # Store as string
            'total_partidas': total_partidas,
            'descripcion':None,
            'hash': None,
            'estado': 'PENDING',
            'fecha_procesamiento': None,
            'id_lote': batch_id
            # Note: created_at and updated_at are handled by DB defaults
        }
    
    def update_status(self, folio: int, estado: str, hash_value: str = None) -> None:
        """Update the status of a tracking record
        
        Args:
            folio: The sale folio number
            estado: New status to set
            hash_value: Optional hash from API response
        """
        query = """
        UPDATE estado_factura_venta
        SET estado = %(estado)s,
            hash = %(hash)s,
            fecha_procesamiento = CURRENT_TIMESTAMP
        WHERE id = %(folio)s
        """
        params = {
            'estado': estado,
            'hash': hash_value,
            'folio': folio
        }
        self.db.execute_query(query, params)
        
    def update_batch_status(self, batch_id: str, estado: str) -> None:
        """Update the status of all records in a batch
        
        Args:
            batch_id: The batch identifier
            estado: New status to set for all records in batch
        """
        query = """
        UPDATE estado_factura_venta
        SET estado = %(estado)s,
            fecha_procesamiento = CURRENT_TIMESTAMP
        WHERE id_lote = %(batch_id)s
        """
        params = {
            'estado': estado,
            'batch_id': batch_id
        }
        self.db.execute_query(query, params)
