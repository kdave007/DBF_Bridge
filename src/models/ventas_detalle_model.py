from typing import Dict, Any, List
import logging

class VentasDetalleModel():
    def __init__(self):
        super().__init__()
        self.table_name = "detalle_factura_venta"  # Detail table
        self.chunk_size = 100
    
    def prepare_record(self, header_id: str, detail: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a detail record for database insertion"""
        try:
            return {
                'folio': detail['Folio'],
                'referencia': detail['REF'],
                'cantidad': float(detail['cantidad']),
                'precio': float(detail['precio']),
                'descuento': float(detail.get('descuento', 0))
            }
        except (KeyError, ValueError) as e:
            logging.error(f"Error preparing detail record: {str(e)}\nDetail data: {detail}")
            raise ValueError(f"Invalid detail data: {str(e)}")
    
    def prepare_batch(self, header_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Prepare a batch of detail records from a header record"""
        details = []
        header_id = f"{header_record['Folio']}"
        
        if 'detalles' in header_record:
            for detail in header_record['detalles']:
                detail_record = self.prepare_record(header_id, detail)
                details.append(detail_record)
        return details
