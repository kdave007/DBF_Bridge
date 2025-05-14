from typing import Dict, Any, List
from datetime import datetime
import json
from .base_model import BaseModel

class VentasModel(BaseModel):
    def __init__(self):
        super().__init__()
        self.table_name = "factura_venta"  # Header table
        self.chunk_size = 100
    
    def prepare_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for database insertion"""
        # Convert the date string to proper timestamp
        # Handle different date formats
        try:
            fecha = datetime.strptime(record['fecha'], '%d/%m/%Y %I:%M:%S %p')
        except ValueError:
            try:
                fecha = datetime.strptime(record['fecha'], '%d/%m/%Y %H:%M:%S')
            except ValueError:
                fecha = datetime.strptime(record['fecha'].replace('a. m.', 'AM').replace('p. m.', 'PM'), '%d/%m/%Y %I:%M:%S %p')
        
        return {
            'cabecera': record['Cabecera'],
            'folio': record['Folio'],
            'cliente': record['cliente'],
            'empleado': record['empleado'],
            'fecha': fecha,
            'total_bruto': float(record['total_bruto'])
        }