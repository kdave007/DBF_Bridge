from typing import Dict, Any, List
from datetime import datetime


class VentasModel():
    def __init__(self):
        
        self.table_name = "factura_venta"  # Header table
        self.chunk_size = 100
    
    def prepare_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for database insertion"""
        # Print record fields for debugging
        if not hasattr(self.__class__, '_printed_fields'):
            print("\nAvailable fields in record:", list(record.keys()))
            setattr(self.__class__, '_printed_fields', True)

        # Convert the date string to proper timestamp
        fecha_str = record['fecha']
        
        # First try direct parse with Spanish AM/PM
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y %I:%M:%S a. m.')
        except ValueError:
            try:
                fecha = datetime.strptime(fecha_str, '%d/%m/%Y %I:%M:%S p. m.')
                # Add 12 hours for PM
                if fecha.hour != 12:  # Don't add 12 hours if it's 12 PM
                    fecha = fecha.replace(hour=fecha.hour + 12)
            except ValueError:
                try:
                    # Try 24-hour format
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S')
                except ValueError as e:
                    print(f"Failed to parse date: {fecha_str}")
                    raise
        
        prepared_record = {
            'id': int(record['Folio']),  # Using folio as ID
            'cabecera': record['Cabecera'],
            'folio': int(record['Folio']),
            'cliente': record['cliente'],
            'empleado': int(record['empleado']),
            'fecha': fecha.strftime('%Y-%m-%d %H:%M:%S'),  # Format as string for PostgreSQL
            'total_bruto': float(record['total_bruto'])
            # fecha_creacion and fecha_actualizacion are handled by DB defaults
        }
        
        if not hasattr(self.__class__, '_printed_prepared'):
            print("\nPrepared record:", prepared_record)
            setattr(self.__class__, '_printed_prepared', True)
            
        return prepared_record