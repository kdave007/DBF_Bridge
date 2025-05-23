import os
import sys
from datetime import datetime

from src.db.response_tracking import ResponseTracking


class APIResponseTracking:
    def __init__(self):
        pass

    def update_tracker(self, responses_status):
        # print(responses_status['create'].get('failed',{}))
        # print(responses_status['update'].get('failed',{}))
        # print(responses_status['delete'].get('failed',{}))

        db_config = {
            'host': 'localhost',
            'database': 'suc_vel',
            'user': 'postgres',
            'password': 'comexcare',
            'port': '5432'
        }


        self.resp_tracking = ResponseTracking(db_config)

        self._create_op(responses_status['create'])
        self._update_op(responses_status['update'])
        self._delete_op(responses_status['delete'])

    def _create_op(self, results):
        action = 'agregado'
        estado = 'ca_completado'
        if results.get('success'):
           
           for item in results.get('success'):
                # Parse the date string from DBF format to a proper date object
                fecha_str = item.get('fecha_emision')
                try:
                    # Remove the 'a. m.' or 'p. m.' part and parse the date
                    fecha_str = fecha_str.replace(' a. m.', '').replace(' p. m.', '')
                    # Format is day/month/year in the DBF records
                    fecha_date = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S').date()
                except (ValueError, AttributeError):
                    # Fallback to current date if parsing fails
                    fecha_date = datetime.now().date()
                    print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
                
                self.resp_tracking.update_status(
                    item.get('folio'),
                    item.get('total_partidas'),
                    item.get('hash'),
                    estado,
                    action,
                    fecha_date
                )


            

    def _update_op(self, results):
        action = 'modificado'
        estado = 'ca_completado'
        if results.get('success'):
            for item in results.get('success'):
                # Parse the date string from DBF format to a proper date object
                fecha_str = item.get('fecha_emision')
                try:
                    # Remove the 'a. m.' or 'p. m.' part and parse the date
                    fecha_str = fecha_str.replace(' a. m.', '').replace(' p. m.', '')
                    # Format is day/month/year in the DBF records
                    fecha_date = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S').date()
                except (ValueError, AttributeError):
                    # Fallback to current date if parsing fails
                    fecha_date = datetime.now().date()
                    print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
                
                self.resp_tracking.update_status(
                    item.get('folio'),
                    item.get('total_partidas'),
                    item.get('hash'),
                    estado,
                    action,
                    fecha_date
                )

    def _delete_op(self, results):
        action = 'eliminado'
        estado = 'ca_eliminado'
        if results.get('success'):
            for item in results.get('success'):
                # Parse the date string from DBF format to a proper date object
                fecha_str = item.get('fecha_emision')
                try:
                    # Remove the 'a. m.' or 'p. m.' part and parse the date
                    fecha_str = fecha_str.replace(' a. m.', '').replace(' p. m.', '')
                    # Format is day/month/year in the DBF records
                    fecha_date = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S').date()
                except (ValueError, AttributeError):
                    # Fallback to current date if parsing fails
                    fecha_date = datetime.now().date()
                    print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
                
                self.resp_tracking.update_status(
                    item.get('folio'),
                    item.get('total_partidas'),
                    item.get('hash'),
                    estado,
                    action,
                    fecha_date
                )