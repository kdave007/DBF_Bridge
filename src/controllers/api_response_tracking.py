import os
import sys
from datetime import datetime, date
from src.config.db_config import PostgresConnection
from src.db.response_tracking import ResponseTracking
from src.db.detail_tracking import DetailTracking


class APIResponseTracking:
    def __init__(self):
        self.db_config = PostgresConnection.get_db_config()
        
        # Initialize ResponseTracking with the configuration dictionary
        self.resp_tracking = ResponseTracking(self.db_config)

    # def update_tracker(self, responses_status):

    #     status_create = self._create_op(responses_status['create'])
    #     print(f'status_create: {status_create}')
    #     status_update = self._update_op(responses_status['update'])
    #     print(f'status_update: {status_update}')
    #     status_delete = self._delete_op(responses_status['delete'])
    #     print(f'status_delete: {status_delete}')

    #     next_step = False
        
    #     if status_create['execute']:
    #         if status_create['done']:
    #             next_step = True
    #         else:
    #             return False

    #     if status_update['execute']:
    #         if status_update['done']:
    #             next_step = True
    #         else:
    #             return False

    #     if status_delete['execute']:
    #         if status_delete['done']:
    #             next_step = True
    #         else:
    #             return False
    #     print(f'CHECK ')
    #     return next_step



    def _create_op(self, item):
        action = 'agregado'
        estado = 'ca_completado'
          
        # Parse the date string from DBF format to a proper date object
        print(f'item to insert {item}')
        
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
        
        return self.resp_tracking.update_status(
            item.get('id'),
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
        done = False
        execute = False

        if results.get('success'):
            execute = True
            for item in results.get('success'):
              
                # Parse the date string from DBF format to a proper date object
                fecha_str = item.get('fecha_emision')
                try:
                   # Extract only the date part (DD/MM/YYYY) and ignore time
                    if fecha_str and isinstance(fecha_str, str):
                        # Get only the date part by splitting on space and taking first part
                        date_part = fecha_str.split(' ')[0]
                        # Split by / to get day, month, year
                        day, month, year = date_part.split('/')
                        # Create date object with just the date components
                        
                        fecha_date = date(int(year), int(month), int(day))
                    else:
                        fecha_date = datetime.now().date()
                except (ValueError, AttributeError):
                    # Fallback to current date if parsing fails
                    fecha_date = datetime.now().date()
                    print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
                
                done = self.resp_tracking.update_status(
                    item.get('id'),
                    item.get('folio'),
                    item.get('total_partidas'),
                    item.get('hash'),
                    estado,
                    action,
                    fecha_date
                )

        return {'done': done, 'execute':execute}


    def _delete_op(self, results):
        action = 'eliminado'
        estado = 'ca_eliminado'
        done = False
        execute = False

        if results.get('success'):
           
            execute = True
            for item in results.get('success'):
                print(f" ITEM {item}")
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
                
                done = self.resp_tracking.delete_by_id(
                    item.get('id')
                )
                
    def _pa_completed(self, id):
        """Update record status to indicate that all details have been processed
        
        Args:
            id: The ID of the record to update
            
        Returns:
            bool: True if the update was successful, False otherwise
        """
        action = 'procesado'
        estado = 'pa_completado'
        
        print(f"Updating record {id} to status: {estado}, action: {action}")
        
        return self.resp_tracking.update_record_status(id, estado, action)

    def update_create_details(self, records):
        """
        Insert or update records in the database
        
        Args:
            db_connection: Database configuration dictionary
            records: List of records to insert/update
            
        Returns:
            Number of records successfully processed
        """
        if not records:
            return 0
            
        # Create a DetailTracking instance with the database configuration
        # db_connection is now a dictionary, not a PostgresConnection object
        detail_tracker = DetailTracking(self.db_config)
        
        # Use batch_insert_details method to insert all records at once
        success = detail_tracker.batch_replace_by_id(records)
        
        if success:
            return len(records)
        else:
            print("Error inserting records")
            return 0