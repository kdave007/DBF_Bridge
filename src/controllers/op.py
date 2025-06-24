
from .send_request import SendRequest
from .send_details import SendDetails
from .api_response_tracking import APIResponseTracking
from src.config.db_config import PostgresConnection

class OP:
    def execute(self, operations):
        self.send_req = SendRequest()
        self.send_det = SendDetails()
        self.api_track = APIResponseTracking()
        self.db_config = PostgresConnection.get_db_config()

        self.bypass_ca = False

        if "create" in operations:
            self._create(operations['create'])

        if "update" in operations:
           self._update(operations['update'])

        if "delete" in operations:
            self._delete(operations['delete'])     



    def _create(self, records):
        base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/vta_fac_g"
        api_key = "123456"
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')
            
            # First request - can be bypassed
            first_request_success = False
            
            if self.bypass_ca:
                print(f"Bypassing first API call for folio: {record.get('folio')}")
                first_request_success = True  # Pretend it was successful
            else:
                # Make the first API call
                ca_req_result = self.send_req.create(record, base_url, api_key)
                
                # Check if the first request was successful
                if ca_req_result['success']:
                    print(f"Successfully processed first request for folio: {record.get('folio')}")
                    #insert in the db the posted CA record
                    self.api_track._create_op(ca_req_result['success'][0])
                    
                    first_request_success = True
                else:
                    print(f"Failed to process first request for folio: {record.get('folio')}")
                    if ca_req_result['failed']:
                        for failure in ca_req_result['failed']:
                            print(f"Failure reason: {failure.get('error_msg')}")
                    # Skip to next record if first request failed
                    continue
            
            if first_request_success:

                if self.bypass_ca:
                    parent_ref = {
                        'parent_id':  111,
                        'fecha': record['dbf_record'].get('fecha')
                    }
                else:    
                    parent_ref = {
                        'parent_id':  ca_req_result['success'][0].get('id'),
                        'fecha': ca_req_result['success'][0].get('fecha_emision')
                    }
                
                print(f"Ready to process details request for folio: {record.get('folio')}")
                
                det_req_results = self.send_det.req_post(record['dbf_record'].get('detalles'), parent_ref)

                if det_req_results['failed']:
                    #one request failed, so skip to next CA
                    continue
                
                # Update the record status to indicate details were processed successfully
                self.api_track._pa_completed(parent_ref['parent_id'])
                self.api_track.update_create_details(det_req_results['records'])
                
                # Call after request handler
                emp =  record['dbf_record']['detalles'][0].get('emp')
                emp_div =  record['dbf_record']['detalles'][0].get('emp_div')
                self._after_request(parent_ref['parent_id'], emp, emp_div)
                

    def _update(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')

    def _delete(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')


    def _after_request(self, id, emp, emp_div):
        self.send_det.send_update_fac_off(id, emp, emp_div)

    

    


