from ast import Try
import os
import sys
from turtle import st
from pathlib import Path
from src.config.db_config import PostgresConnection
from src.db.response_tracking import ResponseTracking
import requests
import json
from decimal import Decimal
from datetime import datetime, date


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

class SendRequest:

    def __init__(self):
        # Get database configuration as a dictionary
        self.db_config = PostgresConnection.get_db_config()
        # Initialize ResponseTracking with the configuration dictionary
        self.response_tracking = ResponseTracking(self.db_config)

    # def send(self, responses_dict):
    #     """Process API operations in batches of 100 and track results"""
    #     # responses_dict contains API operation results (update, delete, create)
    #     if not responses_dict:
    #         return False
        
    #     # API configuration
    #     self.base_url = "http://localhost:3000/api/data"  # Replace with your actual API URL
    #     self.base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/vta_fac_g"
    #     self.api_key = "123456"
    #     self.table_name = "vta_fac_g"
    #     self.headers = {
    #         "Content-Type": "application/json",
    #         "Accept": "application/json",
    #         "x-process-json": "true"
    #     }
    #     self.batch_size = 100
    #     self.json_encoder = CustomJSONEncoder
        
    #     # Separate operations by type
    #     creates = responses_dict.get('create', [])
    #     updates = responses_dict.get('update', [])
    #     deletes = responses_dict.get('delete', [])
    #     print("delete:")
    #     print(deletes)
        

    #     # Initialize results dictionary
    #     results = {
    #         'create': {},
    #         'update': {},
    #         'delete': {},
    #         'next_check':responses_dict.get('next_check', [])
    #     }
        
    #     # Process in batches
    #     if creates:
    #         print(f' CREATE PROCESS >>')
    #         result_create = self.create(creates)
    #         results['create'] = result_create
    #         # print("Creates:")
    #         # print(json.dumps(result_create, indent=4, cls=self.json_encoder))

    #     if updates:  
    #         print(f' UPDATE PROCESS >>')  
    #         result_update = self.update(updates)
    #         results['update'] = result_update
    #         # print("Updates:")
    #         # print(json.dumps(result_update, indent=4, cls=self.json_encoder))

    #     if deletes:
    #         print(f' DELETE PROCESS >>')
    #         result_delete = self.delete(deletes)
    #         results['delete'] = result_delete
    #         # print("Deletes:")
    #         # print(json.dumps(result_delete, indent=4, cls=self.json_encoder))
        
    #     return results

    def create(self, record, base_url, api_key):
        """
        Process a single record and send it to the API
        
        Args:
            record: A single record dictionary containing 'folio' and 'dbf_record'
            
        Returns:
            Dictionary with 'success' and 'failed' lists containing the operation result
        """
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        folio = record.get('folio')
        dbf_record = record.get('dbf_record', {})
        
        print(f"Processing CREATE operation for folio {folio}")
        
        try:
            # Prepare payload for the single record
            try:
                # Prepare payload for a single record
                single_payload = {
                    "emp": str(dbf_record.get('emp')),
                    "emp_div": str(dbf_record.get('emp_div')),
                    "num_doc": folio,
                    "clt": dbf_record.get('clt'),
                    # "fpg": dbf_record.get('fpg'),
                    "fpg": 20,
                    "cmr": dbf_record.get('cmr'),
                    "fch": self._format_date_to_iso(dbf_record.get("fecha")),
                    # "tot_fac": dbf_record.get("total_bruto"),
                    "ser": dbf_record.get('ser'),
                    "hor": self._format_hour_to_12h(dbf_record.get('hor')),
                    "pai": dbf_record.get('pai'),
                    "ent_rel_tip": 1,
                    "mon_c": 1,
                    "cot": 1,
                    "fch_vto": self._format_date_to_iso(dbf_record.get("fecha")),
                    "pre_con_iva_inc": 1,
                    "trm": 1,
                    "dum": 1,
                    "alm": str(dbf_record.get('alm')),
                    "fac": "1",
                    "off": 1,
                    "error de campor":1
                }
            except Exception as e:
                print(f'Error preparing payload: {e}')
                raise
            
            # Send the record
            print(f"Sending record for folio {folio}")
            post_data = json.dumps(single_payload, cls=CustomJSONEncoder)
            print(f"POST Request URL: {base_url}?api_key={api_key}")
            print(f"POST Request Data: {post_data}")


            # print("STOP")
            # sys.exit()
            
            response = requests.post(
                f"{base_url}?api_key={api_key}", 
                headers = headers,
                data=post_data
            )
            
            print(f"Response Status Code for folio {folio}: {response.status_code}")
            print(f"Response Headers for folio {folio}: {response.headers}")
            
            # Process the response
            if response.status_code in [200, 201, 202, 204]:
                try:
                    # Parse response JSON
                    response_json = response.json()
                    print(f"Response JSON for folio {folio}: {response_json}")
                    
                    if 'vta_fac_g' not in response_json:
                        print(f"Key 'vta_fac_g' not found in response for folio {folio}. Full response: {response_json}")
                        raise KeyError("'vta_fac_g' key not found in response")
                    
                    if not response_json['vta_fac_g']:
                        print(f"'vta_fac_g' is empty for folio {folio}. Full response: {response_json}")
                        raise ValueError("'vta_fac_g' is empty in response")
                    
                    # Process each item in the response
                    for resp_item in response_json['vta_fac_g']:
                        id_value = resp_item.get('id')
                        folio_str = str(resp_item.get('num_doc'))
                        
                        # Add to success results
                        results['success'].append({
                            'folio': folio_str,
                            'id': id_value,
                            'fecha_emision': dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles', [])),
                            'hash': record.get('dbf_hash', ''),
                            'details': dbf_record.get('detalles', []),
                            'status': response.status_code
                        })
                        
                        print(f"Successfully processed response for folio {folio_str}")
                except Exception as e:
                    print(f"Error processing response for folio {folio}: {str(e)}")
                    # Add to failed results
                    results['failed'].append({
                        'folio': folio,
                        'fecha_emision': dbf_record.get('fecha'),
                        'total_partidas': len(dbf_record.get('detalles', [])),
                        'hash': record.get('dbf_hash', ''),
                        'status': response.status_code,
                        'error_msg': f"Error processing response: {str(e)}"
                    })
            else:
                # Failed request
                error_message = f"Request failed with status {response.status_code}: {response.text}"
                print(f"Error for folio {folio}: {error_message}")
                
                results['failed'].append({
                    'folio': folio,
                    'fecha_emision': dbf_record.get('fecha'),
                    'total_partidas': len(dbf_record.get('detalles', [])),
                    'hash': record.get('dbf_hash', ''),
                    'status': response.status_code,
                    'error_msg': error_message
                })
                
        except Exception as e:
            error_message = f"Exception during create operation: {str(e)}"
            print(error_message)
            # Mark the record as failed
            results['failed'].append({
                'folio': folio, 
                'fecha_emision': dbf_record.get('fecha'),
                'hash': record.get('dbf_hash', ''),
                'status': None,
                'error_msg': error_message
            })
                  
        return results

    def update(self, update):
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(update)} UPDATE operations in batches of {self.batch_size}")
        for i in range(0, len(update), self.batch_size):
            batch = update[i:i+self.batch_size]
            print(f"Processing UPDATE batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                # Prepare batch payload
                batch_payload = []
                folio_to_item = {}

                current_folio = None

                # Process each item in the batch individually
                for item in batch:
                    folio = item.get('folio')
                    folio_to_item[folio] = item
                    dbf_record = item.get('dbf_record', {})
                
                    # Prepare payload for a single record
                    single_payload = {
                        "id":item.get("id"),
                        "emp":str(dbf_record.get('emp')),
                        "emp_div": str(dbf_record.get('emp_div')),
                        "num_fac": f'VTA/-"{folio}',
                        "num_doc":folio,
                        "clt": dbf_record.get('clt'),
                        "fpg": dbf_record.get('fpg'),
                        "cmr": dbf_record.get('cmr'),
                        "fch": self._format_date_to_iso(dbf_record.get("fecha")),
                        "tot_fac": dbf_record.get("total_bruto"),
                        "ser":dbf_record.get('ser'),
                        "hor":self._format_hour_to_12h(dbf_record.get('hor')),
                        "pai":dbf_record.get('pai'),
                        "ent_rel_tip":1,
                        "mon_c":1,
                        "cot":1,
                        "fch_vto":self._format_date_to_iso(dbf_record.get("fecha")),
                        "pre_con_iva_inc":1,
                        "trm":1,
                        "dum":1,
                        "off":1
                    }
                    
                    # Send the single record
                    print(f"Sending record for folio {folio}")
                    post_data = json.dumps(single_payload, cls=CustomJSONEncoder)
                    print(f"POST Request URL: {self.base_url}/{item.get("id")}?api_key={self.api_key}")
                    print(f"POST Request Data: {post_data}")
                    
                    response = requests.post(
                        f"{self.base_url}/{item.get("id")}?api_key={self.api_key}", 
                        headers=self.headers, 
                        data=post_data
                    )
                    
                    print(f"Response Status Code for folio {folio}: {response.status_code}")
                    print(f"Response Headers for folio {folio}: {response.headers}")
                    
                    # Process this individual response immediately
                    if response.status_code in [200, 201, 202, 204]:
                        try:
                            # Parse response JSON
                            response_json = response.json()
                            print(f"Response JSON for folio {folio}: {response_json}")
                            
                            if 'vta_fac_g' not in response_json:
                                print(f"Key 'vta_fac_g' not found in response for folio {folio}. Full response: {response_json}")
                                continue
                                
                            if not response_json['vta_fac_g']:
                                print(f"'vta_fac_g' is empty for folio {folio}. Full response: {response_json}")
                                continue
                                
                            # Process each item in the response
                            for resp_item in response_json['vta_fac_g']:
                                id_value = resp_item.get('id')
                                folio_str = str(resp_item.get('num_doc'))
                                
                                # Find the original item
                                original_item = folio_to_item.get(folio_str)
                                if not original_item:
                                    print(f"Warning: Could not find original item for folio {folio_str}")
                                    continue
                                    
                                dbf_record = original_item.get('dbf_record', {})
                                
                                # Add to success results
                                results['success'].append({
                                    'folio': folio_str,
                                    'id': id_value,
                                    'fecha_emision': dbf_record.get('fecha'),
                                    'total_partidas': len(dbf_record.get('detalles', [])),
                                    'hash': original_item.get('dbf_hash', ''),
                                    'details': dbf_record.get('detalles', []),
                                    'status': response.status_code
                                })
                                
                                print(f"Successfully processed response for folio {folio_str}")
                        except Exception as e:
                            print(f"Error processing response for folio {folio}: {str(e)}")
                            # Add to failed results
                            results['failed'].append({
                                'folio': folio,
                                'fecha_emision': dbf_record.get('fecha'),
                                'total_partidas': len(dbf_record.get('detalles', [])),
                                'hash': item.get('dbf_hash', ''),
                                'status': response.status_code,
                                'error_msg': f"Error processing response: {str(e)}"
                            })
                    else:
                        # Failed request
                        error_message = f"Request failed with status {response.status_code}: {response.text}"
                        print(f"Error for folio {folio}: {error_message}")
                        
                        results['failed'].append({
                            'folio': folio,
                            'fecha_emision': dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles', [])),
                            'hash': item.get('dbf_hash', ''),
                            'status': response.status_code,
                            'error_msg': error_message
                        })
                # All response processing is now done individually for each record
                        
            except Exception as e:
                error_message = f"Exception during batch create: {str(e)}"
                print(error_message)
              
                # Mark all records in the batch as failed
                for item in batch_payload:
                        original_item = folio_to_item.get(folio)
                        dbf_record = original_item.get('dbf_record', {})
                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        
                        results['failed'].append({
                            'folio': item.get('folio'), 
                            'fecha_emision':  dbf_record.get('fecha'),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': None,
                            'error_msg':error_message
                            })
                  
        return results
    

    def delete(self, deletes):
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(deletes)} DELETE operations in batches of {self.batch_size}")
        for i in range(0, len(deletes), self.batch_size):
            batch = deletes[i:i+self.batch_size]
            print(f"Processing DELETE batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                # Prepare batch payload
                batch_payload = []
                folio_to_item = {}

                for item in batch:
                    print(item)
                    folio = item.get('folio')

                    folio_to_item[folio] = item
                    dbf_record = item.get('dbf_record', {})
                    batch_payload.append({
                        "folio": folio,
                        "cabecera": dbf_record.get("Cabecera"),
                        "cliente": dbf_record.get("cliente"),
                        "empleado": dbf_record.get("empleado"),
                        "fecha": dbf_record.get("fecha"),
                        "total_bruto": dbf_record.get("total_bruto")
                    })
                
                # Extract folios for the URL
                folios = [item.get('folio') for item in batch]
                ids = [str(item.get('id')) for item in batch]
                
                # If only one element, use the ID directly; otherwise join with comma
                if len(ids) == 1:
                    all_ids = ids[0]
                else:
                    all_ids = "%2C".join(ids)

                # Send the single record
                print(f"Sending record for folio {folio}")
                #post_data = json.dumps(single_payload, cls=CustomJSONEncoder)
                print(f"POST Request URL: {self.base_url}/{all_ids}?api_key={self.api_key}")
               
                    
                
                # Make batch DELETE request with folios in the URL
                response = requests.delete(
                    f"{self.base_url}/{all_ids}?api_key={self.api_key}", 
                    headers=self.headers
                )

                print(f"Response Status Code for folio {folio}: {response.status_code}")
                print(f"Response Headers for folio {folio}: {response.headers}")
                
                if response.status_code in [200, 201, 202, 204]:
                    # For DELETE operations, the response might be different from create/update
                    # It could be a list of deleted IDs or a success message
                    try:
                        batch_response = response.json()
                        print(batch_response)
                        
                        # Check for the specific success response format: {"return": "Eliminado(s) con éxito"}
                        if isinstance(batch_response, dict) and batch_response.get('return') == "Eliminado(s) con éxito":
                            # Success response for batch deletion - all items were successfully deleted
                            for folio in folios:
                                if folio in folio_to_item:
                                    original_item = folio_to_item.get(folio)
                                    dbf_record = original_item.get('dbf_record', {})
                                    
                                    results['success'].append({
                                        'folio': folio,
                                        'id': original_item.get('id'),
                                        'status': response.status_code
                                    })
                        # If the response contains a list of deleted items
                        elif isinstance(batch_response, list):
                            for deleted_item in batch_response:
                                folio = deleted_item.get('folio')
                                if folio and folio in folio_to_item:
                                    original_item = folio_to_item.get(folio)
                                    dbf_record = original_item.get('dbf_record', {})
                                    
                                    results['success'].append({
                                        'folio': folio,
                                        'id': deleted_item.get('id'),
                                        'status': response.status_code
                                    })
                        # If the response is any other format
                        else:
                            # Consider all items in the batch as successfully deleted
                            for folio in folios:
                                if folio in folio_to_item:
                                    original_item = folio_to_item.get(folio)
                                    
                                    results['success'].append({
                                        'folio': folio,
                                        'id': original_item.get('id'),
                                        'status': response.status_code
                                    })
                    except ValueError:
                        # If the response is not JSON, consider all items successful
                        for folio in folios:
                            if folio in folio_to_item:
                                original_item = folio_to_item.get(folio)
                                dbf_record = original_item.get('dbf_record', {})
                                
                                results['success'].append({
                                    'folio': folio,
                                    'id': original_item.get('id'),
                                    'status': response.status_code
                                })
        
                else:
                    error_message = f"Batch delete failed with status {response.status_code}: {response.text}"
                    # Mark all items in the batch as failed
                    for folio in folios:
                        if folio in folio_to_item:
                            original_item = folio_to_item.get(folio)
                            dbf_record = original_item.get('dbf_record', {})
                            
                            results['failed'].append({
                                'folio': folio,
                                'fecha_emision': dbf_record.get('fecha'),
                                'total_partidas': len(dbf_record.get('detalles', [])),
                                'hash': original_item.get('dbf_hash', ''),
                                'status': response.status_code,
                            'error_msg':error_message
                            })
                        
            except Exception as e:
                error_message = f"Exception during batch create: {str(e)}"
                print(f' error message {error_message}')
                # Mark all records in the batch as failed
                for item in batch_payload:
                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        print(f'item aa {item}')
                        results['failed'].append({
                            'folio': item.get('folio'), 
                            'status': None,
                            'error_msg':error_message
                            })
                  

        return results

    def update_lote_hash(self):
        pass
        
    def _format_date_to_iso(self, date_str):
        """
        Convert date from format like "30/04/2025 12:00:00 a. m." to "2025-04-30"
        
        Args:
            date_str: Date string in DD/MM/YYYY format with possible time component
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        if not date_str:
            return ""
            
        try:
            # Split by space to separate date and time
            parts = date_str.split(' ')
            date_part = parts[0]
            
            # Split the date part by /
            day, month, year = date_part.split('/')
            
            # Format to YYYY-MM-DD
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        except Exception as e:
            print(f"Error formatting date {date_str}: {e}")
            return date_str  # Return original if parsing fails
            
    def _format_hour_to_12h(self, hour_value):
        """
        Format hour value with minutes and seconds
        
        Args:
            hour_value: Integer representing hour in 24-hour format (0-23)
            
        Returns:
            String in format "hh:00:00"
        """
        if hour_value is None:
            return ""
            
        try:
            # Convert to integer if it's a string
            if isinstance(hour_value, str):
                hour_value = int(hour_value)
                
            # Format hour with minutes and seconds
            return f"{hour_value:02d}:00:00"
        except Exception as e:
            print(f"Error formatting hour: {e}")
            return f"{hour_value}:00:00"  # Return original if parsing fails
            
    def update_lote_hash(self):
        pass  # Return original if parsing fails