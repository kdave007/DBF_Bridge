import os
import sys
from turtle import st
from pathlib import Path
from src.db import response_tracking
from src.db.response_tracking import ResponseTracking
import requests
import json


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

class SendRequest:

    def __init__(self):
        db_config = {
            'host': 'localhost',
            'database': 'suc_vel',
            'user': 'postgres',
            'password': 'comexcare',
            'port': '5432'
        }
        self.response_tracking = ResponseTracking(db_config)

    def send(self, responses_dict):
        """Process API operations in batches of 100 and track results"""
        # responses_dict contains API operation results (update, delete, create)
        if not responses_dict:
            return False
        
        # API configuration
        self.base_url = "http://localhost:3000/api/data"  # Replace with your actual API URL
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-process-json": "true"
        }
        self.batch_size = 100
        self.json_encoder = CustomJSONEncoder
        
        # Separate operations by type
        creates = responses_dict.get('create', [])
        updates = responses_dict.get('update', [])
        deletes = responses_dict.get('delete', [])
        

        # Initialize results dictionary
        results = {
            'create': {},
            'update': {},
            'delete': {}
        }
        
        # Process in batches
        if creates:
            result_create = self.create(creates)
            results['create'] = result_create
            print("Creates:")
            print(json.dumps(result_create, indent=4, cls=self.json_encoder))

        if updates:    
            result_update = self.update(updates)
            results['update'] = result_update
            print("Updates:")
            print(json.dumps(result_update, indent=4, cls=self.json_encoder))

        if deletes:
            result_delete = self.delete(deletes)
            results['delete'] = result_delete
            print("Deletes:")
            print(json.dumps(result_delete, indent=4, cls=self.json_encoder))
        
        return results

    def create(self, creates):
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(creates)} create operations in batches of {self.batch_size}")
        for i in range(0, len(creates), self.batch_size):
            batch = creates[i:i+self.batch_size]
            print(f"Processing create batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                # Prepare batch payload
                batch_payload = []
                folio_to_item = {}

                for item in batch:
                   
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
                
                # Make batch POST request
                response = requests.post(
                    f"{self.base_url}", 
                    headers=self.headers, 
                    data=json.dumps(batch_payload, cls=CustomJSONEncoder)
                )
                
                
                # Process batch response
                if response.status_code in [200, 201, 202, 204]:
                    batch_response = response.json()
                   

                    for item in batch_response:
                        # The API should return a response with status for each record
                        folio = item.get('folio')
        
                        # Get the original item with hash and fecha
                        original_item = folio_to_item.get(folio)
                        dbf_record = original_item.get('dbf_record', {})
                        results['success'].append({
                            'folio': item.get('folio'), 
                            'fecha_emision': dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles')),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': response.status_code
                            })
        
                else:
                    for item in batch_payload:
                        folio = item.get('folio')
    
                        # Get the original item with hash and fecha
                        original_item = folio_to_item.get(folio)
                        dbf_record = original_item.get('dbf_record', {})

                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        print(f'original_item {original_item}')
                        results['failed'].append({
                            'folio': item.get('folio'),
                            'fecha_emision':  dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles')),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': response.status_code,
                            'error_msg':error_message
                            })
                        
            except Exception as e:
                error_message = f"Exception during batch create: {str(e)}"
              
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

    def update(self, updates):
        results = {
            'success': {},  # Will store folio -> result for successful operations
            'failed': {}   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(updates)} update operations in batches of {self.batch_size}")
        for i in range(0, len(updates), self.batch_size):
            batch = updates[i:i+self.batch_size]
            print(f"Processing update batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                 # Prepare batch payload
                batch_payload = []
                folio_to_item = {}

                for item in batch:
                   
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
                
                # Make batch POST request
                response = requests.post(
                    f"{self.base_url}", 
                    headers=self.headers, 
                    data=json.dumps(batch_payload, cls=CustomJSONEncoder)
                )
                
                if response.status_code in [200, 201, 202, 204]:
                    batch_response = response.json()
                    
                    
                    for item in batch_response:
                        # The API should return a response with status for each record
                        folio = item.get('folio')
        
                        # Get the original item with hash and fecha
                        original_item = folio_to_item.get(folio)
                        dbf_record = original_item.get('dbf_record', {})
                        results['success'].append({
                            'folio': item.get('folio'), 
                            'fecha_emision': dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles')),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': response.status_code
                            })
                else:
                    for item in batch_payload:
                        folio = item.get('folio')
    
                        # Get the original item with hash and fecha
                        original_item = folio_to_item.get(folio)
                        dbf_record = original_item.get('dbf_record', {})

                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        print(f'original_item {original_item}')
                        results['failed'].append({
                            'folio': item.get('folio'),
                            'fecha_emision':  dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles')),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': response.status_code,
                            'error_msg':error_message
                            })
        
                        
            except Exception as e:
                error_message = f"Exception during batch create: {str(e)}"
              
                # Mark all records in the batch as failed
                for item in batch_payload:
                        original_item = folio_to_item.get(folio)
                        dbf_record = original_item.get('dbf_record', {})
                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        
                        results['failed'].append({
                            'folio': item.get('folio'), 
                            'fecha_emision':  dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles')),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': None,
                            'error_msg':error_message
                            })
                  

        return results

    def delete(self, deletes):# TO DO : update whole method <------------------------------------------------------
        results = {
            'success': {},  # Will store folio -> result for successful operations
            'failed': {}   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(deletes)} delete operations in batches of {self.batch_size}")
        for i in range(0, len(deletes), self.batch_size):
            batch = deletes[i:i+self.batch_size]
            print(f"Processing delete batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                # Prepare batch payload
                batch_payload = []
                folio_to_item = {}

                for item in batch:
                   
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
                
                # Make batch DELETE request
                response = requests.delete(
                    f"{self.base_url}", 
                    headers=self.headers,
                    data=json.dumps(batch_payload, cls=self.json_encoder)
                )
                
                if response.status_code in [200, 201, 202, 204]:
                    batch_response = response.json()
                    # The API should return a response with status for each record
                    folio = item.get('folio')
    
                    # Get the original item with hash and fecha
                    original_item = folio_to_item.get(folio)
                    dbf_record = original_item.get('dbf_record', {})

                    for item in batch_response:
                        print(f'item {item}')
                        results['success'].append({
                            'folio': item.get('folio'), 
                            'fecha_emision': dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles')),
                            'hash': original_item.get('dbf_hash', ''),
                            'status': response.status_code
                            })
        
                else:
                    for item in batch_payload:
                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        print(f'item {item}')
                        results['failed'].append({
                            'folio': item.get('folio'), 
                            'status': response.status_code,
                            'error_msg':error_message
                            })
                        
            except Exception as e:
                error_message = f"Exception during batch create: {str(e)}"
              
                # Mark all records in the batch as failed
                for item in batch_payload:
                        error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        print(f'item {item}')
                        results['failed'].append({
                            'folio': item.get('folio'), 
                            'status': None,
                            'error_msg':error_message
                            })
                  

        return results

    def update_lote_hash(self):
        pass