import os
import sys
from turtle import st
from pathlib import Path
from src.db import response_tracking
from src.db.response_tracking import ResponseTracking
import requests
import json
from src.controllers.api_request_process import CustomJSONEncoder

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

    def update_db(self, responses_dict):
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
            'success': {},  # Will store folio -> result for successful operations
            'failed': {}   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(creates)} create operations in batches of {self.batch_size}")
        for i in range(0, len(creates), self.batch_size):
            batch = creates[i:i+self.batch_size]
            print(f"Processing create batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                # Prepare batch payload
                batch_payload = []
                for item in batch:
                    folio = item.get('folio')
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
                    data=json.dumps({"records": batch_payload}, cls=CustomJSONEncoder)
                )
                
                # Process batch response
                if response.status_code in [200, 201, 202, 204]:
                    # The API should return a response with status for each record
                    batch_response = response.json()
                    batch_results = batch_response.get("results", [])
                    
                    # Process each result in the batch response
                    for idx, result_data in enumerate(batch_results):
                        if idx < len(batch):  # Safety check
                            item = batch[idx]
                            folio = item.get('folio')
                            
                            # Create result object
                            result = {
                                "folio": folio,
                                "status_code": response.status_code,
                                "operation": "create",
                                "dbf_hash": item.get("dbf_hash", ""),
                            }
                            
                            # Check individual record success
                            if result_data.get("success", False):
                                result["success"] = True
                                results['success'][folio] = result
                                self.request_completed(result)
                                print(f"Successfully created record with folio {folio}")
                            else:
                                result["success"] = False
                                result["error"] = result_data.get("error", "Unknown error")
                                results['failed'][folio] = result
                                self.request_pending(result)
                                print(f"Failed to create record with folio {folio}: {result['error']}")
                else:
                    # If the batch request failed entirely
                    error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                    print(error_message)
                    
                    # Mark all records in the batch as failed
                    for item in batch:
                        folio = item.get('folio')
                        result = {
                            "folio": folio,
                            "status_code": response.status_code,
                            "operation": "create",
                            "dbf_hash": item.get("dbf_hash", ""),
                            "success": False,
                            "error": error_message
                        }
                        results['failed'][folio] = result
                        self.request_pending(result)
                        
            except Exception as e:
                error_message = f"Exception during batch create: {str(e)}"
                print(error_message)
                
                # Mark all records in the batch as failed
                for item in batch:
                    folio = item.get('folio')
                    results['failed'][folio] = {
                        "folio": folio,
                        "success": False,
                        "error": error_message,
                        "operation": "create",
                        "dbf_hash": item.get("dbf_hash", "")
                    }
                    #self.request_pending(results['failed'][folio])
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
                batch_id_map = {}  # Map to store index -> sql_id for response processing
                
                for idx, item in enumerate(batch):
                    folio = item.get('folio')
                    dbf_record = item.get('dbf_record', {})
                    sql_record = item.get('sql_record', {})
                    sql_id = sql_record.get('id')
                    
                    # Store the SQL ID for this index
                    batch_id_map[idx] = sql_id
                    
                    batch_payload.append({
                        "id": folio,
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
                    data=json.dumps({"records": batch_payload}, cls=CustomJSONEncoder)
                )
                
                # Process batch response
                if response.status_code in [200, 201, 202, 204]:
                    # The API should return a response with status for each record
                    batch_response = response.json()
                    batch_results = batch_response.get("results", [])
                    
                    # Process each result in the batch response
                    for idx, result_data in enumerate(batch_results):
                        if idx < len(batch):  # Safety check
                            item = batch[idx]
                            folio = item.get('folio')
                            
                            # Create result object
                            result = {
                                "folio": folio,
                                "status_code": response.status_code,
                                "operation": "update",
                                "dbf_hash": item.get("dbf_hash", ""),
                                
                            }
                            
                            # Check individual record success
                            if result_data.get("success", False):
                                result["success"] = True
                                results['success'][folio] = result
                                self.request_completed(result)
                                print(f"Successfully updated record with folio {folio}")
                            else:
                                result["success"] = False
                                result["error"] = result_data.get("error", "Unknown error")
                                results['failed'][folio] = result
                                self.request_pending(result)
                                print(f"Failed to update record with folio {folio}: {result['error']}")
                else:
                    # If the batch request failed entirely
                    error_message = f"Batch update failed with status {response.status_code}: {response.text}"
                    print(error_message)
                    
                    # Mark all records in the batch as failed
                    for item in batch:
                        folio = item.get('folio')
                        result = {
                            "folio": folio,
                            "status_code": response.status_code,
                            "operation": "update",
                            "dbf_hash": item.get("dbf_hash", ""),
                           
                            "success": False,
                            "error": error_message
                        }
                        results['failed'][folio] = result
                        self.request_pending(result)
                        
            except Exception as e:
                error_message = f"Exception during batch update: {str(e)}"
                print(error_message)
                
                # Mark all records in the batch as failed
                for item in batch:
                    folio = item.get('folio')
                    results['failed'][folio] = {
                        "folio": folio,
                        "success": False,
                        "error": error_message,
                        "operation": "update",
                        
                        "dbf_hash": item.get("dbf_hash", "")
                    }
                    self.request_pending(results['failed'][folio])

        return results

    def delete(self, deletes):
        results = {
            'success': {},  # Will store folio -> result for successful operations
            'failed': {}   # Will store folio -> result for failed operations
        }
        
        print(f"Processing {len(deletes)} delete operations in batches of {self.batch_size}")
        for i in range(0, len(deletes), self.batch_size):
            batch = deletes[i:i+self.batch_size]
            print(f"Processing delete batch {i//self.batch_size + 1} with {len(batch)} operations")
            
            try:
                # Prepare batch payload with IDs to delete
                batch_payload = []
                for item in batch:
                    sql_record = item.get('sql_record', {})
                    sql_id = sql_record.get('id')
                    if sql_id:
                        batch_payload.append({"id": sql_id})
                
                # Make batch DELETE request
                response = requests.delete(
                    f"{self.base_url}", 
                    headers=self.headers,
                    data=json.dumps({"records": batch_payload}, cls=self.json_encoder)
                )
                
                # Process batch response
                if response.status_code in [200, 201, 202, 204]:
                    # The API should return a response with status for each record
                    batch_response = response.json()
                    batch_results = batch_response.get("results", [])
                    
                    # Process each result in the batch response
                    for idx, result_data in enumerate(batch_results):
                        if idx < len(batch):  # Safety check
                            item = batch[idx]
                            folio = item.get('folio')
                            
                            # Create result object
                            result = {
                                "folio": folio,
                                "status_code": response.status_code,
                                "operation": "delete",
                                "sql_hash": item.get("sql_hash", "")
                            }
                            
                            # Check individual record success
                            if result_data.get("success", False):
                                result["success"] = True
                                results['success'][folio] = result
                                self.request_completed(result)
                                print(f"Successfully deleted record with folio {folio}")
                            else:
                                result["success"] = False
                                result["error"] = result_data.get("error", "Unknown error")
                                results['failed'][folio] = result
                                self.request_pending(result)
                                print(f"Failed to delete record with folio {folio}: {result['error']}")
                else:
                    # If the batch request failed entirely
                    error_message = f"Batch delete failed with status {response.status_code}: {response.text}"
                    print(error_message)
                    
                    # Mark all records in the batch as failed
                    for item in batch:
                        folio = item.get('folio')
                        result = {
                            "folio": folio,
                            "status_code": response.status_code,
                            "operation": "delete",
                            "sql_hash": item.get("sql_hash", ""),
                            "success": False,
                            "error": error_message
                        }
                        results['failed'][folio] = result
                        self.request_pending(result)
                        
            except Exception as e:
                error_message = f"Exception during batch delete: {str(e)}"
                print(error_message)
                
                # Mark all records in the batch as failed
                for item in batch:
                    folio = item.get('folio')
                    results['failed'][folio] = {
                        "folio": folio,
                        "success": False,
                        "error": error_message,
                        "operation": "delete",
                        "sql_hash": item.get("sql_hash", "")
                    }
                    self.request_pending(results['failed'][folio])

        return results

    def update_lote_hash(self):
        pass