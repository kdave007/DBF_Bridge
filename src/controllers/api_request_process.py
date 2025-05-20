import json
import os
import sys
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, date
import logging


class APIRequestProcess:
    def __init__(self, base_url: str = "http://localhost:3000/api/data", api_key: str = None):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-process-json" : "true"
        }
        
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
            
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("api_requests.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("APIRequestProcess")

    def execute_actions(self, api_operations):
        """Execute all API operations (update, delete, create)"""
        update_list = api_operations.get('update', [])
        delete_list = api_operations.get('delete', [])
        add_list = api_operations.get('create', [])
        
        self.logger.info(f"Processing {len(update_list)} updates, {len(delete_list)} deletes, {len(add_list)} creates")
        
        # Execute operations
        update_results = self.update(update_list)
        delete_results = self.delete(delete_list)
        add_results = self.add(add_list)
        
        # Return summary of results
        return {
            "update": update_results,
            "delete": delete_results,
            "create": add_results,
            "total_success": sum(r.get("success", False) for r in update_results + delete_results + add_results),
            "total_failed": sum(not r.get("success", False) for r in update_results + delete_results + add_results)
        }

    def update(self, update_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Update existing records in the API"""
        results = []
        
        for element in update_list:
            try:
                # Extract data from the element
                folio = element.get("folio")
                dbf_record = element.get("dbf_record", {})
                sql_record = element.get("sql_record", {})
                
                # Prepare the payload for the API
                payload = {
                    "id": sql_record.get("id"),
                    "folio": folio,
                    "cabecera": dbf_record.get("Cabecera"),
                    "cliente": dbf_record.get("cliente"),
                    "empleado": dbf_record.get("empleado"),
                    "fecha": dbf_record.get("fecha"),
                    "total_bruto": dbf_record.get("total_bruto"),
                    "detalles": dbf_record.get("detalles", [])
                }
                
                # Make the API request
                url = f"{self.base_url}/ventas/{sql_record.get('id')}"
                self.logger.info(f"Updating record with folio {folio}")
                response = requests.put(url, headers=self.headers, json=payload)
                
                # Check if the request was successful
                if response.status_code in [200, 201, 204]:
                    self.logger.info(f"Successfully updated record with folio {folio}")
                    results.append({
                        "folio": folio,
                        "success": True,
                        "status_code": response.status_code,
                        "response": response.json() if response.content else None
                    })
                else:
                    self.logger.error(f"Failed to update record with folio {folio}. Status code: {response.status_code}")
                    results.append({
                        "folio": folio,
                        "success": False,
                        "status_code": response.status_code,
                        "error": response.text
                    })
            except Exception as e:
                self.logger.error(f"Exception while updating record: {str(e)}")
                results.append({
                    "folio": element.get("folio", "unknown"),
                    "success": False,
                    "error": str(e)
                })
        
        return results

    def delete(self, delete_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Delete records from the API"""
        results = []
        
        for element in delete_list:
            try:
                # Extract data from the element
                folio = element.get("folio")
                sql_record = element.get("sql_record", {})
                
                # Make the API request
                url = f"{self.base_url}/ventas/{sql_record.get('id')}"
                self.logger.info(f"Deleting record with folio {folio}")
                response = requests.delete(url, headers=self.headers)
                
                # Check if the request was successful
                if response.status_code in [200, 202, 204]:
                    self.logger.info(f"Successfully deleted record with folio {folio}")
                    results.append({
                        "folio": folio,
                        "success": True,
                        "status_code": response.status_code
                    })
                else:
                    self.logger.error(f"Failed to delete record with folio {folio}. Status code: {response.status_code}")
                    results.append({
                        "folio": folio,
                        "success": False,
                        "status_code": response.status_code,
                        "error": response.text
                    })
            except Exception as e:
                self.logger.error(f"Exception while deleting record: {str(e)}")
                results.append({
                    "folio": element.get("folio", "unknown"),
                    "success": False,
                    "error": str(e)
                })
        
        return results

    def add(self, add_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create new records in the API"""
        results = []
        
        for element in add_list:
            try:
                # Extract data from the element
                folio = element.get("folio")
                dbf_record = element.get("dbf_record", {})
                
                # Prepare the payload for the API
                payload = {
                    "folio": folio,
                    "cabecera": dbf_record.get("Cabecera"),
                    "cliente": dbf_record.get("cliente"),
                    "empleado": dbf_record.get("empleado"),
                    "fecha": dbf_record.get("fecha"),
                    "total_bruto": dbf_record.get("total_bruto"),
                    "detalles": dbf_record.get("detalles", [])
                }
                
                # Make the API request
                url = f"{self.base_url}/ventas"
                self.logger.info(f"Creating new record with folio {folio}")
                response = requests.post(url, headers=self.headers, json=payload)
                
                # Check if the request was successful
                if response.status_code in [200, 201, 204]:
                    self.logger.info(f"Successfully created record with folio {folio}")
                    results.append({
                        "folio": folio,
                        "success": True,
                        "status_code": response.status_code,
                        "response": response.json() if response.content else None
                    })
                else:
                    self.logger.error(f"Failed to create record with folio {folio}. Status code: {response.status_code}")
                    results.append({
                        "folio": folio,
                        "success": False,
                        "status_code": response.status_code,
                        "error": response.text
                    })
            except Exception as e:
                self.logger.error(f"Exception while creating record: {str(e)}")
                results.append({
                    "folio": element.get("folio", "unknown"),
                    "success": False,
                    "error": str(e)
                })
        
        return results

    def update_lote_hash(self):
        pass