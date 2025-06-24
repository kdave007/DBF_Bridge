import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz
import os
import sys


class SendDetails:
    def __init__(self) -> None:
        pass
    
    def req_update(self, records):
        """
        Post records one by one to the API endpoint
        
        Args:
            records: List of records to post
            
        Returns:
            Dictionary with counts of processed records and their status
        """
        import requests
        import json
        
        # API configuration
        base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/mov_g"
        api_key = "123456"
        
        # Set headers for API requests
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-process-json": "true"
        }
        
        # Track results
        result_counts = {
            'total': len(records),
            'success': 0,
            'failed': 0,
            'records': []
        }
        
        print(f"\n=== Posting {len(records)} records one by one ===\n")
        
        # Process each record individually
        for i, record in enumerate(records):
            try:
                # Prepare the payload for posting based on the record data
                # Map the record fields to the expected payload structure
                single_payload = {
                    "id": str(record.get('sql_id')),
                     # "emp": str(record.get('emp')),
                    "emp_div": str(record.get('emp_div')),
                    "can_und": record.get('cantidad'),
                    "por_dto": record.get('descuento'),
                    "pre": record.get('precio'),
                    "fch": record.get('fecha'),
                    "art": record.get('art'),
                    "vta_fac": record.get('parent_id'),
                    "vta_fac_num_lin": i+1,
                    "und_med":1,
                    "hor":self._format_hour_to_12h(record.get('hor')),
                    "reg_iva_vta":record.get('reg_iva_vta'),
                    "mov_tip":record.get('mov_tip'),
                    "ser_vta":str(record.get('ser_vta')),
                    "alm":str(record.get('alm'))
                }
                
                # Convert payload to JSON
                post_data = json.dumps(single_payload)
                post_url = f"{base_url}/{single_payload.get('id')}?api_key={api_key}"
                
                print(f"\n[{i+1}/{len(records)}] Posting record for folio: {record.get('folio')}")
                print(f"URL: {post_url}")
                print(f"Payload: {post_data}")
                
                # Send the POST request
                response = requests.post(post_url, data=post_data, headers=headers)
                
                # Process the response
                status_code = response.status_code
                print(f"Response Status: {status_code}")
                print(f'original record : {record}')
                
                record_result = {
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    'status_code': status_code,
                    "fecha": record.get('fecha'),
                    'success': False,
                    'hash_detail': record.get('detail_hash'),
                    'detail_id':record.get('sql_id')
                }
                
                # Check if the request was successful
                if status_code in [200, 201, 202, 204]:
                    try:
                        response_json = response.json()
                        print(f"Response: {json.dumps(response_json, indent=2)}")
                        
                        # Extract ID from 'mov_g' key if it exists
                        if 'mov_g' in response_json and isinstance(response_json['mov_g'], list) and len(response_json['mov_g']) > 0:
                            record_id = response_json['mov_g'][0].get('id')
                            if record_id:
                                record_result['detail_id'] = record_id
                                print(f"Extracted detail update ID: {record_id}")

                                # self.send_update_fac_off(record.get('parent_id'), self._format_date_to_iso(record.get("fecha"))) 

                        record_result['success'] = True
                        result_counts['success'] += 1
                    except ValueError:
                        print(f"Response (not JSON): {response.text}")
                        record_result['response'] = response.text
                        record_result['success'] = True
                        result_counts['success'] += 1
                else:
                    print(f"Failed with status {status_code}: {response.text}")
                    record_result['error'] = response.text
                    result_counts['failed'] += 1

              
                # Add the record result to the tracking
                result_counts['records'].append(record_result)
                
            except Exception as e:
                print(f"Exception while posting record: {str(e)}")
                result_counts['failed'] += 1
                result_counts['records'].append({
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    "fecha": record.get('fecha'),
                    'success': False,
                    'error': str(e)
                })
        
        # Print summary
        print("\n=== Post All Summary ===")
        print(f"Total records: {result_counts['total']}")
        print(f"Successful: {result_counts['success']}")
        print(f"Failed: {result_counts['failed']}")
        print("========================\n")

        
        
        return result_counts

    def req_post(self, records, parent_ref):
        """
        Post records one by one to the API endpoint
        
        Args:
            records: List of records to post
            
        Returns:
            Dictionary with counts of processed records and their status
        """
        import requests
        import json
        
        # API configuration
        base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/mov_g"
        api_key = "123456"
        
        # Set headers for API requests
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-process-json": "true"
        }

        print(records)
        
        # Track results
        result_counts = {
            'total': len(records),
            'success': 0,
            'failed': 0,
            'records': []
        }
        
        print(f"\n=== Posting {len(records)} records one by one ===\n")
        
        # Process each record individually
        for i, record in enumerate(records):
            print(f' record detail to post is {record}')
            try:
                # Prepare the payload for posting based on the record data
                # Map the record fields to the expected payload structure
                single_payload = {
                    "alm":str(record.get('alm')),
                    "art": record.get('art'),
                    "und_med":1,
                    "can_und": record.get('cantidad'),
                    "can":record.get('cantidad'),
                    "emp_div": str(record.get('emp_div')),
                    "emp": str(record.get('emp')),
                    "fch": self._format_date_to_iso(parent_ref.get("fecha")),
                    "hor":self._format_hour_to_12h(record.get('hor')),
                    "pre": float(record.get('imp_part', 0)) + float(record.get('iva_part', 0)),
                    "por_dto": record.get('descuento'),
                    "reg_iva_vta":record.get('reg_iva_vta'),
                    "vta_fac": parent_ref.get('parent_id'),
                    "clt":record.get('clt'),
                    "mov_tip":record.get('mov_tip'),
                    "cal_arr":1
                   
                }
                
                # Convert payload to JSON
                post_data = json.dumps(single_payload)
                post_url = f"{base_url}?api_key={api_key}"
                
                print(f"\n[{i+1}/{len(records)}] Posting record for folio: {record.get('folio')}")
                print(f"URL: {post_url}")
                print(f"Payload: {post_data}")
                
                # print("STOP")
                # sys.exit()
            
                
               
                # Send the POST request
                response = requests.post(post_url, data=post_data, headers=headers)
                
                # Process the response
                status_code = response.status_code
                print(f"Response Status: {status_code}")
                print(f'original record : {record}')
                
                record_result = {
                    'folio': record.get('Folio'),
                    'ref': record.get('REF'),
                    'status_code': status_code,
                    "fecha": self._format_date_to_iso(parent_ref.get("fecha")),
                    'success': False,
                    'hash_detail': record.get('detail_hash')
                }
                
                # Check if the request was successful
                if status_code in [200, 201, 202, 204]:
                    try:
                        response_json = response.json()
                        print(f"Response: {json.dumps(response_json, indent=2)}")
                        
                        # Extract ID from 'mov_g' key if it exists
                        # if 'mov_g' in response_json and isinstance(response_json['mov_g'], list) and len(response_json['mov_g']) > 0:
                        if 'mov_g' in response_json: 
                            record_id = response_json['mov_g'][0].get('id')
                            if record_id:
                                record_result['detail_id'] = record_id
                                record_result['parent_id'] = parent_ref.get('parent_id')
                                print(f"Extracted detail ID: {record_id}")

                                # self.send_update_fac_off(record.get('parent_id'), str(record.get('emp')), str(record.get('emp_div')) ) 
                                record_result['success'] = True
                                result_counts['success'] += 1
                        
                       
                    except ValueError:
                        print(f"Response (not JSON): {response.text}")
                        record_result['response'] = response.text
                        result_counts['failed'] += 1
                else:
                    print(f"Failed with status {status_code}: {response.text}")
                    record_result['error'] = response.text
                    result_counts['failed'] += 1

           
                # Add the record result to the tracking
                result_counts['records'].append(record_result)
                
            except Exception as e:
                print(f"Exception while posting record: {str(e)}")
                result_counts['failed'] += 1
                result_counts['records'].append({
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    "fecha": record.get('fecha'),
                    'success': False,
                    'error': str(e)
                })
        
        # Print summary
        print("\n=== Post All Summary ===")
        print(f"Total records: {result_counts['total']}")
        print(f"Successful: {result_counts['success']}")
        print(f"Failed: {result_counts['failed']}")
        print("========================\n")
         
        return result_counts


    def delete_post(self, records):
        """
        Delete records one by one from the API endpoint
        
        Args:
            records: List of records to delete
            
        Returns:
            Dictionary with counts of processed records and their status
        """
        import requests
        import json
        
        # API configuration
        base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/mov_g"
        api_key = "123456"
        
        # Set headers for API requests
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-process-json": "true"
        }
        
        # Track results
        result_counts = {
            'total': len(records),
            'success': 0,
            'failed': 0,
            'records': []
        }
        
        print(f"\n=== Deleting {len(records)} records one by one ===\n")
        
        # Process each record individually
        for i, record in enumerate(records):
            try:
                # Get the record ID for deletion
                record_id = record.get('id')
                
                if not record_id:
                    print(f"Skipping record without ID: {record}")
                    result_counts['failed'] += 1
                    result_counts['records'].append({
                        'folio': record.get('folio'),
                        'ref': record.get('ref'),
                        'status_code': 400,
                        "fecha": record.get('fecha'),
                        'success': False,
                        'error': "Missing record ID for deletion"
                    })
                    continue
                
                # Construct the delete URL with the record ID
                delete_url = f"{base_url}/{record_id}?api_key={api_key}"
                
                print(f"\n[{i+1}/{len(records)}] Deleting record for folio: {record.get('folio')}")
                print(f"URL: {delete_url}")
                
                # Send the DELETE request
                response = requests.delete(delete_url, headers=headers)
                
                # Process the response
                status_code = response.status_code
                print(f"Response Status: {status_code}")
                print(f'Original record: {record}')
                
                record_result = {
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    'status_code': status_code,
                    "fecha": record.get('fecha'),
                    'success': False,
                    'id': record_id
                }
                
                # Check if the request was successful
                if status_code in [200, 201, 202, 204]:
                    try:
                        # Try to parse JSON response if available
                        try:
                            response_json = response.json()
                            print(f"Response: {json.dumps(response_json, indent=2)}")
                        except ValueError:
                            # Not a JSON response
                            print(f"Response (not JSON): {response.text}")
                            record_result['response'] = response.text
                        
                        record_result['success'] = True
                        result_counts['success'] += 1
                    except Exception as parse_error:
                        # Handle any other parsing errors
                        print(f"Error parsing response: {str(parse_error)}")
                        record_result['response'] = response.text
                        record_result['success'] = True  # Still consider it successful if status code was good
                        result_counts['success'] += 1
                else:
                    print(f"Failed with status {status_code}: {response.text}")
                    record_result['error'] = response.text
                    result_counts['failed'] += 1
                
                # Add the record result to the tracking
                result_counts['records'].append(record_result)
                
            except Exception as e:
                print(f"Exception while deleting record: {str(e)}")
                result_counts['failed'] += 1
                result_counts['records'].append({
                    'folio': record.get('folio'),
                    'ref': record.get('ref'),
                    "fecha": record.get('fecha'),
                    'success': False,
                    'error': str(e)
                })
        
        # Print summary
        print("\n=== Delete Summary ====")
        print(f"Total records: {result_counts['total']}")
        print(f"Successful: {result_counts['success']}")
        print(f"Failed: {result_counts['failed']}")
        print("========================\n")
        
        return result_counts

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


    def send_update_fac_off(self, id, emp, emp_div):
        """
        Send a request to set the 'off' field to 0 for a specific record
        
        Args:
            id: The ID of the record to update
            
        Returns:
            dict: Result of the operation with success status and response details
        """
        import requests
        import json
        
        try:
            # API configuration
            base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/vta_fac_g"
            api_key = "123456"
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "x-process-json": "true"
            }

            # Prepare payload
            post_data = json.dumps({
                "off": 0,
                "emp": emp,
                "emp_div": emp_div,
                "fpg": 20
            })
            
            # Construct URL with the ID
            post_url = f"{base_url}/{id}?api_key={api_key}"
            
            print(f"Sending off=0 update for ID: {id}")
            print(f"URL: {post_url}")
            print(f"Payload: {post_data}")
            
            # Send the POST request
            response = requests.post(post_url, data=post_data, headers=headers)
            
            # Process response
            if response.status_code in [200, 201, 202, 204]:
                try:
                    # Parse the JSON response
                    response_json = response.json()
                    print(f"Response JSON: {json.dumps(response_json, indent=2)}")
                    
                    # Check if the response contains the expected structure and the same ID
                    if ('vta_fac_g' in response_json and 
                        isinstance(response_json['vta_fac_g'], list) and 
                        len(response_json['vta_fac_g']) > 0 and 
                        'id' in response_json['vta_fac_g'][0] and 
                        str(response_json['vta_fac_g'][0]['id']) == str(id)):
                        
                        print(f"Successfully validated ID {id} in response")
                        return {
                            "success": True,
                            "id": id,
                            "status_code": response.status_code,
                            "response": response.text
                        }
                    else:
                        print(f"ID validation failed for ID {id}. Response does not contain matching ID.")
                        return {
                            "success": False,
                            "id": id,
                            "status_code": response.status_code,
                            "error": "Response does not contain matching ID",
                            "response": response.text
                        }
                except Exception as e:
                    print(f"Error validating response for ID {id}: {str(e)}")
                    return {
                        "success": False,
                        "id": id,
                        "status_code": response.status_code,
                        "error": f"Error validating response: {str(e)}",
                        "response": response.text
                    }
            else:
                print(f"Failed to update off field for ID {id}. Status: {response.status_code}")
                return {
                    "success": False,
                    "id": id,
                    "status_code": response.status_code,
                    "error": response.text
                }
                
        except Exception as e:
            error_message = f"Exception updating off field for ID {id}: {str(e)}"
            print(error_message)
            return {
                "success": False,
                "id": id,
                "error": error_message
            }

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