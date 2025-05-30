import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz


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
                    "emp": "1",
                    "emp_div": "1",
                    "can": record['details'].get('cantidad'),
                    "pre": record['details'].get('precio'),
                    "fch": record['details'].get('fecha'),
                    "art": record.get('ref'),
                    "vta_fac": record.get('parent_id'),
                    "vta_fac_num_lin": i+1,
                    "und_med":1
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
                    'hash_detail': record.get('detail_hash')
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
                                record_result['id'] = record_id
                                print(f"Extracted ID: {record_id}")
                        
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

    def req_post(self, records):
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
                    "emp": "1",
                    "emp_div": "1",
                    "can": record.get('cantidad'),
                    "pre": record.get('precio'),
                    "fch": record.get('fecha'),
                    "art": record.get('ref'),
                    "vta_fac": record.get('id'),
                    "vta_fac_num_lin": i+1,
                    "und_med":1
                }
                
                # Convert payload to JSON
                post_data = json.dumps(single_payload)
                post_url = f"{base_url}?api_key={api_key}"
                
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
                    'hash_detail': record.get('detail_hash')
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
                                record_result['id'] = record_id
                                print(f"Extracted ID: {record_id}")
                        
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
