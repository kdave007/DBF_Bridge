import os
import sys
from turtle import st
from pathlib import Path
from db import response_tracking
from src.db.response_tracking import ResponseTracking


class AfterResponseProcess:
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
        # responses_dict contains API operation results (update, delete, create)
        if not responses_dict:
            return False
            
        # Check if all operations were successful
        total_success = responses_dict.get('total_success', 0)
        total_failed = responses_dict.get('total_failed', 0)
        
        if total_failed == 0 and total_success > 0:
            # All operations succeeded
            return True
            
        # Log failed operations if any
        for op_type in ['update', 'delete', 'create']:
            for result in responses_dict.get(op_type, []):
                if not result.get('success'):
                    print(f"Failed {op_type} operation for folio {result.get('folio')}")
                    self.request_pending(result)
                else:
                    self.request_completed(result)
                    
        return False

   

    def request_completed(self, request_response):
        print(f' completed : {request_response.get("folio")}')
        self.response_tracking.update_status()

    def request_pending(self, request_response):
        print(f' pending : {request_response.get("folio")}')
        self.response_tracking.update_status()

    def update_lote_hash(self):
        pass