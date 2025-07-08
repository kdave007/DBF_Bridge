
from .find_matches_process import MatchesProcess
from .api_response_tracking import APIResponseTracking
from .send_request import SendRequest
from .details_controller import DetailsController
from .op import OP
from datetime import date
import os
import sys
import time

class WorkFlow:
    def start(self, config, start_date, end_date):

        print("\n" + "="*80)
        print("====>  Procesando cabeceras de facturas de venta a ERP  <====")
        print("="*80)
        time.sleep(4)

        self.matches_process = MatchesProcess()
        result = self.matches_process.compare_data(config, start_date, end_date)
      
        print(f' MAIN W Result {result}')

        print("\n" + "="*80)
        print("====>  Procesando recibos de cobro  de venta a ERP  <====")
        print("="*80)
        time.sleep(4)

        # print("STOP")
        # sys.exit()

        if result:
            #{
            # #     "update": update_results,
            # #     "delete": delete_results,
            # #     "create": add_results,
            # #     "total_success": sum(r.get("success", False) for r in update_results + delete_results + add_results),
            # #     "total_failed": sum(not r.get("success", False) for r in update_results + delete_results + add_results)
            # # }
            op = OP()
            op.execute(result['api_operations'])


        # print("STOP")
        # sys.exit()

        
        # sample = {
        #     "matched": is_match,
        #     "dbf_hash": dbf_hash,
        #     "sql_hash": sql_hash,
        #     "lote_id": lote_record.get('lote'),
        #     "fecha_referencia": lote_record.get('fecha_referencia'),
        #     "detailed_comparison":{
        #         "status": "completed",
        #         "total_dbf_records": len(dbf_records_by_folio),
        #         "total_sql_records": len(sql_records_by_folio),
        #         "api_operations": api_operations,
        #         "summary": {
        #             "matching_count": len(matching),
        #             "create_count": len(in_dbf_only),
        #             "update_count": len(mismatched),
        #             "delete_count": len(in_sql_only),
        #             "total_actions_needed": len(in_dbf_only) + len(mismatched) + len(in_sql_only)
        #          }
        #     }
        # }
        # if result:
        #     pass
            #print(f' api actions /////// { result['detailed_comparison']['api_operations']} //////////////////////////')
            # api = APIRequestProcess()
            # result = api.execute_actions(result['api_operations'])
            # # {
            # #     "update": update_results,
            # #     "delete": delete_results,
            # #     "create": add_results,
            # #     "total_success": sum(r.get("success", False) for r in update_results + delete_results + add_results),
            # #     "total_failed": sum(not r.get("success", False) for r in update_results + delete_results + add_results)
            # # }

            # send_request = SendRequest()
            # requests_results = send_request.send(result['api_operations'])

            # api_tracker = APIResponseTracking()
            # execution_done = api_tracker.update_tracker(requests_results)

            
            # if execution_done:
            #     #if we updated or posted anything, means we did actions, so check details, if not, continue
            #     print('Checking details actions...')
            #     details_controller = DetailsController()
            #     details_controller.process(requests_results, start_date, end_date)
            # else:
            #     pass
            #     #check anyways the details
            

        return  result 