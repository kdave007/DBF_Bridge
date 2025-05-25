

from .find_matches_process import MatchesProcess
from .api_response_tracking import APIResponseTracking
from .send_request import SendRequest
from .details_controller import DetailsController
from datetime import date

class WorkFlow:
    def start(self, config):

         # Let's try with the exact date from your screenshot: 20/03/2025
        start_date = date(2025, 4, 29)  # March 20, 2025
        end_date = date(2025, 4, 30)  # March 20, 2025


        self.matches_process = MatchesProcess()
        result = self.matches_process.compare_data(config, start_date, end_date)
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
        if result:
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

            send_request = SendRequest()
            requests_results = send_request.send(result['api_operations'])

            api_tracker = APIResponseTracking()
            api_tracker.update_tracker(requests_results)

            details_controller = DetailsController()
            details_controller.process(requests_results, start_date, end_date)
            
            

        return  result