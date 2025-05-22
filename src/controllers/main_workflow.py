
from .find_matches_process import MatchesProcess
from .api_request_process import APIRequestProcess
from .after_response_process import AfterResponseProcess

class WorkFlow:
    def start(self, config):

        self.matches_process = MatchesProcess()
        result = self.matches_process.compare_batches(config)
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
        if result.get('detailed_comparison'):
            #print(f' api actions /////// { result['detailed_comparison']['api_operations']} //////////////////////////')
            api = APIRequestProcess()
            result = api.execute_actions(result['detailed_comparison']['api_operations'])
            # {
            #     "update": update_results,
            #     "delete": delete_results,
            #     "create": add_results,
            #     "total_success": sum(r.get("success", False) for r in update_results + delete_results + add_results),
            #     "total_failed": sum(not r.get("success", False) for r in update_results + delete_results + add_results)
            # }


            
            after_response = AfterResponseProcess()
            after_response.update_db(result)
            

        return  result