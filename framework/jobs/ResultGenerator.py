import json
import pytz
import copy
from datetime import datetime
from framework.utils.LoggerUtils import LoggerUtils

logger = LoggerUtils.logger

class ResultGenerator(object):  
    @staticmethod
    def get_ist_run_date_time():
        ist_timezone = pytz.timezone("Asia/Kolkata")
        
        current_ist = datetime.now(ist_timezone)

        run_date = str(current_ist.date())  # Only the date (YYYY-MM-DD)
        run_time = current_ist.strftime("%Y-%m-%d %H:%M:%S")  # Time in DD-MM-YYYY HH24:MM:SS format

        return run_date, run_time

    
    @staticmethod
    def get_flag_value(src_res_main, dest_res_main):
        src_res = copy.deepcopy(src_res_main)
        dest_res = copy.deepcopy(dest_res_main)
        if isinstance(src_res, dict):
            src_res = src_res.get("result", src_res)
        if isinstance(dest_res, dict):
            dest_res = dest_res.get("result", dest_res)

        if isinstance(src_res, list) and 'chk_id' in src_res[0]:
            src_res = [{'result': chk['result']} for chk in src_res]
        if isinstance(dest_res, list) and 'chk_id' in dest_res[0]:
            dest_res = [{'result': chk['result']} for chk in dest_res]

        logger.info(f"Calculating flag value for src details: {src_res}, dest details: {dest_res} ")
        if src_res is None and dest_res is None:
            return "test was not run for both tables"
        elif src_res is None or dest_res is None:
            return "test for one table was not run, probably because missing config"
        elif src_res == -1 and dest_res == -1:
            return "test failed for both tables"
        elif src_res == -1 or dest_res == -1:
            return "test failed for one of the tables"
        elif src_res == dest_res:
            return "Y"
        else:
            return "N"

    @staticmethod
    def get_result(res):
        result = copy.deepcopy(res)
        filtered_result = {key: value for key, value in result.items() if 'flg' in key}
        eligible_flags_count = sum(1 for value in filtered_result.values() if value != "test was not run for both tables")
        succeded_flags_count = sum(1 for value in filtered_result.values() if value == "Y")

        rslt = "NONE"
        if succeded_flags_count == 0:
            rslt = "ALL_MISMATCH"
        elif succeded_flags_count != eligible_flags_count:
            rslt = "PARTIAL_MATCH"
        elif succeded_flags_count == eligible_flags_count:
            rslt = "ALL_MATCH"
        
        rslt_prcntg = round(succeded_flags_count/eligible_flags_count, 2) * 100

        return rslt, rslt_prcntg

    @staticmethod
    def get_details(res1, res2):
        dtls = {}
        dtls["db"] = res1["db_name"]
        dtls["schema"] = res1["schema"]
        dtls["name"] = res1["name"]

        dtls["count"] = res1["getTotalCount"] if res1["getTotalCount"] is not None else ""
        
        dtls["incrementalCount"] = res1["getIncrementalCount"] if res1["getIncrementalCount"] is not None else ""
        
        ddl_res = ResultGenerator.compare_ddl_details(res1["getDDL"].get("result", res1["getDDL"]), res2["getDDL"].get("result", res2["getDDL"]))
        
        dtls["columnCount"] = len(res1["getDDL"].get("result", res1["getDDL"])) if res1["getDDL"].get("result", res1["getDDL"]) is not None else ""

        dtls["fun_rcon"] = res1["fun_rcon"] if res1["fun_rcon"] is not None else ""

        dtls["columnType"] = ddl_res

        return dtls
    
    @staticmethod
    def compare_ddl_details(list1, list2):
        print("hi, typeof lists", type(list1), type(list2))

        ddl_labelled_list = []
        
        for item1 in list1:
            matched = False
            for item2 in list2:
                if item1[0] == item2[0]:
                    matched = True
                    if item1 == item2:
                        ddl_labelled_list.append(item1 + ["match"])
                    else:
                        ddl_labelled_list.append(item1 + ["mismatch"])
                        break
            if not matched:
                ddl_labelled_list.append(item1 + ["extra"])

        for item2 in list2:
            matched = False
            for item1 in list1:
                if item2[0] == item1[0]:
                    matched = True
            if not matched:
                ddl_labelled_list.append(item2 + ["missing"])
        
        ddl_details = {
            "extraColumns": [],
            "missingColumns": [],
            "mismatchedColumns": [],
            "matchedColumns": []
        }
        for item in ddl_labelled_list:
            ddl_labelled_dict_item = {
                "column_name": item[0],
                "data_type": item[1],
                "is_nullable": item[2],
            }

            if item[-1] == "extra":
                ddl_details["extraColumns"].append(ddl_labelled_dict_item)
            elif item[-1] == "missing":
                ddl_details["missingColumns"].append(ddl_labelled_dict_item)
            elif item[-1] == "mismatch":
                ddl_details["mismatchedColumns"].append(ddl_labelled_dict_item)
            elif item[-1] == "match":
                ddl_details["matchedColumns"].append(ddl_labelled_dict_item["column_name"])
        return ddl_details
    
    
    @staticmethod
    def reduce_ddl(src_ddl, dest_ddl, audit_columns):
        if audit_columns is None or (isinstance(audit_columns, str) and audit_columns.strip() == "") or isinstance(audit_columns,str) == False:
            logger.warning("audit columns value is found invalid or empty. src and dest DDL won't be modified to remove any audit columns. Please set comma separated string of required columns in the variable named audit_columns")

        audit_columns_set = {col.strip().upper() for col in audit_columns.split(",")}

        filtered_src_ddl = [col_details for col_details in src_ddl if col_details[0].upper() not in audit_columns_set]
        filtered_dest_ddl = [col_details for col_details in dest_ddl if col_details[0].upper() not in audit_columns_set]

        sorted_src_ddl = sorted(filtered_src_ddl, key=lambda x: x[0].upper())
        sorted_dest_ddl = sorted(filtered_dest_ddl, key=lambda x: x[0].upper())

        src_ddl = sorted_src_ddl
        dest_ddl = sorted_dest_ddl


# "{
# ""db"":""db1"",
# ""schema"":""s1"",
# ""table"":""t1"",
# ""count"": ""324243234"",
# ""inr_cnt"": ""34324"",
# ""col_cnt"": ""2"",
# ""col_typ"": {
#     ""col1"": ""String"",
#     ""col2"": ""Data""
#     },
# ""fun_rcon"":{
#  ""col_nm"":"""",
#  ""agg_typ"":"""",
#  ""qry"":"""",
#  ""rslt"":""""
# }
# }"


    @staticmethod
    def create_result(func_list, src_res, dest_res):
        result = {}
        # logger.info("comparing")
        # comp_report = DataComparator(details)
        # storing the report in S3
        # logger.info(comp_report, dag_run_id)

        run_date, run_time = ResultGenerator.get_ist_run_date_time()
        result['run_date'] = run_date
        result['run_time'] = run_time
        
        result['src_db'] = src_res['db_name'].upper()
        result['src_sch'] = src_res['schema'].upper()
        result['src_tbl'] = src_res['name'].upper()
        result['src_typ'] = src_res['engine'].upper()
        result['trg_db'] = dest_res['db_name'].upper()
        result['trg_sch'] = dest_res['schema'].upper()
        result['trg_tbl'] = dest_res['name'].upper()
        result['trg_typ'] = dest_res['engine'].upper()
        
        result['cnt_flg'] = ResultGenerator.get_flag_value(src_res['getTotalCount'], dest_res['getTotalCount'])
        result['incr_cnt_flg'] = ResultGenerator.get_flag_value(src_res['getIncrementalCount'], dest_res['getIncrementalCount'])

        audit_columns = src_res.get('audit_columns', None)
        ResultGenerator.reduce_ddl(src_res['getDDL'], dest_res['getDDL'], audit_columns)

        result['col_typ_flg'] = ResultGenerator.get_flag_value(src_res['getDDL'], dest_res['getDDL'])
        result['col_cnt_flg'] = result['col_typ_flg'] if result['col_typ_flg'] != "N" else ResultGenerator.get_flag_value(len(src_res['getDDL'].get('result', src_res['getDDL'])), len(dest_res['getDDL'].get('result', dest_res['getDDL'])))

        result['fun_rcon_flg'] = ResultGenerator.get_flag_value(src_res['fun_rcon'], dest_res['fun_rcon'])
        # result['cmp_flg'] = None
        # result['cmg_rslt'] = None

        rslt, rslt_prcntg = ResultGenerator.get_result(result)
        result['rslt'] = rslt
        result['rcn_prcntg'] = rslt_prcntg

        result['src_dtls'] = json.dumps(ResultGenerator.get_details(src_res, dest_res))
        result['trg_dtls'] = json.dumps(ResultGenerator.get_details(dest_res, src_res))

        return result
        


# func_list = [
#     'getTotalCount',
#     'getIncrementalCount',
#     'getPKCount',
#     'getDDL',
#     'fun_rcon',
#     'getData'
# ]
    
        
        
        
# run date
# run time
# src_db
# src_sch
# src_tbl
# src_typ
# trg_db
# trg_sch
# trg_tbl
# trg_typ
# cnt_flg
# inr_cnt_flg
# col_cnt_flg
# col_typ_flg
# fun_rcn_flg
# cmp_flg
# cmg_rslt
# src_dtls
# trg_dtls
# rslt
# rcn_prcntg
