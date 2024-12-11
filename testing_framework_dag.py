from airflow import DAG
import copy
from datetime import datetime
# from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
# import time
from framework.main import test_handler
# from framework.jobs.DataComparator import DataComparator
from framework.jobs.ResultGenerator import ResultGenerator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Variable
# import pandas as pd
from airflow.models import XCom
from framework.utils.LoggerUtils import LoggerUtils
import json


args = { "provide_context":True, 'retries':1 }
logger = LoggerUtils.logger

func_list = [
    'getTotalCount',
    'getIncrementalCount',
    'getDDL',
    'fun_rcon',
]

report_config_key = "recon_testing_results_table_config"
json_config = {}

try:
    json_config = json.loads(Variable.get("json_config_recon"))
except AirflowNotFoundException:
    logger.error("Error in finding Table config for Test Results Storage. Variable named 'recon_testing_results_table_config' could not found.") 



def dag_start_task(**kwargs):
    dag_run = kwargs.get('dag_run')
    # config = dag_run.conf if dag_run else None
    # if not config:
    #     logger.error("DAG run config is null. Further tasks can't be run.")
    #     dag_run.state = State.FAILED
    
    # json_config = config
    if json_config is None:
        dag_run.state = State.FAILED
    try:
        tmp = json.loads(Variable.get(report_config_key))
    except AirflowNotFoundException:
        logger.error("Error in finding Table config for Test Results Storage. Variable named 'recon_testing_results_table_config' could not found.") 
        dag_run.state = State.FAILED



@provide_session
def dag_end_task(session=None, **kwargs):
    logger.info("Testing DAG has ended.")
    dag_id = kwargs['dag'].dag_id
    run_id = kwargs['run_id']
    
    session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.run_id == run_id
    ).delete()
    session.commit()
    return

def count_check(**kwargs):
    details = kwargs['details']
    func_name = 'getTotalCount'
    xcom_keyword = details['keyword'] + "--" + func_name
    
    total_count_res = test_handler.call_test(func_name, details)

    kwargs['ti'].xcom_push(key=xcom_keyword, value=json.dumps(total_count_res))

    logger.info('total_count:', total_count_res)

def incr_count_check(**kwargs):
    details = kwargs['details']
    func_name = 'getIncrementalCount'
    xcom_keyword = details['keyword'] + "--" + func_name
    
    incremental_count_res = test_handler.call_test(func_name, details)

    kwargs['ti'].xcom_push(key=xcom_keyword, value=json.dumps(incremental_count_res))

    logger.info('incremental_count:', incremental_count_res)

def pk_check(**kwargs):
    return
    details = kwargs['details']
    func_name = 'getPKCount'
    xcom_keyword = details['keyword'] + "--" + func_name

    distinct_pk_count = test_handler.call_test(func_name, details)

    kwargs['ti'].xcom_push(key=xcom_keyword, value=json.dumps(distinct_pk_count))

    logger.info('distinct_pk_count:', distinct_pk_count)

def ddl_check(**kwargs):
    details = kwargs['details']
    func_name = 'getDDL'
    xcom_keyword = details['keyword'] + "--" + func_name

    ddl = test_handler.call_test(func_name, details)

    kwargs['ti'].xcom_push(key=xcom_keyword, value=json.dumps(ddl))

    logger.info('ddl:', ddl)

def functional_recon(**kwargs):
    details = kwargs['details']
    func_name = 'fun_rcon'
    xcom_keyword = details['keyword'] + "--" + func_name

    res = test_handler.call_test(func_name, details)

    kwargs['ti'].xcom_push(key=xcom_keyword, value=json.dumps(res))

    logger.info('res:', res)

def data_check(**kwargs):
    return
    details = kwargs['details']
    func_name = 'getData'
    xcom_keyword = details['keyword'] + "--" + func_name

    data = test_handler.call_test(func_name, details)

    kwargs['ti'].xcom_push(key=xcom_keyword, value=json.dumps(data))

    logger.info('data:'), data

def result_generator(**kwargs):
    src_details = kwargs['src']
    dest_details = kwargs['dest']
    xcom_keyword_src = src_details['keyword']
    xcom_keyword_dest = dest_details['keyword']

    try:
        audit_columns = json.loads(Variable.get("audit_columns"))
    except AirflowNotFoundException:
        logger.warning("Error in finding Table config for Test Results Storage. Variable named 'audit_columns' could not found. Continuing without audit columns.") 

    src_res = {}
    dest_res = {}

    src_res['audit_columns'] = audit_columns
    dest_res['audit_columns'] = audit_columns

    src_res["db_name"] = src_details["db_name"]
    src_res["schema"] = src_details["schema"]
    src_res["name"] = src_details["name"]
    src_res["engine"] = src_details["engine"]

    dest_res["db_name"] = dest_details["db_name"]
    dest_res["schema"] = dest_details["schema"]
    dest_res["name"] = dest_details["name"]
    dest_res["engine"] = dest_details["engine"]

    for func_name in func_list:
        xcom_keyword_src_for_func = xcom_keyword_src + "--" + func_name
        xcom_keyword_dest_for_func = xcom_keyword_dest + "--" + func_name

        print("------------------func_name:", func_name)
        src_res[func_name] = json.loads(kwargs['ti'].xcom_pull(task_ids=None, key=xcom_keyword_src_for_func))
        dest_res[func_name] = json.loads(kwargs['ti'].xcom_pull(task_ids=None, key=xcom_keyword_dest_for_func))
        print(src_res[func_name])
        print(dest_res[func_name])

    test_result = ResultGenerator.create_result(func_list, src_res, dest_res)

    if test_result is None:
        logger.warning('An empty result was generated')
        return
    
    print(f"Result for src: {src_details['keyword']} and dest: {dest_details['keyword']} ---------------")
    print(test_result)
    print("storing the result...")
    report_config = json.loads(Variable.get(report_config_key))
    print("report_config: ", report_config)
    result = {
        "details":report_config,
        "test_result":test_result,
        "engine":"snowflake"
    }
    test_handler.call_test('store_test_result', result)



    dag_run_id = kwargs['dag_run'].run_id




with DAG('RECON_DAG_TEST', schedule_interval="30 11 * * *", default_args=args, start_date=datetime(2024, 9, 11), catchup=False) as dag:
    tasks_dict = dag.task_dict

    start = PythonOperator(
        task_id='start',
        python_callable=dag_start_task,
    )
    end = PythonOperator(
        task_id='end',
        python_callable=dag_end_task,
    )
    task_cnt = 0
    for src_dest_details in json_config:
        src_main = src_dest_details['source']
        dest_main = src_dest_details['destination']

        for idx, table in enumerate(src_dest_details["tables"]):
            task_cnt = task_cnt + 1

            src = copy.deepcopy(src_main)
            dest = copy.deepcopy(dest_main)


            src.update(table["src"])
            dest.update(table["dest"])

            fun_rcon = table.get("fun_rcon", None)

            if fun_rcon is not None:
                src["fun_rcon"] = table["fun_rcon"]
                dest["fun_rcon"] = table["fun_rcon"]

            src_db_name = src["db_name"]
            src_schema = src["schema"]
            src_tbl_name = src["name"]
            src_keyword_main = src_db_name + src_schema + src_tbl_name
            src_keyword = str(task_cnt) + src_db_name + src_schema + src_tbl_name
            src['keyword'] = src_keyword

            dest_db_name = dest["db_name"]
            dest_schema = dest["schema"]
            dest_tbl_name = dest["name"]
            dest_keyword_main = dest_db_name + dest_schema + dest_tbl_name
            dest_keyword = str(task_cnt) + dest_db_name + dest_schema + dest_tbl_name
            dest['keyword'] = dest_keyword

            source_name = src['name']
            destination_name = dest['name']

            tid = f"src--{src_keyword_main}--dest--{dest_keyword_main}"
            if tid not in tasks_dict:
                src_dest_task = DummyOperator(
                    task_id = tid
                )
            else:
                src_dest_task = tasks_dict[tid]
            
            table_name = src['name']

            table_noter = DummyOperator(
                task_id = f'{task_cnt}_{table_name}'
            )

            count_check_src = PythonOperator(
                task_id=f'count_check_src--{src_keyword}',
                python_callable=count_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':src}
            )
            incr_count_check_src = PythonOperator(
                task_id=f'incr_count_check_src--{src_keyword}',
                python_callable=incr_count_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':src}
            )
            pk_check_src = PythonOperator(
                task_id=f'pk_check_src--{src_keyword}',
                python_callable=pk_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':src}
            )
            ddl_check_src = PythonOperator(
                task_id=f'ddl_check_src--{src_keyword}',
                python_callable=ddl_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':src}
            )
            functional_recon_src = PythonOperator(
                task_id=f'functional_recon_src--{src_keyword}',
                python_callable=functional_recon,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':src}
            )
            data_check_src = PythonOperator(
                task_id=f'data_check_src--{src_keyword}',
                python_callable=data_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':src}
            )
            
            count_check_dest = PythonOperator(
                task_id=f'count_check_dest--{dest_keyword}',
                python_callable=count_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':dest}
            )
            incr_count_check_dest = PythonOperator(
                task_id=f'incr_count_check_dest--{dest_keyword}',
                python_callable=incr_count_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':dest}
            )

            pk_check_dest = PythonOperator(
                task_id=f'pk_check_dest--{dest_keyword}',
                python_callable=pk_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':dest}
            )
            ddl_check_dest = PythonOperator(
                task_id=f'ddl_check_dest--{dest_keyword}',
                python_callable=ddl_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':dest}
            )
            functional_recon_dest = PythonOperator(
                task_id=f'functional_recon_dest--{dest_keyword}',
                python_callable=functional_recon,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':dest}
            )
            data_check_dest = PythonOperator(
                task_id=f'data_check_dest--{dest_keyword}',
                python_callable=data_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':dest}
            )

            result_generator_task = PythonOperator(
                task_id=f"Compare_{src_keyword}--{dest_keyword}",
                python_callable=result_generator,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={"src":src, "dest": dest}
            )
            
            start >> src_dest_task >> table_noter
            table_noter >> count_check_src >> incr_count_check_src >> pk_check_src >> ddl_check_src >> functional_recon_src >> data_check_src >> result_generator_task >> end
            table_noter >> count_check_dest >> incr_count_check_dest >> pk_check_dest >> ddl_check_dest >> functional_recon_dest >> data_check_dest >> result_generator_task >> end
            # table_noter >> count_and_pk_check_src >> end
            # >> ddl_check_src >> data_check_src >> compare >> end
            # table_noter >> count_and_pk_check_dest >> end
            # >> ddl_check_dest >> data_check_dest >> compare >> end
            
            