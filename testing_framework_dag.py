from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import time
from framework.main import test_handler
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd

args = { "provide_context":True }

test_map = {
            'count_and_pk_check': 'getTotalCountandPKCount',
            'ddl_check': 'getDDL',
            'functional_recon': 'func_check',
            'data_check': 'getData'
        }

json_config = [
    {
        "source": {
            "name": "Opus_GG_DWHSTAGE_source1",
            "host": "opus-gg.bagicdatahub.bajajallianz.com.bagicdatahub.bajajallianz.com",
            "port": 1521,
            "service_name": "DWSTGGG1",
            "schema": "GG_DWHSTAGE",
            "auth_mechanism": "asm",
            "secret_key": "DPM_Source_OPUS",
            "engine": "oracle",
        },
        "destination": {
            "name": "snowflake_INGESTION_DEV_DB",
            "account": "iz53102.ap-south-1.privatelink",
            "warehouse": "PROD_HISTORICAL_INGESTION_WH",
            "db_name": "BAGIC_PROD_MIRROR_DB",
            "schema": "OPUS_GG_DWHSTAGE",
            "auth_mechanism": "asm",
            "secret_key": "empower_snowflake",
            "role": "EMPOWER_FOUNDATION_ROLE",
            "engine": "snowflake",
        },
        "table_details": [
            {
                "source_name": "BJAZ_HM_CLAIM_TRACKER_FRO",
                "destination_name": "BJAZ_HM_CLAIM_TRACKER_FRO",
                "primary_key": "CLAIM_ID,VERSION_NO",
                "watermark_column": "GG_CHANGE_DATE"
            },
            {
                "source_name": "BJAZ_HM_CLAIM_TRACKER_CDC",
                "destination_name": "BJAZ_HM_CLAIM_TRACKER_CDC",
                "primary_key": "CLAIM_ID,VERSION_NO",
                "watermark_column": "GG_CHANGE_DATE"
            },
        ]
    },
    {
    "source": {
        "name": "Opus_GG_DWHSTAGE_source2",
        "host": "opus-gg.bagicdatahub.bajajallianz.com.bagicdatahub.bajajallianz.com",
        "port": 1521,
        "service_name": "DWSTGGG1",
        "schema": "GG_DWHSTAGE",
        "auth_mechanism": "asm",
        "secret_key": "DPM_Source_OPUS",
        "engine": "oracle",
    },
    "destination": {
        "name": "snowflake_INGESTION_MIRROR_DB",
        "account": "iz53102.ap-south-1.privatelink",
        "warehouse": "PROD_HISTORICAL_INGESTION_WH",
        "db_name": "BAGIC_PROD_MIRROR_DB",
        "schema": "OPUS_GG_DWHSTAGE",
        "auth_mechanism": "asm",
        "secret_key": "empower_snowflake",
        "role": "EMPOWER_FOUNDATION_ROLE",
        "engine": "snowflake",
    },
    "table_details": [
        {
            "source_name": "BJAZ_HM_CLAIM_FRO",
            "destination_name": "BJAZ_HM_CLAIM_FRO",
            "primary_key": "CLAIM_ID,VERSION_NO",
            "watermark_column": "GG_CHANGE_DATE"
        },
        {
            "source_name": "BJAZ_HM_CLAIM_CDC",
            "destination_name": "BJAZ_HM_CLAIM_CDC",
            "primary_key": "CLAIM_ID,VERSION_NO",
            "watermark_column": "GG_CHANGE_DATE"
        },
    ]
    }
]

def dag_start_task(**kwargs):
    test_id = kwargs['dag_run'].run_id
    return test_id

def dag_end_task(**kwargs):
    print("Testing DAG has ended")

def count_and_pk_check(**kwargs):
    details = kwargs['details']
    total_count, distinct_pk_count = test_handler('count_and_pk_check', details)
    print(total_count, distinct_pk_count)
    # return total_count, distinct_pk_count

def ddl_check(**kwargs):
    details = kwargs['details']
    ddl_df = test_handler('ddl_check', details)
    print(ddl_df)
    # return ddl_df

def functional_recon(**kwargs):
    details = kwargs['details']
    ddl_df = test_handler('ddl_check', details)
    print(ddl_df)
    # return ddl_df

def data_check(**kwargs):
    details = kwargs['details']
    data_df = test_handler('data_check', details)
    print(data_df)
    # return data_df

def compare_src_dest(**kwargs):
    test_id = kwargs['dag_run'].run_id
    return test_id


with DAG('testing_framework_dag', schedule_interval=None, default_args=args, start_date=datetime(2024, 9, 11), catchup=False) as dag:
    tasks_dict = dag.task_dict

    start = PythonOperator(
        task_id='start',
        python_callable=dag_start_task,
    )
    end = PythonOperator(
        task_id='end',
        python_callable=dag_end_task,
    )

    for src_dest_details in json_config:
        src = src_dest_details['source']
        dest = src_dest_details['destination']
        source_name = src['name']
        destination_name = dest['name']

        tid = f"src-{source_name}-dest-{destination_name}"
        if tid not in tasks_dict:
            src_dest_task = DummyOperator(
                task_id = tid
            )
        else:
            src_dest_task = tasks_dict[tid]
        
        table_details = src_dest_details['table_details']
        for table in table_details:
            src = {**src, **table}
            dest = {**dest, **table}
            table_name = table['source_name']

            table_noter = DummyOperator(
                task_id = table_name
            )

            count_and_pk_check_src = PythonOperator(
                task_id=f'count_and_pk_check_src--{source_name}-{table_name}',
                python_callable=count_and_pk_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':src}
            )
            ddl_check_src = PythonOperator(
                task_id=f'ddl_check_src--{source_name}-{table_name}',
                python_callable=ddl_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':src}
            )
            functional_recon_src = PythonOperator(
                task_id=f'functional_recon_src--{source_name}-{table_name}',
                python_callable=functional_recon,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':src}
            )
            data_check_src = PythonOperator(
                task_id=f'data_check_src--{source_name}-{table_name}',
                python_callable=data_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':src}
            )
            
            count_and_pk_check_dest = PythonOperator(
                task_id=f'count_and_pk_check_dest--{destination_name}-{table_name}',
                python_callable=count_and_pk_check,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                op_kwargs={'details':dest}
            )
            ddl_check_dest = PythonOperator(
                task_id=f'ddl_check_dest--{destination_name}-{table_name}',
                python_callable=ddl_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':dest}
            )
            functional_recon_dest = PythonOperator(
                task_id=f'functional_recon_dest--{destination_name}-{table_name}',
                python_callable=functional_recon,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':dest}
            )
            data_check_dest = PythonOperator(
                task_id=f'data_check_dest--{destination_name}-{table_name}',
                python_callable=data_check,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs={'details':dest}
            )

            compare = PythonOperator(
                task_id=f"compare_{table['source_name']}",
                python_callable=compare_src_dest,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                op_kwargs=dest
            )
            
            start >> src_dest_task >> table_noter
            table_noter >> count_and_pk_check_src >> ddl_check_src >> functional_recon_src >> data_check_src >> compare >> end
            table_noter >> count_and_pk_check_dest >> ddl_check_dest >> functional_recon_dest >> data_check_dest >> compare >> end
            
            