'''
TITLE: MONTHLY PIPELINE DAG
DAG ID: catchup_monthly_dag_pipeline
SCHEDULE_INTERVAL: None [Manually Trigger]
DESCRIPTION:
This DAG is a Kafka catchup DAG that runs the monthly kafka part of the pipeline for the last month. 

AUTHOR: SARTAJ
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Sartaj',
    'depends_on_past': False,
    'retry': 0,
    }

def housekeeping_dag_pipeline_start():
    print("Pipeline started at {}".format(datetime.now()))

def delete_todays_data():
    print("Consumer awoken at {}".format(datetime.now()))

with DAG(
    default_args=default_args,
    dag_id="housekeeping_dag_pipeline",
    schedule_interval=None,
    start_date=datetime(2022, 10, 12),  
    catchup=False,
) as dag:
    task0= PythonOperator(
        task_id='housekeeping_dag_pipeline_start',
        python_callable=housekeeping_dag_pipeline_start
    )

    task1 = PythonOperator(
        task_id='delete_todays_data',
        python_callable=delete_todays_data
    )

    # Pipeline
    task0 >> task1