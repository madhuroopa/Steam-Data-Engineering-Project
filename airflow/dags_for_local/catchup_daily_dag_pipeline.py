'''
TITLE: DAILY PIPELINE CATCHUP DAG
DAG ID: catchup_daily_dag_pipeline
SCHEDULE_INTERVAL: None [Manually Trigger]
DESCRIPTION:
This DAG is a Kafka catchup DAG that runs the daily kafka part of the pipeline for the last day. 

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

def catchup_daily_pipeline_start():
     print("Pipeline started at {}".format(datetime.now()))

def catchup_run_consumer():
    print("Consumer awoken at {}".format(datetime.now()))

def catchup_run_producer():
    print("Producer awoken at {}".format(datetime.now()))

def catchup_backup_raw_data():
    print("Raw data backed up at {}".format(datetime.now()))

with DAG(
    default_args=default_args,
    dag_id="catchup_daily_dag_pipeline",
    schedule_interval= None, 
    start_date=datetime(2022, 10, 12),  
    catchup=False,
) as dag:
    task0= PythonOperator(
        task_id='catchup_daily_pipeline_start',
        python_callable=catchup_daily_pipeline_start
    )

    task1 = PythonOperator(
        task_id='catchup_consumer_awoken',
        python_callable=catchup_run_consumer
    )
    
    task2 = PythonOperator(
        task_id='catchup_producer_awoken',
        python_callable=catchup_run_producer
    )

    task3 = PythonOperator(
        task_id='catchup_backup_raw_data',
        python_callable=catchup_backup_raw_data
    )

    # Pipeline
    task0 >> [task1, task2] >> task3 
  