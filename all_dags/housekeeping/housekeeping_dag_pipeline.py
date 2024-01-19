'''
TITLE: HOUSEKEEPING DAG
DAG ID: housekeeping_dag_pipeline
SCHEDULE_INTERVAL: None [Manually Trigger]
DESCRIPTION:
This DAG deletes todays raw and processed data

AUTHOR: SARTAJ
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
#from delete_todays_data import *

default_args = {
    'owner': 'Sartaj',
    'depends_on_past': False,
    'retry': 1,
    'retry_delay': timedelta(minutes=1)
}

def housekeeping_dag_pipeline_start():
    print("Pipeline started at {}".format(datetime.now()))

def delete_todays_data():
    print("Deleting Todays data{}".format(datetime.now()))


with DAG(
    default_args=default_args,
    dag_id="housekeeping_dag_pipeline",
    schedule_interval= None,
    catchup=False,
) as dag:
    task0= PythonOperator(
        task_id='housekeeping_dag_pipeline_start',
        python_callable=housekeeping_dag_pipeline_start
    )

    task1 = PythonOperator(
        task_id='consumer_awoken',
        python_callable=delete_todays_data
    )
    # Pipeline
    task0 >> task1