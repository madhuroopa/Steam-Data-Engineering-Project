from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# import sys
# sys.path.append( '../../code/spark/')
import os
import daily_script

default_args = {
    'owner': 'Sartaj',
    'depends_on_past': False,
    'retry': 1,
    'retry_delay': timedelta(minutes=1)
}

def scrape_data():
    # call kafka producer and consumer
    print("Daily scrape_data Completed")
    print("Current working directory is:", os.getcwd())

def daily_raw_data_backup():
    # run backup_raw_data.sh
    print("Daily daily_raw_data_backup Completed")
    print("Current working directory is:", os.getcwd())

def process_data():
    daily_script_obj = daily_script.DailyScript()
    daily_script_obj.runner()
    print("Daily process_data Completed")
    print("Current working directory is:", os.getcwd())

def daily_cleaned_data_backup():
    # run backup_cleaned_data.sh
    print("Daily backup_cleaned_data Completed")
    print("Current working directory is:", os.getcwd())

with DAG(
    default_args=default_args,
    dag_id="test_spark_dag_finding_location",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )
    
    task2 = PythonOperator(
        task_id='daily_cleaned_data_backup',
        python_callable=daily_cleaned_data_backup
    )

    task1 >> task2
