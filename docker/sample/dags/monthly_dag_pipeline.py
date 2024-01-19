'''
TITLE: MONTHLY PIPELINE DAG
DAG ID: monthly_dag_pipeline
SCHEDULE_INTERVAL: EVERY LAST DAY OF THE MONTH AT 11PM
DESCRIPTION:
This DAG runs the monthly pipeline for the last month. 

AUTHOR: SARTAJ
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'Sartaj',
    'depends_on_past': False,
    'retry': 1,
    'retry_delay': timedelta(minutes=1)
}

def monthly_pipeline_start():
    print("Pipeline started at {}".format(datetime.now()))

def run_consumer():
    print("Consumer awoken at {}".format(datetime.now()))

def run_producer():
    print("Producer awoken at {}".format(datetime.now()))

def backup_raw_data():
    print("Raw data backed up at {}".format(datetime.now()))

def spark_data_processing_job():
    print("Spark job started at {}".format(datetime.now()))

def backup_processed_data():
    print("Cleaned data backed up at {}".format(datetime.now()))

def export_cleaned_data():
    print("Cleaned data exported at {}".format(datetime.now()))

def pipeline_housekeeping():
    print("Dashboard triggered at {}".format(datetime.now()))

with DAG(
    default_args=default_args,
    dag_id="monthly_dag_pipeline",
    schedule_interval='0 23 L * *', # Last day of every month at 11pm  
    start_date=datetime(2022, 10, 12),  
    catchup=False,
) as dag:
    task0= PythonOperator(
        task_id='monthly_pipeline_start',
        python_callable=monthly_pipeline_start
    )

    task1 = PythonOperator(
        task_id='consumer_awoken',
        python_callable=run_consumer
    )
    
    task2 = PythonOperator(
        task_id='producer_awoken',
        python_callable=run_producer
    )

    task3 = PythonOperator(
        task_id='backup_raw_data',
        python_callable=backup_raw_data
    )

    task4 = PythonOperator(
        task_id='spark_data_processing_job',
        python_callable=spark_data_processing_job
    )

    task5 = PythonOperator(
        task_id='backup_processed_data',
        python_callable=backup_processed_data
    )

    task6 = PythonOperator(
        task_id='export_cleaned_data',
        python_callable=backup_raw_data
    )
    
    task7 = PythonOperator(
        task_id='pipeline_housekeeping',
        python_callable=pipeline_housekeeping
    )

    # Pipeline
    task0 >> [task1, task2] >> task3 >> task4 >> [task5, task6] >> task7
  