'''
TITLE: DAILY PIPELINE DAG
DAG ID: daily_dag_pipeline
SCHEDULE_INTERVAL: EVERY DAY AT 11PM
DESCRIPTION:
This DAG runs the daily pipeline for the current day. 

AUTHOR: SARTAJ
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import dotenv
dotenv.load_dotenv()

from daily_scripting_kafka_consumer import MostPlayedGamesConsumer
from daily_scripting_kafka_producer import MostPlayedGamesProducer

from daily_script import DailyScript

default_args = {
    'owner': 'Sartaj',
    'depends_on_past': False,
    'retry': 1,
    'retry_delay': timedelta(minutes=1)
}

DAILY_RAW_DATA_SOURCE = os.getenv("DAILY_RAW_DATA_SOURCE")
DAILY_RAW_DATA_DESTINATION = os.getenv("DAILY_RAW_DATA_DESTINATION")

DAILY_PROCESSED_DATA_SOURCE = os.getenv("DAILY_PROCESSED_DATA_SOURCE")
DAILY_PROCESSED_DATA_DESTINATION = os.getenv("DAILY_PROCESSED_DATA_DESTINATION")
DAILY_PROCESSED_DATA_BACKUP_DESTINATION = os.getenv("DAILY_PROCESSED_DATA_BACKUP_DESTINATION")

def daily_pipeline_start():
    print("Pipeline started at {}".format(datetime.now()))

def run_consumer():
    print("Consumer awoken at {}".format(datetime.now()))
    #most_played_games_consumer_obj = MostPlayedGamesConsumer()
    #most_played_games_consumer_obj.runner()

def run_producer():
    print("Producer awoken at {}".format(datetime.now()))
    #most_played_games_producer_obj = MostPlayedGamesProducer()
    #most_played_games_producer_obj.runner()

def spark_data_processing_job():
    print("Spark job started at {}".format(datetime.now()))
    #daily_script_obj = DailyScript()
    #daily_script_obj.runner()

with DAG(
    default_args=default_args,
    dag_id="daily_dag_pipeline",
    schedule_interval='0 23 * * *', # Every day at 11pm
    start_date=datetime(2022, 10, 12),  
    catchup=False,
) as dag:
    task0= PythonOperator(
        task_id='daily_pipeline_start',
        python_callable=daily_pipeline_start
    )

    task1 = PythonOperator(
        task_id='consumer_awoken',
        python_callable=run_consumer
    )
    
    task2 = PythonOperator(
        task_id='producer_awoken',
        python_callable=run_producer
    )

    task3 = BashOperator(
        task_id='backup_raw_data',
        #bash_command=f's3_backup_script.sh {DAILY_RAW_DATA_SOURCE} {DAILY_RAW_DATA_DESTINATION}',
    )

    task4 = PythonOperator(
        task_id='spark_data_processing_job',
        python_callable=spark_data_processing_job
    )

    task5 = BashOperator(
        task_id='backup_processed_data',
        #bash_command=f's3_backup_script.sh {DAILY_PROCESSED_DATA_SOURCE} {DAILY_PROCESSED_DATA_BACKUP_DESTINATION}',
    )

    task6 = BashOperator(
        task_id='export_cleaned_data',
        #bash_command=f'ec2_to_s3_load_script.sh {DAILY_PROCESSED_DATA_SOURCE} {DAILY_PROCESSED_DATA_DESTINATION}',
    )
    
    task7 = BashOperator(
        task_id='pipeline_housekeeping',
        #bash_command=f'pipeline_housekeeping.sh path/to/localEC2/folder/',
    )

    # Pipeline
    task0 >> [task1, task2] >> task3 >> task4 >> [task5, task6] >> task7
  