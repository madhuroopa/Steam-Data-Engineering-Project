from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


#NEED TO ADD JAVA TO RUN SPARK

# import sys
# sys.path.append( '../../code/spark/')
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

def daily_raw_data_backup():
    # run backup_raw_data.sh
    print("Daily daily_raw_data_backup Completed")

def process_data():
    daily_script_obj = daily_script.DailyScript()
    daily_script_obj.runner()
    print("Daily process_data Completed")

def daily_cleaned_data_backup():
    # run backup_cleaned_data.sh
    print("Daily backup_cleaned_data Completed")

# Create a sub-DAG for tasks 1 and 2
with DAG(
    default_args=default_args,
    dag_id="subdag_1_2",
    schedule_interval=None,  # This sub-DAG is not scheduled independently
    start_date=datetime(2021, 10, 12),  # Add start_date here
    catchup=False,
) as subdag_1_2:
    task1 = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data
    )
    
    task2 = PythonOperator(
        task_id='daily_raw_data_backup',
        python_callable=daily_raw_data_backup
    )

    task1 >> task2

# Create a sub-DAG for tasks 3 and 4
with DAG(
    default_args=default_args,
    dag_id="subdag_3_4",
    schedule_interval=None,  # This sub-DAG is not scheduled independently
    start_date=datetime(2021, 10, 12),  # Add start_date here
    catchup=False,
) as subdag_3_4:
    task3 = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    task4 = PythonOperator(
        task_id='daily_cleaned_data_backup',
        python_callable=daily_cleaned_data_backup
    )

    task3 >> task4

# Main DAG
with DAG(
    default_args=default_args,
    dag_id="daily_pipeline",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@daily'
) as dag:
    
    trigger_subdag_1_2 = TriggerDagRunOperator(
        task_id="trigger_subdag_1_2",
        trigger_dag_id="subdag_1_2",
        dag=dag,
    )

    trigger_subdag_3_4 = TriggerDagRunOperator(
        task_id="trigger_subdag_3_4",
        trigger_dag_id="subdag_3_4",
        dag=dag,
    )

    # Set up the relationships
    trigger_subdag_1_2 >> trigger_subdag_3_4
