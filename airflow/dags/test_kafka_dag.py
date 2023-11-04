#incorrect, need to design DAG such that task 2 starts when task 1 ends
import sys
from datetime import datetime, timedelta
sys.path.append( '../../code/kafka_scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
import daily_scripting_kafka_consumer, daily_scripting_kafka_producer



default_args = {
    'owner': 'Sartaj',
    'depends_on_past': False,
    'retry': 1,
    'retry_delay': timedelta(minutes=1)
}

def run_consumer():
    daily_scripting_consumer_obj = daily_scripting_kafka_consumer.MostPlayedGamesConsumer()
    daily_scripting_consumer_obj.runner()


def run_producer():
    daily_scripting_kafka_producer_obj = daily_scripting_kafka_producer.MostPlayedGamesProducer()
    daily_scripting_kafka_producer_obj.runner()


with DAG(
    default_args=default_args,
    dag_id="test_spark_dag",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='get_sklearn',
        python_callable=run_consumer
    )
    
    task2 = PythonOperator(
        task_id='get_matplotlib',
        python_callable=run_producer
    )

    task1 >> task2