from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Sartaj',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


def get_selenium():
    import selenium
    print(f"Selenium detected in docker airflow with version: {selenium.__version__} ")


def get_pyspark():
    import pyspark
    print(f"Pyspark detected in docker airflow with version: {pyspark.__version__}")


with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies_v03",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_selenium
    )
    
    task2 = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_pyspark
    )

    task1 >> task2