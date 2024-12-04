from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def hw():
    print("Hello, World!")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('test_dags', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    fetch_mongo_data = PythonOperator(
        task_id='help',
        python_callable=hw,
        provide_context=True
    )
