from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import boto3
from datetime import datetime


# Функция для подключения к S3
def connect_s3():
    s3_config = Variable.get("minio_s3_config", deserialize_json=True)
    access_key = s3_config[0]
    secret_key = s3_config[1]
    endpoint_url = s3_config[2]

    # Создаем клиент S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url
    )
    return s3_client


# Задача для проверки подключения
def check_s3_connection():
    client = connect_s3()
    try:
        response = client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        print(f"Подключение успешно. Найдено {len(buckets)} бакетов: {buckets}")
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        raise


# Определение DAG
with DAG(
        dag_id="check_s3_connection",
        default_args={
            "owner": "airflow",
            "retries": 1,
        },
        description="DAG для проверки подключения к MinIO (S3)",
        schedule_interval=None,  # Запуск вручную
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=["s3", "minio", "test"],
) as dag:
    # Операция проверки подключения
    test_connection = PythonOperator(
        task_id="test_s3_connection",
        python_callable=check_s3_connection
    )


test_connection
