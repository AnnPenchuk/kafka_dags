import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, time
from confluent_kafka import Producer
from settings import settings


# Подключение к S3
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


def check_s3_connection():
    client = connect_s3()
    try:
        response = client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        print(f"Подключение успешно. Найдено {len(buckets)} бакетов: {buckets}")
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        raise


#
# # Подключение к Kafka Producer
# def connect_kafka_producer():
#     KAFKA_URL = settings.kafka
#     kafka_config = Variable.get(KAFKA_URL, deserialize_json=True)
#
#     return Producer({'bootstrap.servers': kafka_config['bootstrap_servers']})
#
#
# def send_to_kafka(data):
#     """Отправка данных в Kafka (как бинарные данные)"""
#     TOPIC_NAME = settings.topic2
#
#     kafka_config = {
#         'bootstrap.servers': settings.kafka,
#     }
#
#     producer = Producer(kafka_config)
#     producer.produce(topic=TOPIC_NAME, value=data)
#     producer.flush()
#     print(f"Сообщение отправлено в Kafka на топик {TOPIC_NAME}")
#
# def read_from_minio_to_kafka():
#     client = connect_s3()
#     try:
#         response = client.list_buckets()
#         #buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
#         #print(f"Подключение успешно. Найдено {len(buckets)} бакетов: {buckets}")
#     except Exception as e:
#         print(f"Ошибка подключения: {e}")
#         raise
#     bucket_name = settings.bucket_name
#     response = client.list_objects_v2(Bucket=bucket_name)
#     if 'Contents' in response:
#         print(f"Содержимое бакета {bucket_name}:")
#         for obj in response['Contents']:
#             print(type(obj))
#             file_key = obj['Key']
#             print(f"Обрабатывается файл: {file_key}")
#             file_response = client.get_object(Bucket=bucket_name, Key=file_key)
#             file_data = file_response['Body'].read()
#             # Отправка данных в Kafka
#             send_to_kafka(file_data)
#     else:
#         print(f"Бакет {client.bucket_name} пуст или недоступен.")



#
#
# with DAG(
#     dag_id="s3_to_kafka_avro",
#     default_args={"owner": "airflow", "retries": 1},
#     description="DAG для записи данных в формате Avro из S3 в Kafka",
#     schedule_interval=None,
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
# ) as dag:
#
#     # Операция проверки подключения
#     test_connection = PythonOperator(
#         task_id="test_s3_connection",
#         python_callable=check_s3_connection
#     )
#
#     read_from_minio_to_kafka = PythonOperator(
#         task_id="read_from_minio",
#         python_callable=read_from_minio_to_kafka
#     )
#
#
# test_connection>>read_from_minio_to_kafka



