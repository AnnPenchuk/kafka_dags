import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import boto3
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from settings import settings


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


# Задача для подключения
def check_s3_connection():
    client = connect_s3()
    try:
        response = client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        print(f"Подключение успешно. Найдено {len(buckets)} бакетов: {buckets}")
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        raise


def to_s3(message):
    print(type(message))
    print(message)
    if message is None:
        print("No messages to process")
        return

    # Настраиваем подключение к MinIO
    client = connect_s3()
    bucket_name = settings.bucket_name

    n = 0
    buffer = io.BytesIO(message)  # Используем байтовые данные напрямую

        # Генерируем имя файла
    file_name = f"{n}_{datetime.now().strftime('%Y%m%d%H%M%S')}.avro"

    try:
        client.upload_fileobj(buffer, bucket_name, file_name)
        print(f"Uploaded file: {file_name}")
        n += 1
    except Exception as e:
        print(f"Failed to upload file {file_name}: {e}")



def consume_kafka(**kwargs):
    schema_registry_client = SchemaRegistryClient({
        'url': settings.schema_registry,
    })

    avro_deserializer = AvroDeserializer(schema_registry_client)
    consumer = Consumer({
        'bootstrap.servers': settings.kafka,
        'group.id': settings.group_id,
        'auto.offset.reset': 'earliest'
    })
    topic1=settings.topic1
    consumer.subscribe([topic1])


    while True:
            msg = consumer.poll(6.0)
            print(type(msg))
            if msg is not None:
                 avro_deserializer = AvroDeserializer(schema_registry_client)
                 print(f"Avro message: {msg.value()}")
                 #deserialized_key = avro_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                 deserialized_value = avro_deserializer(msg.value(),SerializationContext(msg.topic(), MessageField.VALUE))



                 if deserialized_value is not None:
                     print("Value{} \n".format(deserialized_value))
                 to_s3(msg.value())
                 if msg is None:
                     print('messages are over')
                     consumer.close()

            if msg is None:
                print('No message received')
                continue





# Определение DAG
with DAG(
        dag_id="dag2_kafka_s3",
        default_args={
            "owner": "airflow",
            "retries": 1,
        },
        description="DAG для  подключения kafka к MinIO (S3)",
        schedule_interval=None,  # Запуск вручную
        start_date=datetime(2023, 1, 1),
        catchup=False,

) as dag:
    # Операция проверки подключения
    test_connection = PythonOperator(
        task_id="test_s3_connection",
        python_callable=check_s3_connection
    )

    consume_from_topic = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_kafka,
        provide_context=True,
    )


test_connection>>consume_from_topic
