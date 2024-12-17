import io
import json

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pyarrow import fs

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


def  s3_fs():
    s3_config = Variable.get("minio_s3_config", deserialize_json=True)
    access_key = s3_config[0]
    secret_key = s3_config[1]
    endpoint_url = s3_config[2]


    s3_fs = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=endpoint_url,
        region='us-east-1'
    )
    return s3_fs


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



def to_s3(json_doc):
    fs= s3_fs()
    print(json_doc)
    # Преобразование в DataFrame
    df = pd.DataFrame(json_doc)
    df['_id'] = df['_id'].apply(lambda x: x.get('oid') if isinstance(x, dict) else x)
    df['created'] = df['created'].apply(lambda x: x.get('date') if isinstance(x, dict) else x)
    # Преобразование столбца 'passengers' в строку
    df['passengers'] = df['passengers'].apply(lambda x: str(x)[1:-1])
    # Загрузка файла в S3
    bucket_name = 'reciept-bucket'
    file_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    #file_name = f"{'reciept'}.parquet"
    parquet_file_path = f"{bucket_name}/{file_name}"
    df.to_parquet(f"{parquet_file_path}", engine="pyarrow", filesystem=fs)
    print(f"Загрузка завершена: {file_name}")





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

    data_value = []
    size = 0  # Текущий размер
    batch_limit = 9  # Лимит
    while True:
            msg = consumer.poll(6.0)
            if msg is not None:
                 print(f"Avro message: {msg.value()}")
                 deserialized_value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                 deserialized_key = avro_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                 print("Deserialized Value:", deserialized_value)
                 print("Deserialized Key:", deserialized_key)

                 if size > batch_limit:
                     # Отправить текущую партию
                     print(f"Отправка {size} сообщений")
                     to_s3(data_value)  # Замените своей функцией
                     data_value = []
                     size = 0
                 # Добавить сообщение в буфер
                 data_value.append(deserialized_value)
                 size += 1
                 print(data_value)
                 if msg is None:
                     print('messages are over')
                     print(size, " сообщений загружено")
                     to_s3(data_value)
                     consumer.close()

            if msg is None:
                print('No message received')
            continue





# Определение DAG
with DAG(
        dag_id="dag2_kafka_to_s3_parquet",
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

    kafka_to_s3 = PythonOperator(
        task_id='kafka_to_s3',
        python_callable=consume_kafka,
        provide_context=True,
    )


test_connection>>kafka_to_s3
