import io
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


def to_s3_avro(message):
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


def to_s3(json_doc):
    # json_doc = [{
    #     '_id': '675785ffe659c3a770165c8a',
    #     'travel': 2,
    #     'payer': 3,
    #     'amount': 19738,
    #     'code_departure': 'BOM',
    #     'code_arrival': 'LAX',
    #     'number_of_passengers': 2,
    #     'passengers': [
    #         {'name': 'Jayden Slawa', 'birth_date': '1962-09-16'},
    #         {'name': 'Beverly Alvarez', 'birth_date': '1979-05-30'}
    #     ],
    #     'created': '2024-12-10T00:06:23.425Z'
    # }]
    n = 0
    client = connect_s3()
    print(json_doc)
    # # Преобразование сложных типов в плоскую структуру
    # flat_data = []
    # for record in json_doc:
    #     for passenger in record['passengers']:
    #         flat_data.append({
    #             '_id': record['_id'],
    #             'travel': record['travel'],
    #             'payer': record['payer'],
    #             'amount': record['amount'],
    #             'code_departure': record['code_departure'],
    #             'code_arrival': record['code_arrival'],
    #             'number_of_passengers': record['number_of_passengers'],
    #             'passenger_name': passenger['name'],
    #             'passenger_birth_date': passenger['birth_date'],
    #             'created': record['created']
    #         })

    # Преобразование в DataFrame
    # json_doc["_id"] = str(json_doc["_id"])  # Конвертация _id в строку
    data = pd.DataFrame([json_doc])
    # # Преобразовать данные в формат Parquet
    table = pa.Table.from_pandas(data)
    buffer = io.BytesIO()  # Создаем буфер
    pq.write_table(table, buffer)  # Записываем таблицу в Parquet-формате в буфер
    buffer.seek(0)  # Устанавливаем курсор в начало буфера
    n+= 1
    bucket_name = 'reciept-bucket'
    # Загрузка файла в S3
    parquet_file_name = f"{n}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    client.upload_fileobj(buffer, bucket_name, f"{parquet_file_name}")
    print(f"Загрузка завершена: {parquet_file_name}")





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
                 print(f"Avro message: {msg.value()}")
                 deserialized_value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                 deserialized_key = avro_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                 print("Deserialized Value:", deserialized_value)
                 print("Deserialized Key:", deserialized_key)
                 to_s3(deserialized_value)
                 if msg is None:
                     print('messages are over')
                     consumer.close()

            if msg is None:
                print('No message received')
                #deserialized_value= {'_id': {'oid': '675785ffe659c3a770165c8a'}, 'travel': 2, 'payer': 3, 'amount': 19738, 'code_departure': 'BOM', 'code_arrival': 'LAX', 'number_of_passengers': 2, 'passengers': [{'name': 'Jayden Slawa', 'birth_date': '1962-09-16'}, {'name': 'Beverly Alvarez', 'birth_date': '1979-05-30'}], 'created': {'date': '2024-12-10T00:06:23.425Z'}}
                # to_s3(deserialized_value)
                # break
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
