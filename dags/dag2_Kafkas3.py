# from io import BytesIO
# from typing import Any, Dict
# import confluent_kafka
# from airflow import DAG
# #from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from confluent_kafka import Consumer, KafkaException
# import boto3
# import json
# import fastavro
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
# from confluent_kafka.serialization import SerializationContext, MessageField
#
# #import dlt
#
# #from helpers.kafka.consumer import avro_deserializer
# #from ingest.kafka import kafka_consumer
#
#
# #
# # def write_json_to_avro(json_data):
# #         schema_registry_conf = {'url': 'http://localhost:8085'}  # URL реестра схем
# #         schema_registry_client = SchemaRegistryClient(schema_registry_conf)
# #         # Получение списка всех subjects
# #         subjects = schema_registry_client.get_subjects()
# #         print("Список доступных схем (subjects):")
# #         for subject in subjects:
# #             print(subject)
# #         subject_name = "avro_shema"  # Замените на ваше имя схемы
# #         try:
# #             schema_response = schema_registry_client.get_latest_version(subject_name)
# #             schema_str = schema_response.schema.schema_str
# #             print(f"Latest schema for subject '{subject_name}':\n{schema_str}")
# #         except Exception as e:
# #             print(f"Error retrieving latest schema for subject '{subject_name}': {e}")
# #
# #
# #         schema = json.loads(schema_str)
# #         avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client)
# #         # Сериализация JSON в Avro
# #         serialized_data = avro_serializer(
# #             json_data,
# #             SerializationContext("my_topic", MessageField.VALUE)
# #         )
# #         print(serialized_data)
# #         buffer = BytesIO()
# #         fastavro.writer(buffer, schema, serialized_data)
# #         return buffer
# # #
#
# def consume_kafka_to_minio(**kwargs):
#
#     consumer = Consumer({
#         'bootstrap.servers': 'kafka:9092',
#         'sasl.mechanism': 'PLAIN',
#         'security.protocol': 'PLAINTEXT',
#         'group.id': 'group.id',
#         'auto.offset.reset': 'earliest'
#     })
#
#     KAFKA_TOPIC = "my_topic"
#     #consumer.subscribe([KAFKA_TOPIC])
#
#     # Подключение к MinIO через boto3
#     s3_client = boto3.client(
#         's3',
#         endpoint_url="http://minio:9000",  # Указываем адрес MinIO
#         aws_access_key_id="uNVVo4yHBhFHRB2nM08b",
#         aws_secret_access_key="27vvBm4pg36OVvjyeNge1MrsIQPYNWaPqiN7yet6"
#     )
#
#     S3_BUCKET_NAME = "test-bucket"
#     S3_KEY_PREFIX = "kafka_data/"
#
#     schema_registry_client = SchemaRegistryClient({
#         'url': "http://localhost:8085",
#     })
#
#     avro_deserializer = AvroDeserializer(schema_registry_client)
#
#     consumer.subscribe([KAFKA_TOPIC])
#     while True:
#         msg = consumer.poll(1.0)
#         if msg is None:
#             continue
#         deserialized_key = avro_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
#         deserialized_value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
#
#         if deserialized_value is not None:
#             print("Key {}: Value{} \n".format(deserialized_key, deserialized_value))
#
#     consumer.close()
#
#     # try:
#     #     # Подписываемся на топик
#     #     consumer.subscribe([KAFKA_TOPIC])
#     #
#     #     print(f"Подписан на топик: {KAFKA_TOPIC}")
#     #     while True:
#     #         # Читаем сообщение (таймаут 1 сек)
#     #         msg = consumer.poll(5.0)
#     #
#     #         if msg is None:
#     #             print("нет сообщений")
#     #             continue  # Нет новых сообщений
#     #         if msg.error():
#     #             # Обрабатываем ошибку
#     #             if msg.error().code() == KafkaException._PARTITION_EOF:
#     #                 print("Достигнут конец раздела")
#     #             else:
#     #                 print(f"Ошибка: {msg.error()}")
#     #                 break
#     #         else:
#     #             # Выводим сообщение
#     #             print(f"Получено сообщение: {msg.value().decode('utf-8')} от {msg.topic()}[{msg.partition()}]")
#     #         # Десериализация JSON сообщения
#     #         message_value = msg.value().decode('utf-8')
#     #         print(f"Received message: {message_value}")
#     #         json_data = json.loads(message_value)
#     #
#     #         # Преобразуем JSON в список для Avro (fastavro ожидает массив записей)
#     #         #avro_data = [json_data]
#     #
#     #         # Записываем данные в формате Avro в буфер
#     #         avro_buffer = write_json_to_avro(json_data)
#     #         #print(avro_buffer)
#     #
#     #         # Генерируем ключ для файла в S3
#     #         s3_key = f"{S3_KEY_PREFIX}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.avro"
#     #
#     #         s3_client.put_object(
#     #             Bucket=S3_BUCKET_NAME,
#     #             Key=s3_key,
#     #             Body=avro_buffer.getvalue(),
#     #             ContentType='application/octet-stream'
#     #         )
#     #         print(f"Uploaded Avro file to MinIO: {s3_key}")
#     # except Exception as e:
#     #         print(f"Error: {e}")
#     # finally:
#     #         consumer.close()
#
#
# #consume_kafka_to_minio()
#
# # def check_minio_connection(**kwargs):
# #     import boto3
# #     """
# #     Проверяет подключение к MinIO и наличие указанного бакета.
# #     """
# #     try:
# #         # Подключение к MinIO через boto3
# #         s3_client = boto3.client(
# #             's3',
# #             # endpoint_url=connect_s3().endpoint_url,
# #             # aws_access_key_id= connect_s3().access_key,
# #             # aws_secret_access_key=connect_s3().secret_key
# #             endpoint_url="http://minio:9000",  # Указываем адрес MinIO
# #             aws_access_key_id="uNVVo4yHBhFHRB2nM08b",
# #             aws_secret_access_key="27vvBm4pg36OVvjyeNge1MrsIQPYNWaPqiN7yet6"
# #         )
# #         # Проверка наличия бакета
# #         response = s3_client.list_buckets()
# #         bucket_names = [bucket['Name'] for bucket in response.get('Buckets', [])]
# #         S3_BUCKET_NAME="test-bucket"
# #
# #         if S3_BUCKET_NAME in bucket_names:
# #             print(f"Bucket '{S3_BUCKET_NAME}' доступен.")
# #         else:
# #             print(f"Bucket '{S3_BUCKET_NAME}' не найден.")
# #             raise ValueError(f"Bucket '{S3_BUCKET_NAME}' отсутствует в MinIO.")
# #
# #     except Exception as e:
# #         print(f"Ошибка подключения к MinIO: {e}")
# #         raise
#
#
#
#
#
#
# #Создаем DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }
#
# with DAG('kafka_to_minio', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
#     # test = PythonOperator(
#     #     task_id="test",
#     #     python_callable=check_minio_connection
#     # )
#
#     kafkaS3 = PythonOperator(
#         task_id='kafka_data',
#         python_callable=consume_kafka_to_minio
#     )
#
#
# # # #test>>kafkaS3