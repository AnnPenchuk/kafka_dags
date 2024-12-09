import json
import logging
from datetime import timedelta, datetime
from time import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from bson import ObjectId, json_util
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from pymongo import MongoClient



# Функция для подключения к MongoDB и извлечения данных
def fetch_data_from_mongo():
    MONGO_HOST = "mongodb+srv://smartintegratordev:YV1ZVX0XbmgGfaq0@cluster0.9kxy2.mongodb.net/"
    client = MongoClient(
        host=MONGO_HOST,
    )
    db = client["data_platform_si"]
    collection = db["Receipt"]
    start_date = datetime(2024, 12, 1)
    end_date = datetime(2024, 12, 11)

    query = {
        "created": {
            "$gte": start_date,
            "$lt": end_date
        }
    }
    documents_list = list(collection.find(query))
    json_result = json.dumps(documents_list, default=json_util.default)
    return json_result

def get_key(msg: dict, pos: int) -> dict:
    _id = msg["_id"]["oid"]
    ts = int(time())
    return {
            "collection": "reciept",
            "pos": pos,
            "timestamp": ts,
            "_id": _id
    }
def send_msg(msg: dict, pos: int) -> None:
        key_msg = get_key(msg=msg, pos=pos)
        schema1 = 'data.avro.receipt_value'
        schema2 = 'data.avro.receipt_key'
        TOPIC_NAME = 'data.travelagency.avro.receipt'
        version: str | int = "latest"
        SCHEMA_REGISTRY_URL = "http://172.30.181.198:8085"
        schema_registry_conf = {
            'url': SCHEMA_REGISTRY_URL,
        }
        producer_conf = {
            'bootstrap.servers': '172.30.181.198:9092',
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'PLAINTEXT',
            'group.id': 'prodcharm',
            'auto.offset.reset': 'earliest',
        }
        producer = Producer(producer_conf)
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        schema_version1 = schema_registry_client.get_version(schema1, version)
        schema_version2 = schema_registry_client.get_version(schema2, version)
        registered_schema1 = schema_version1.schema.schema_str
        registered_schema2 = schema_version2.schema.schema_str
        avro_serializer_key = AvroSerializer(schema_registry_client, registered_schema2)
        avro_serializer_value = AvroSerializer(schema_registry_client, registered_schema1)
        producer.produce(
            topic=TOPIC_NAME,
            key=avro_serializer_key(
                key_msg,
                SerializationContext(schema2, MessageField.KEY)
            ),
            value=avro_serializer_value(
                msg,
                SerializationContext(schema1, MessageField.VALUE)
            )
        )
        producer.flush()



def producer():
    json_result=fetch_data_from_mongo()
    msgs = json.loads(
        json_result
        .replace("$", "")
        .replace('"names"', '"passengers"')
        .replace('"number_of_names"', '"number_of_passengers"')
        .replace('"passenger"', '"name"')
    )

    admin_client = AdminClient({
            'bootstrap.servers': '172.30.181.198:9092'
        })
    num_partitions = 1
    replication_factor = 1

    topic = 'data.travelagency.avro.receipt1'
    if topic not in admin_client.list_topics(timeout=10).topics:
            NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
            logging.info(f'create {topic}')

    position = 0

    for msg in msgs:
        send_msg(msg, position)
        position += 1



# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
with DAG('dag1_producer_mongo', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    fetch_mongo_data = PythonOperator(
        task_id='fetch_mongo_data',
        python_callable=fetch_data_from_mongo,
        provide_context=True
    )

    # Задача: Отправка данных в Kafka
    producer = PythonOperator(
        task_id='send_to_kafka',
        python_callable=producer,
        provide_context=True
    )
    fetch_mongo_data>>producer