from io import BytesIO
from typing import Any, Dict
import confluent_kafka
from airflow import DAG
from airflow.models import Connection
#from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

#from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.utils import db
from confluent_kafka import Consumer, KafkaException
import boto3
import json
import fastavro
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField



def consume_kafka(**kwargs):
    schema_registry_client = SchemaRegistryClient({
        'url': "http://172.30.181.198:8085",
    })

    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer = Consumer({
        'bootstrap.servers': '172.30.181.198:9092',
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'PLAINTEXT',
        'group.id': 'prodcy',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(["data.travelagency.avro.receipt"])

    while True:
        msg = consumer.poll(5.0)  # Wait for a message for 3 seconds

        if msg is None:
            print('No message received')
        elif msg.error():
            print('Error: {}'.format(msg.error()))
        else:
            print("Received message:", msg.value())
            print("Received message:", msg.key())
    consumer.close()


#Создаем DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('read_Kafka', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    consume_from_topic = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_kafka,
        provide_context=True,  # Pass Airflow context to the function
    )
