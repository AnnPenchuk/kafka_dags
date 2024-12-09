import logging
from datetime import datetime, timedelta
from time import time
from pymongo import MongoClient
from bson import json_util
import json


from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

MODULE_NAME = "logs.txt"

MONGO_HOST = "mongodb+srv://smartintegratordev:YV1ZVX0XbmgGfaq0@cluster0.9kxy2.mongodb.net/"

KAFKA_URL = '172.30.181.198:9092'
TOPIC_NAME = 'data.travelagency.avro.receipt1'

SCHEMA_REGISTRY_URL = "http://172.30.181.198:8085"
#SCHEMA_NAME = "data.travelagency.avro.receipt1"


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
#json_result =json_result[0]
print(json_result)

producer_conf = {
    'bootstrap.servers': KAFKA_URL,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'PLAINTEXT',
    'group.id': 'test_prod',	# produce_group
    'auto.offset.reset': 'earliest',
}

schema_registry_conf = {
    'url': SCHEMA_REGISTRY_URL,
}

# SCHEMA_VALUE = "reciept_value.json"
#
# with open(f"{SCHEMA_VALUE}", "r", encoding="utf-8") as f:
#     deployment_schema_value = f.read()

#print(deployment_schema_value)

schema_registry_client = SchemaRegistryClient(
	schema_registry_conf
)
#
# try:
#     schema_registry_client.register_schema(
#         subject_name=SCHEMA_NAME,
#         schema=Schema(schema_str=deployment_schema_value, schema_type='AVRO')
#     )
#
#     schema_registry_client.set_compatibility(SCHEMA_NAME, 'BACKWARD')
#     logging.info(f"Create new schema for {SCHEMA_NAME}.")
# except Exception as e:
#     logging.error(f"Error creating schema {SCHEMA_NAME}: {e}")

admin_client = AdminClient(
    {
        'bootstrap.servers': KAFKA_URL,
    }
)

# Edit if need
num_partitions = 1
replication_factor = 1

if TOPIC_NAME not in admin_client.list_topics(timeout=10).topics:
    NewTopic(
        TOPIC_NAME,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    logging.info(f'Create topyk {TOPIC_NAME}')


producer = Producer(producer_conf)

# schema_registry_client = SchemaRegistryClient(schema_registry_conf)

version: str|int = "latest"  # 1

# schema_value_version = schema_registry_client.get_version(SCHEMA_NAME, version)
#
# registered_schema_value = schema_value_version.schema.schema_str
#
# avro_serializer_value = AvroSerializer(schema_registry_client, registered_schema_value)


SCHEMA_KEY = "data.avro.receipt_key"
SCHEMA_VALUE = "data.avro.receipt_value"
#
# with open(f"./schemas/V1__reciept_key.json", "r", encoding="utf-8") as f:
#     deployment_schema_key = f.read()
#
# print(deployment_schema_key)
#
# with open(f"./schemas/reciept_value.json", "r", encoding="utf-8") as f:
#     deployment_schema_value = f.read()
#
# print(deployment_schema_value)


schema_registry_conf = {
    'url': 'http://172.30.181.198:8085',

}

#producer = Producer(producer_conf)

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

topic = 'data.travelagency.avro.receipt'
schema1 = 'data.avro.receipt_value'
schema2 = 'data.avro.receipt_key'
#version: str | int = "latest"  # 1

schema_version1 = schema_registry_client.get_version(schema1, version)
schema_version2 = schema_registry_client.get_version(schema2, version)
registered_schema1 = schema_version1.schema.schema_str
registered_schema2 = schema_version2.schema.schema_str

#avro_serializer = AvroSerializer(schema_registry_client, registered_schema)

producer = Producer(producer_conf)

avro_serializer_key = AvroSerializer(schema_registry_client, registered_schema2)
avro_serializer_value = AvroSerializer(schema_registry_client, registered_schema1)

msgs = json.loads(
  	json_result
		.replace("$", "")
		.replace('"names"', '"passengers"')
    	.replace('"number_of_names"', '"number_of_passengers"')
      	.replace('"passenger"', '"name"')
    )




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
    try:
        key_msg = get_key(msg=msg, pos=pos)

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

        logging.info(f"Send msg: {key_msg}")

    except Exception as e:
        logging.error(f"{e}\nkey:{key_msg}\nvalue:{msg}")

position = 0

for msg in msgs:
	send_msg(msg, position)
	position += 1


def send_msg(msg: dict, pos: int) -> None:
    key_msg = ":".join(map(str, list(get_key(msg, pos=pos).values())))  # type str

    producer.produce(
        topic=TOPIC_NAME,
        key=StringSerializer()(
            key_msg,
            SerializationContext(schema2, MessageField.KEY)
        ),
        value=StringSerializer()(
            json.dumps(msg),
            SerializationContext(schema1, MessageField.VALUE)
        )
    )

    producer.flush()

    logging.info(f"Send msg: {key_msg}")



position = 0

for msg in msgs:
	send_msg(msg, position)
	position += 1