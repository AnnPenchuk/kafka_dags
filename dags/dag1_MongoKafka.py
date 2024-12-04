from datetime import datetime, timedelta

import json

import confluent_kafka
from bson import ObjectId
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from confluent_kafka import Producer, Consumer



#Функция для преобразования данных MongoDB (с ObjectId) в JSON
def mongo_to_json(doc):
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
    return doc

def convert_datetimes(obj):
    if isinstance(obj, dict):
        return {key: convert_datetimes(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetimes(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

# Функция для подключения к MongoDB и извлечения данных
def fetch_data_from_mongo():
    mongo_url = "mongodb+srv://smartintegratordev:YV1ZVX0XbmgGfaq0@cluster0.9kxy2.mongodb.net"
    database_name = "data_platform_si"
    collection_name = "Receipt"

    # Подключение к MongoDB
    client = MongoClient(mongo_url)
    db = client[database_name]
    collection = db[collection_name]

    # Пример: Извлечение всех документов
    documents = list(collection.find().limit(1))
    print(f"Извлечено {len(documents)} документов из MongoDB")
    documents_json=[]
    for doc in documents:
        doc = convert_datetimes(doc)
        doc = mongo_to_json(doc)
        print(doc)
        documents_json.append(doc)
    # Закрытие подключения
    client.close()
    return documents_json

#
# def create_topic():
#         # Подключение к Kafka
#         admin_client = AdminClient({
#             'bootstrap.servers': 'localhost:9092'
#         })
#         topic_name = "topic"
#         num_partitions = 1
#         replication_factor = 1
#
#         # Подготовка топика
#         # Проверяем, существует ли топик
#         existing_topics = admin_client.list_topics(timeout=10).topics
#         if topic_name in existing_topics:
#             print(f"Топик '{topic_name}' уже существует.")
#             return
#         # Создаем топик
#         new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
#
#
#         # Отправляем запрос на создание
#         futures = admin_client.create_topics([new_topic])
#         for topic, future in futures.items():
#                     try:
#                      future.result()  # Ждем завершения
#                      print(f"Топик {topic} успешно создан!")
#                     except Exception as e:
#                         print(f"Ошибка создания топика {topic}: {e}")
#                         continue
#create_topic()

# создание топика


#
# # Функция для отправки данных в Kafka
# def send_data_to_kafka(**kwargs):
#     # """
#     #     Отправляет сообщения через kafka
#     #     :param user_json: полученный пользователь
#     #     """
#     #
#     # producer = Producer(
#     #     {
#     #         "bootstrap.servers": "kafka:9092",
#     #         "sasl.mechanism": "SCRAM-SHA-256",
#     #         "security.protocol": "SASL_SSL"
#     #     }
#     # )
#     # user_json=fetch_data_from_mongo
#     # data_json = json.dumps(user_json)
#     #
#     #
#     # topic = "my_topic"
#     # producer.produce(topic, value=data_json)
#     # producer.flush()
#
#     kafka_bootstrap_servers = "kafka:9092"  # Адрес Kafka
#     topic_name = "topic"  # Название топика Kafka
#
#     # Создание Kafka Producer
#     producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})
#
#     # Получение данных из предыдущей задачи (XCom)
#     #documents = kwargs['ti'].xcom_pull(task_ids='fetch_mongo_data')
#     documents=fetch_data_from_mongo()
#     if not documents:
#         print("Нет данных для отправки в Kafka")
#         return
#
#     # Отправка данных в Kafka
#     for doc in documents:
#         producer.produce(topic_name, value=json.dumps(doc))
#         # Отправка сообщения
#     # Очистка буфера
#         producer.flush()
#         print(f"Отправлено сообщение в Kafka: {doc}")
#
# def test():
#     # consumer = Consumer({
#     #     'bootstrap.servers': 'harmless-llama-10955-eu2-kafka.upstash.io:9092',
#     #     'sasl.mechanism': 'SCRAM-SHA-256',
#     #     'security.protocol': 'SASL_SSL',
#     #     'group.id': 'YOUR_CONSUMER_GROUP',
#     #     'auto.offset.reset': 'earliest'
#     # })
#     # consumer.subscribe(["my_topic"])
#     #
#     # try:
#     #     while True:
#     #         msg = consumer.poll(1.0)
#     #         if msg is None:
#     #             continue
#     #
#     #         if msg.error():
#     #             print(f"Consumer error: {msg.error()}")
#     #             continue
#     #
#     #         data = json.loads(msg.value().decode('utf-8'))
#     #         return data
#     # finally:
#     #     consumer.close()
#     conf = {
#         'bootstrap.servers': 'localhost:9092',
#         'group.id':  'test_goup_for_airflow',
#         'auto.offset.reset': 'earliest'
#     }
#
#     consumer = Consumer(conf)
#     consumer.subscribe(['topic'])
#
#     try:
#         while True:
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 print("нет сообщений")
#                 continue
#             if msg.error():
#                 print(f"Ошибка: {msg.error()}")
#                 continue
#             print(f"Получено сообщение: {msg.value().decode('utf-8')}")
#
#     finally:
#         consumer.close()
#test()
#fetch_data_from_mongo()
#send_data_to_kafka()






# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
with DAG('mongo_to_kafka', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    fetch_mongo_data = PythonOperator(
        task_id='fetch_mongo_data',
        python_callable=fetch_data_from_mongo,
        provide_context=True
    )

    # create_topic = PythonOperator(
    #     task_id='create_topic',
    #     task_id='create_topic',
    #     python_callable=create_topic,
    #     provide_context=True
    # )

    # # Задача: Отправка данных в Kafka
    # send_to_kafka = PythonOperator(
    #     task_id='send_to_kafka',
    #     python_callable=send_data_to_kafka,
    #     provide_context=True
    # )

    # test = PythonOperator(
    #     task_id='test',
    #     python_callable=test,
    #     provide_context=True
    # )

    #Последовательность выполнения
    #fetch_mongo_data >>create_topic>> send_to_kafka>>test
