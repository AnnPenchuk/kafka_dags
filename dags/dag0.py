from datetime import timedelta, datetime
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator



# def list_files_in_directory():
#     directory_path = "./dags"
#     import os
#     import traceback
#     try:
#         if not os.path.exists(directory_path):
#             raise FileNotFoundError(f"Директория {directory_path} не существует.")
#
#         files = os.listdir(directory_path)
#         if not files:
#             print(f"Директория {directory_path} пуста.")
#         else:
#             print(f"Список файлов в {directory_path}:")
#             for file in files:
#                 print(file)
#     except Exception as e:
#         print("Произошла ошибка:")
#         traceback.print_exc()



def fetch_data_from_mongo():

    print("hell")
    from settings import settings
    pprint(settings)


# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
with DAG('dag0_test0', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    fetch_mongo_data = PythonOperator(
        task_id='fetch_mongo_data',
        python_callable=fetch_data_from_mongo,
        provide_context=True
    )
