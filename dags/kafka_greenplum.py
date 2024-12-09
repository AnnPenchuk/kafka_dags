from airflow import DAG
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'kafka_to_greenplum',
    default_args=default_args,
    description='Read data from Kafka via PXF and write to Greenplum',
    schedule_interval='@hourly',  # Задайте нужный интервал
    start_date=days_ago(1),
    catchup=False,
)

# 1. Создание внешней таблицы для Kafka
create_external_table = PostgresOperator(
    task_id='create_external_table',
    postgres_conn_id='greenplum_conn',  # Настройте подключение в Airflow
    sql="""
    CREATE EXTERNAL TABLE IF NOT EXISTS kafka_topic_data (
        column1 TEXT,
        column2 INT,
        column3 DATE
    )
    LOCATION ('pxf://your_kafka_topic?PROFILE=kafka&SERVER=default')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """,
    dag=dag,
)

# 2. Создание внутренней таблицы для данных
create_internal_table = PostgresOperator(
    task_id='create_internal_table',
    postgres_conn_id='greenplum_conn',
    sql="""
    CREATE TABLE IF NOT EXISTS greenplum_table (
        column1 TEXT,
        column2 INT,
        column3 DATE
    );
    """,
    dag=dag,
)

# 3. Загрузка данных из Kafka в Greenplum
load_data = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='greenplum_conn',
    sql="""
    INSERT INTO greenplum_table
    SELECT * FROM kafka_topic_data;
    """,
    dag=dag,
)

# Задаем последовательность выполнения задач
create_external_table >> create_internal_table >> load_data
