import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.utils.task_group import TaskGroup
import os

data_path = '/Users/oe/airflow/olist_data'

default_args = {
    'owner': 'oguz',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

table_list = ['sellers', 'product_category_name_translation', 'orders', 'order_items',
              'customers', 'geolocation', 'order_payments', 'order_reviews', 'products']


def csvToPostgres(table_name):
    # Open Postgres Connection
    # Get postgress connection object
    # connection = PostgresHook.get_connection("miuul_workshop")
    connection = PostgresHook.get_connection("pg_local")
    # get postgress URI
    URI = connection.get_uri()
    URI = URI.replace('postgres://', 'postgresql://')
    conn = create_engine(URI)
    # CSV loading to table.
    if table_name == 'product_category_name_translation':
        data = pd.read_csv(f'{data_path}/{table_name}.csv')
        print(f'{data_path}/{table_name}.csv')
    else:
        data = pd.read_csv(f'{data_path}/olist_{table_name}_dataset.csv')

        pass
    data.to_sql(f'{table_name}', if_exists='replace', index=False, con=conn)


with DAG(
        dag_id='olist_local_pg_etl',
        schedule_interval='@daily',
        start_date=datetime(year=2022, month=7, day=1),
        catchup=False
) as dag:
    task_get_data = BashOperator(task_id='get_data',
                                 bash_command=f'kaggle datasets download olistbr/brazilian-ecommerce -p {data_path}')

    task_check_file_exists = FileSensor(task_id='check_file_exists',
                                        filepath='brazilian-ecommerce.zip',
                                        fs_conn_id='my_file_path')

    task_extract_zip = BashOperator(task_id='extract_zip',
                                    bash_command=f'unzip -o {data_path}/brazilian-ecommerce.zip -d {data_path}')

    task_check_sub_file_exists = FileSensor(task_id='check_sub_file_exists',
                                            filepath='olist_customers_dataset.csv',
                                            fs_conn_id='my_file_path')

    with TaskGroup("load", dag=dag) as load:
        for table_name in table_list:
            table_name = PythonOperator(
                default_args=default_args,
                task_id=table_name,
                python_callable=csvToPostgres,
                op_kwargs={'table_name': table_name},
                dag=dag)

    task_get_data >> task_check_file_exists >> task_extract_zip >> task_check_sub_file_exists >> load
