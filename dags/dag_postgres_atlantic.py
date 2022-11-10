from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pandas as pd
import os

dag_path = os.getcwd()
default_args = {
    'owner': 'ilia',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def insert_data():
    # это legacy - проблема с COPY - постоянный конфликт с типами, лучше использовтаь интерфейс DBeaver
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    c = conn.cursor()
    with open("/opt/airflow/data/atlantic.csv", 'r') as f:
        c.copy_from(f, 'cyclones', sep=',', null='')


with DAG(dag_id="create_cyclones_v4", default_args=default_args, start_date=datetime(2022, 11, 1),
         schedule_interval='@daily', catchup=False) as dag:
    task1 = PostgresOperator(task_id='create_postgres_table', postgres_conn_id='postgres_localhost',
                             sql="""
                             create table if not exists cyclones ("id" text,
                                                                  "name" text,
                                                                  "date" text,
                                                                  "time" text,
                                                                  "event" text,
                                                                  "status" text,
                                                                  "latitude" text,
                                                                  "longitude" text,
                                                                  "maximum_wind" INT,
                                                                  "minimum_pressure" INT,
                                                                  "low_wind_ne" INT,
                                                                  "low_wind_se" INT,
                                                                  "low_wind_sw" INT,
                                                                  "low_wind_nw" INT,
                                                                  "moderate_wind_ne" INT,
                                                                  "moderate_wind_se" INT,
                                                                  "moderate_wind_sw" INT,
                                                                  "moderate_wind_nw" INT,
                                                                  "high_wind_ne" INT,
                                                                  "high_wind_se" INT,
                                                                  "high_wind_sw" INT,
                                                                  "high_wind_nw" INT)""")


    task3 = PythonOperator(task_id='incert_data', python_callable=insert_data)
    #task3 = PostgresOperator(task_id='incert_data', postgres_conn_id='postgres_localhost',
    #                         sql=r"""COPY cyclones(ID, Name, Date, Time, Event, Status, Latitude, Longitude,
    #                         Maximum_Wind, Minimum_Pressure, Low_Wind_NE, Low_Wind_SE, Low_Wind_SW, Low_Wind_NW,
    #                         Moderate_Wind_NE, Moderate_Wind_SE, Moderate_Wind_SW, Moderate_Wind_NW, High_Wind_NE,
    #                         High_Wind_SE, High_Wind_SW, High_Wind_NW)
    #                         FROM '/data/atlantic.csv' DELIMITER ',' CSV HEADER""",
    #                         )
    task1 >> task3