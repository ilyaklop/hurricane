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


def get_path():
    print(dag_path)
    try:
        file = pd.read_csv(r"./data/atlantic.csv", low_memory=False)
        print('file was read')
    except Exception as e:
        print(e)


def insert_data(ti):
    path = ti.xcom_pull(task_ids='get_path')
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    c = conn.cursor()
    with open("/opt/airflow/data/atlantic.csv", 'r') as f:
        c.copy_from(f, 'cyclones', sep=',')

    #c.execute(f"""
    #COPY cyclones(ID, Name, Date, Time, Event, Status, Latitude, Longitude,
    #Maximum_Wind, Minimum_Pressure, Low_Wind_NE, Low_Wind_SE, Low_Wind_SW, Low_Wind_NW,
    #Moderate_Wind_NE, Moderate_Wind_SE, Moderate_Wind_SW, Moderate_Wind_NW, High_Wind_NE,
    #High_Wind_SE, High_Wind_SW, High_Wind_NW)
    #FROM '/opt/airflow/data/atlantic.csv' DELIMITER ',' CSV HEADER""")


with DAG(dag_id="create_cyclones_v3", default_args=default_args, start_date=datetime(2022, 11, 1),
         schedule_interval='@daily') as dag:
    task1 = PostgresOperator(task_id='create_postgres_table', postgres_conn_id='postgres_localhost',
                             sql="""
                             create table if not exists cyclones ("ID" text,
                                                                  "Name" text,
                                                                  "Date" text,
                                                                  "Time" text,
                                                                  "Event" text,
                                                                  "Status" text,
                                                                  "Latitude" text,
                                                                  "Longitude" text,
                                                                  "Maximum Wind" INT,
                                                                  "Minimum Pressure" INT,
                                                                  "Low Wind NE" INT,
                                                                  "Low Wind SE" INT,
                                                                  "Low Wind SW" INT,
                                                                  "Low Wind NW" INT,
                                                                  "Moderate Wind NE" INT,
                                                                  "Moderate Wind SE" INT,
                                                                  "Moderate Wind SW" INT,
                                                                  "Moderate Wind NW" INT,
                                                                  "High Wind NE" INT,
                                                                  "High Wind SE" INT,
                                                                  "High Wind SW" INT,
                                                                  "High Wind NW" INT)""")

    task2 = PythonOperator(task_id='get_path', python_callable=get_path)

    task3 = PythonOperator(task_id='incert_data', python_callable=insert_data)
    #task3 = PostgresOperator(task_id='incert_data', postgres_conn_id='postgres_localhost',
    #                         sql=r"""COPY cyclones(ID, Name, Date, Time, Event, Status, Latitude, Longitude,
    #                         Maximum_Wind, Minimum_Pressure, Low_Wind_NE, Low_Wind_SE, Low_Wind_SW, Low_Wind_NW,
    #                         Moderate_Wind_NE, Moderate_Wind_SE, Moderate_Wind_SW, Moderate_Wind_NW, High_Wind_NE,
    #                         High_Wind_SE, High_Wind_SW, High_Wind_NW)
    #                         FROM '/data/atlantic.csv' DELIMITER ',' CSV HEADER""",
    #                         )
    task1 >> task2 >> task3