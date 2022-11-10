from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta


default_args = {
    'owner': 'ilia',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id="check_updated_file_v2", default_args=default_args, start_date=datetime(2022, 11, 8),
          schedule_interval='@daily', catchup=False, max_active_runs=1)

task1 = FileSensor(task_id='sensor_file',
                   filepath='/opt/airflow/data/cyclones_{{ ds_nodash }}.csv',
                   timeout=86400, poke_interval=20, dag=dag)  #параметры чтобы удобно было тестировать

task2 = PostgresOperator(task_id='rollback_data', postgres_conn_id="postgres_localhost",
                         sql=r"""WITH up_data AS (UPDATE ONLY cyclones_history SET end_date = NULL 
                         WHERE to_date(end_date, 'YYYYMMDD')=to_date('{{ yesterday_ds }}',  'YYYY-MM-DD'))
                         DELETE FROM cyclones_history WHERE to_date(start_date, 'YYYYMMDD')=to_date('{{ ds }}',  'YYYY-MM-DD')""",
                         parameters={"yest_date": " {{ yesterday_ds }}", "curr_date": "{{ ds }}"}, dag=dag)

task1>>task2