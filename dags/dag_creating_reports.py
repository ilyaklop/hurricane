from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import calendar
import os
import pandas as pd


dag_path = os.getcwd()
default_args = {
    'owner': 'ilia',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id="create_reports_v1", default_args=default_args, start_date=datetime(2013, 1, 1),
          schedule_interval='@monthly', catchup=False)

#скорее всего тут не юзер дэйт а ставим {dt} шедулер на monthly и прогоняем все до последней даты
def load_daily_cyclons(start_date, end_date):
    """Подключаемся к базе, выкачиваем и группируем данные. Сохраняем в файлы csv
    Должен ли быть сгенерирован  пустой файл, если данных за день нет?
    """
    p_conn = PostgresHook(postgres_conn_id="postgres_localhost").get_conn()

    query = f"""select id, date, status 
    from
    (SELECT id, "date", max("time") as max_time, max(status) as status
    FROM public.cyclones
    where to_date("date", 'YYYYMMDD') >=to_date('{start_date}', 'YYYY-MM-DD') and 
    to_date("date", 'YYYYMMDD') <to_date('{end_date}', 'YYYY-MM-DD')
    group by id, date) x"""

    df = pd.read_sql_query(query, p_conn)
    if df.empty:
        raise AirflowSkipException('No rows to load')

    for item in set(df['date']):
        tmp = df[df['date'] == item]
        tmp.to_csv(f'/opt/airflow/data/cyclones_{item}.csv')


task1 = PythonOperator(task_id='load_daily_cyclons', python_callable=load_daily_cyclons,
                       op_kwargs={'start_date': "{{ ds }}", 'end_date': "{{ next_ds }}"}, dag=dag)
task1


