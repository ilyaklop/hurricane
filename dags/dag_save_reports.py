from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging


default_args = {
    'owner': 'ilia',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id="create_history_v10", default_args=default_args, start_date=datetime(2013, 1, 1),
          schedule_interval='@daily', catchup=False, max_active_runs=1)
logger = logging.getLogger(__name__)

def check_status(row, yest):
    """если за текщий день нет данных о вчерашнем циклоне, то считаем, что он закончился.причем вчера  """
    if row['status_hist'] != row['status_curr']:
        if pd.isna(row['status_hist']):
            row['status_hist'] = row['status_curr']
            row['start_date'] = row['date']
        elif pd.isna(row['status_curr']):
            row['end_date'] = yest  # ставим вчерашнюю дату!
        else:
            row['end_date'] = yest  # ставим вчерашнюю дату!
    else:
        pass
    return row


def choose_new_stat(row):
    new_row = []
    if (row['status_hist'] != row['status_curr']) and (not pd.isna(row['status_hist'])) and (not pd.isna(row['status_curr'])):
        new_row = [row['id'], row['status_curr'], row['date'], np.nan, np.nan, np.nan]
    else:
        pass
    return new_row


def data_to_query(df, yest):
    df_update = df[~df['end_date'].isna()][['id', 'status', 'start_date']]
    df_insert = df[df['end_date'].isna()][['id', 'status', 'start_date']]

    def make_insert_string(row):
        """НА выходе получаем структуру ('item1', 'item2', 'item3')"""
        return "('" + "', '".join(str(item) for item in row) + "')"

    if (df_update.empty) and (not df_insert.empty):
        d_insert = df_insert.apply(lambda x: make_insert_string(x), axis=1)
        str_row = ", ".join(str(item) for item in d_insert)
        query_insert = f"""INSERT INTO public.cyclones_history(id, status, start_date) VALUES {str_row}"""
        return query_insert
    elif (not df_update.empty) and (df_insert.empty):
        id_set = set(df_update['id'])
        str_id = "'" + "', '".join(str(item) for item in id_set) + "'"
        query_update = f"""UPDATE ONLY public.cyclones_history SET end_date ={yest} WHERE end_date is NULL and id in ({str_id})
        """
        return query_update
    elif (not df_update.empty) and (not df_insert.empty):
        id_set = set(df_update['id'])
        str_id = "'" + "', '".join(str(item) for item in id_set) + "'"
        query_update = f"""UPDATE ONLY public.cyclones_history SET end_date ={yest} WHERE end_date is NULL and id in ({str_id})
        """
        d_insert = df_insert.apply(lambda x: make_insert_string(x), axis=1)
        str_row = ", ".join(str(item) for item in d_insert)
        query_insert = f"""INSERT INTO public.cyclones_history(id, status, start_date) VALUES {str_row}"""
        query = f"""WITH up_data AS ({query_update})\n {query_insert}"""
        return query
    else:
        return """select 400""" #"unexpected error"


def load_history(curr_date, yest_date):
    """Подгружаем данные из файла и подргужаем данные из таблицы хистори. Обрабатываем в пандасе.
    Перезаписываем обратно?!
    """
    p_conn = PostgresHook(postgres_conn_id="postgres_localhost").get_conn()
    load_query = """select id, status, start_date, end_date
            from public.cyclones_history where end_date is NULL"""
    sql_df = pd.read_sql_query(load_query, p_conn)
    try:
        day_df = pd.read_csv(f'/opt/airflow/data/cyclones_{curr_date}.csv', sep=',')
        if sql_df.empty:
            logger.info("i'm in sql-empty branch")
            # здесь всегда новые начальные статусы
            day_df['end_date'] = np.nan  # тут
            day_df.rename({'date': 'start_date'}, axis=1, inplace=True)
            history = day_df.copy()
        else:
            logger.info("i'm not in sql-empty branch")
            history = pd.merge(sql_df, day_df, on='id', how='outer', suffixes=('_hist', '_curr'))
            #вот это можно переписать с ON CONFLICT в sql
            history = history[history['status_curr'] != history['status_hist']]
            if history.empty:
                logger.info('history is empty')
                upload_query = """select 400"""
                return upload_query
            else:
                history = history.apply(lambda x: check_status(x, yest_date), axis=1)
                new_rows = history.apply(lambda x: choose_new_stat(x), axis=1)
                if new_rows.str.len().sum() == 0:
                    pass
                else:
                    new_rows_df = pd.DataFrame(
                        columns=['id', 'status_hist', 'start_date', 'end_date', 'date', 'status_curr'],
                        data=[item for item in new_rows])
                    new_rows_df.dropna(how='all', inplace=True)
                    history = pd.concat([history, new_rows_df])
                #
                history.drop(['date', 'status_curr'], axis='columns', inplace=True)
                history.rename({'status_hist': 'status'}, axis=1, inplace=True)
                history['start_date'] = history['start_date'].astype(str)
        upload_query = data_to_query(history, yest_date)
    except FileNotFoundError:
        logger.info("i'm in exception branch")
        """возможно обращаемся к базе и закрываем все открытые статусы напрямую"""
        if sql_df.empty:
            upload_query = """select 400"""
        else:
            upload_query = f"""UPDATE ONLY cyclones_history SET end_date = '{yest_date}' where end_date is NULL"""
    return upload_query


task1 = PostgresOperator(task_id='create_postgres_table', postgres_conn_id='postgres_localhost',
                         sql="""
                         create table if not exists cyclones_history (start_date text,
                                                                      end_date text,
                                                                      id text,
                                                                      status text)
                         """, dag=dag)

task2 = PythonOperator(task_id='load_history', python_callable=load_history,
                       op_kwargs={'curr_date': "{{ ds_nodash }}", 'yest_date': "{{ yesterday_ds_nodash }}"}, dag=dag)

task3 = PostgresOperator(task_id='upload_data', postgres_conn_id='postgres_localhost',
                         sql="{{ ti.xcom_pull(task_ids='load_history', key='return_value') }}", dag=dag)

task4 = BashOperator(task_id='del_file',
                     bash_command="""if [ -f /opt/airflow/data/cyclones_{{ ds_nodash }}.csv ]; then 
                       rm /opt/airflow/data/cyclones_{{ ds_nodash }}.csv 
                      fi""",
                     env={"DAY": "{{ ds_nodash }}"}, dag=dag)

# я решил вынести скрипт отката истории в отдельный файл, для удобства запуска исторического расчета
# задумка такая: текущим дагом выполняем спокойно рачсчет за указанный промежуток времени, без задержек на сенсор
# и дагом check_updated_file выполняем при необходимости откат истории, после чего опять запускаем текущий даг и
# на выходе получаем новую историю. В prod версии можно эти таски перенести в dag_save_reports

task1>>task2>>task3>>task4
