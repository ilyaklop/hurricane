from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
import pandas as pd
import numpy as np


default_args = {
    'owner': 'ilia',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id="create_history_v1", default_args=default_args, start_date=datetime(2013, 1, 1),
          schedule_interval='@monthly', catchup=False)


def check_status(row, yest):
    """если за текщий день нет данных о вчерашнем циклоне, то считаем, что он закончился.причем вчера  """
    if row['Status_hist'] != row['Status_curr']:
        if pd.isna(row['Status_hist']):
            row['Status_hist'] = row['Status_curr']
            row['start_date'] = row['Date']
        elif pd.isna(row['Status_curr']):
            row['end_date'] = yest  # ставим вчерашнюю дату!
        else:
            row['end_date'] = yest  # ставим вчерашнюю дату!
    else:
        pass
    return row


def choose_new_stat(row):
    new_row = []
    if (row['Status_hist']!=row['Status_curr']) and (not pd.isna(row['Status_hist'])) and (not pd.isna(row['Status_curr'])):
        new_row = [row['ID'], row['Status_curr'], row['Date'], np.nan, np.nan, np.nan]
    else:
        pass
    return new_row


def data_to_query(df, yest):
    df_update = df[~df['end_date'].isna()][['ID', 'Status', 'start_date']]
    df_insert = df[df['end_date'].isna()][['ID', 'Status', 'start_date']]
    query = ""

    def make_values_list(row, query):
        str_row=', '.join(str(item) for item in row)
        t_query = f"""INSERT INTO public.cyclones_history(id, status, start_date) VALUES({str_row}) /n"""
        query = query + t_query
        return query

    if (df_update.empty) and (not df_insert.empty):
        d_insert = df_insert.apply(lambda x: make_values_list(x, query), axis=1)
        d_insert.reset_index(drop=True, inplace=True)
        query_insert = d_insert[0]
        return query_insert
    elif (not df_update.empty) and (df_insert.empty):
        id_list= list(df_update['ID'])
        str_id = ', '.join(str(item) for item in id_list)
        query_update = f"""UPDATE ONLY public.cyclones_history SET end_date ={yest} WHERE end_date is NULL and id in ({str_id})
        """
        return query_update
    elif (not df_update.empty) and (not df_insert.empty):
        id_list= list(df_update['ID'])
        str_id = ', '.join(str(item) for item in id_list)
        query_update = f"""UPDATE ONLY public.cyclones_history SET end_date ={yest} WHERE end_date is NULL and id in ({str_id})
        """
        d_insert = df_insert.apply(lambda x: make_values_list(x, query), axis=1)
        d_insert.reset_index(drop=True, inplace=True)
        query_insert = d_insert[0]
        query = query_update + query_insert
        return query
    else:
        return """select 400""" #"unexpected error"


def load_history(curr_date, yest_date):
    """Подгружаем данные из файла и подргужаем данные из таблицы хистори. Обрабатываем в пандасе.
    Перезаписываем обратно?!
    """
    p_conn = PostgresHook(postgres_conn_id="postgres_localhost").get_conn()
    try:
        day_df = pd.read_csv(f'/opt/airflow/data/cyclones_{curr_date}.csv', sep=',')
        load_query = """select id, status, start_date, end_date
        from public.cyclones_history where end_date is NULL"""
        sql_df = pd.read_sql_query(load_query, p_conn)

        if sql_df.empty:
            # из первого файла формируем начальные статусы
            day_df['end_date'] = np.nan  # тут
            day_df.rename({'Date': 'start_date'}, axis=1, inplace=True)
            history = day_df.copy()
        else:
            history = pd.merge(sql_df, day_df, on='ID', how='outer', suffixes=('_hist', '_curr'))
            history = history[history['Status_curr'] != history['Status_hist']]  # тут
            history = history.apply(lambda x: check_status(x, yest_date), axis=1)
            new_rows = history.apply(lambda x: choose_new_stat(x), axis=1)
            if new_rows.str.len().sum() == 0:
                pass
            else:
                new_rows_df = pd.DataFrame(
                    columns=['ID', 'Status_hist', 'start_date', 'end_date', 'Date', 'Status_curr'],
                    data=[item for item in new_rows])
                new_rows_df.dropna(how='all', inplace=True)
                history = pd.concat([history, new_rows_df])
            #
            history.drop(['Date', 'Status_curr'], axis='columns', inplace=True)
            history.rename({'Status_hist': 'Status'}, axis=1, inplace=True)
            history['start_date'] = history['start_date'].astype(str)
        upload_query = data_to_query(history, yest_date)
        with open("sql/upload_query.sql", 'w') as f:
            f.write(upload_query)
    except FileNotFoundError:
        """возможно обращаемся к базе и закрываем все открытые статусы напрямую инсертом"""
        upload_query = f"""UPDATE ONLY cyclones_history SET end_date = '{yest_date}'""" #where id = {id} and status= {status}
        with open("sql/upload_query.sql", 'w') as f:
            f.write(upload_query)


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
                         sql="sql/upload_query.sql", dag=dag)

task1>>task2>>task3
