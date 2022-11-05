FROM apache/airflow:2.4.2
RUN pip install --upgrade pip
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
USER airflow
COPY /data/atlantic.csv /opt/airflow/data/atlantic.csv
COPY /sql/upload_query.sql /opt/airflow/sql/upload.sql
USER root
RUN chmod 777 /opt/airflow/data
RUN chmod 777 /opt/airflow/sql
USER airflow


