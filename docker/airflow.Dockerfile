FROM apache/airflow:3.2.2
RUN pip install --no-cache-dir clickhouse-connect
COPY airflow/dags/ /opt/airflow/dags/
