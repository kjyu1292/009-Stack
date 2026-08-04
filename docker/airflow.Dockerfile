FROM apache/airflow:3.2.2
RUN pip install --no-cache-dir \
	    clickhouse-connect \
	    numpy \
	    polars \
	    pyarrow
COPY airflow/dags/ /opt/airflow/dags/
