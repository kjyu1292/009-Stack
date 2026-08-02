FROM apache/spark:3.5.1

USER root
RUN pip install "clickhouse-connect==0.6.8" pandas

COPY spark-jobs/ /opt/spark-jobs/

USER spark
