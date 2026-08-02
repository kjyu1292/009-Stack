FROM apache/superset:latest
USER root
RUN pip install --target=/app/.venv/lib/python3.10/site-packages psycopg2-binary clickhouse-connect
USER superset
