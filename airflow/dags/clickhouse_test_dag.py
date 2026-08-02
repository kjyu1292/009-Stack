from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def insert_event():
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='default', password='upbase123',
        database='upbase'
    )
    client.command("""INSERT INTO events (event_time, user_id, event_type) VALUES (now(), 99, 'airflow_test')""")

default_args = {
    'owner': 'upbase',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='clickhouse_test_dag',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    insert_task = PythonOperator(
        task_id='insert_event_into_clickhouse',
        python_callable=insert_event,
    )
