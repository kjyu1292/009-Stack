from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta

def clear_table():
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='default', password='upbase123',
        database='upbase'
    )
    client.command("TRUNCATE TABLE events")

def generate_fresh_data(**context):
    num_rows = context["params"]["num_rows"]

    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='default', password='upbase123',
        database='upbase'
    )
    client.command(f"""
        INSERT INTO events (event_time, user_id, event_type)
        SELECT
            now() - INTERVAL rand() % 30 DAY,
            (rand() % 500) + 1,
            ['login', 'logout', 'purchase', 'signup', 'click', 'view_page'][(rand() % 6) + 1]
        FROM numbers({num_rows})
    """)

default_args = {
    'owner': 'upbase',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='generate_data_reset_dag',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"num_rows": Param(2000, type="integer", minimum=2000, maximum=100000,
                              description="Number of random rows to insert")},
) as dag:
    clear_task = PythonOperator(
        task_id='clear_events_table',
        python_callable=clear_table,
    )
    generate_task = PythonOperator(
        task_id='generate_fresh_events',
        python_callable=generate_fresh_data,
    )

    clear_task >> generate_task  # clear must finish before generating
