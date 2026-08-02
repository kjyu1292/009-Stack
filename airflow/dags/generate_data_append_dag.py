from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.models.param import Param
from datetime import datetime, timedelta

def generate_and_append(**context):
    #num_rows = context["params"]["num_rows"]

    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='default', password='upbase123',
        database='upbase'
    )
    client.command("""
        INSERT INTO events (event_time, user_id, event_type)
        SELECT
            now() - INTERVAL rand() % 30 DAY,
            (rand() % 500) + 1,
            ['login', 'logout', 'purchase', 'signup', 'click', 'view_page'][(rand() % 6) + 1]
        FROM numbers(500)
    """)

default_args = {
    'owner': 'upbase',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='generate_data_append_dag',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    #params = {"num_rows": Param(500, type="integer", minimum=1, maximum=1000,
    #                           description="Number of random rows to append")},
) as dag:
    append_task = PythonOperator(
        task_id='generate_and_append_events',
        python_callable=generate_and_append,
    )
