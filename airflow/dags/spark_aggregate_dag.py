from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'upbase',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='spark_aggregate_dag',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark_job = KubernetesPodOperator(
        task_id='run_spark_aggregate_events',
        name='spark-aggregate-events',
        namespace='default',
        image='upbase/spark:local',
        image_pull_policy='Never',
        service_account_name='default',
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",
            "--deploy-mode", "cluster",
            "--name", "aggregate-events",
            "--conf", "spark.kubernetes.container.image=upbase/spark:local",
            "--conf", "spark.kubernetes.namespace=default",
            "--conf", "spark.executor.instances=1",
            "local:///opt/spark-jobs/aggregate_events.py",
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )
