from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

with DAG(
    dag_id="spark_hello_world_k8s",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # no cliente será provavelmente um cron
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=["spark", "kubernetes", "prod"],
) as dag:

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark_hello",
        namespace="data-platform",
        # caminho dentro do container do scheduler (já explico abaixo)
        application_file="k8s/spark-hello.yaml",
        do_xcom_push=True,
    )

    monitor_spark = SparkKubernetesSensor(
        task_id="monitor_spark_hello",
        namespace="data-platform",
        application_name="{{ task_instance.xcom_pull('submit_spark_hello')['metadata']['name'] }}",
        attach_log=True,
    )

    submit_spark >> monitor_spark