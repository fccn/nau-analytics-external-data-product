from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# DAG simples só para testar o fluxo fim-a-fim
with DAG(
    dag_id="spark_hello_world_k8s",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # trigger manual via UI
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=["demo", "spark", "kubernetes"],
) as dag:

    spark_hello = KubernetesPodOperator(
        task_id="spark_hello",
        name="spark-hello-world",
        namespace="data-platform",
        image="nauedu/nau-analytics-external-data-product:feat_add_jupyter_to_dockerfile",
        cmds=["bash", "-lc"],
        arguments=["python /opt/spark/src/jobs/hello_spark.py"],
        is_delete_operator_pod=True,
        in_cluster=True,
    )