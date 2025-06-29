from datetime import datetime
from airflow import DAG
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

# Helper function to get config from ConfigMap
def get_spark_config(app_name):
    """Get Spark configuration from ConfigMap"""
    k8s_hook = KubernetesHook(conn_id="kubernetes_default")
    try:
        config_map = k8s_hook.get_configmap(name=app_name, namespace="airflow")
        return config_map.data
    except Exception:
        # Fallback defaults if ConfigMap doesn't exist
        return {
            "driver_cores": "1",
            "driver_memory": "1g",
            "driver_core_limit": "1200m",
            "executor_cores": "2",
            "executor_instances": "1",
            "executor_memory": "1g",
            "spark_version": "3.5.2",
            "java_home": "/opt/java/openjdk"
        }
#test
# Step 1: DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="test1",
    default_args=default_args,
    description="Runs daily car crash pipeline with config and date using Spark on Kubernetes",
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 20),
    catchup=False,
    params={
        "date": Param("2024-04-20", type="string"),
    },
    tags=["car_crash", "daily", "spark"],
) as dag:
    
    # Get Spark configuration from ConfigMap
    spark_config = get_spark_config("ingest-job-config-map")
    
    # Task B - Process data using Spark
    process_data = SparkKubernetesOperator(
        task_id="ingest-job",
        namespace="airflow",
        application_file={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": "process-data-{{ ds }}", "namespace": "airflow"},
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "image": "jaihind213/daily_pipeline_car_crash:0.0.4-0.1",
                "imagePullPolicy": "Always",
                "mainApplicationFile": "local:///opt/daily_pipeline_car_crash/ingest_job.py",
                "arguments": [
                    "/opt/daily_pipeline_car_crash/default_job_config.ini",
                    "{{ params.date }}"
                ],
                "sparkVersion": spark_config.get("spark_version", "3.5.2"),
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "cores": int(spark_config.get("driver_cores", "1")),
                    "coreLimit": spark_config.get("driver_core_limit", "1200m"),
                    "memory": spark_config.get("driver_memory", "1g"),
                    "serviceAccount": "spark",
                    "env": [
                        {"name": "JAVA_HOME", "value": spark_config.get("java_home", "/opt/java/openjdk")}
                    ],
                    "envFrom": [
                        {"secretRef": {"name": "car-crash-secret"}}
                    ]
                },
                "executor": {
                    "cores": int(spark_config.get("executor_cores", "1")),
                    "instances": int(spark_config.get("executor_instances", "1")),
                    "memory": spark_config.get("executor_memory", "1g"),
                    "env": [
                        {"name": "JAVA_HOME", "value": spark_config.get("java_home", "/opt/java/openjdk")}
                    ],
                    "envFrom": [
                        {"secretRef": {"name": "car-crash-secret"}}
                    ]
                }
            }
        },
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )
    
   
    # Define task dependencies: a > b > c
    #pull_data >> process_data >> upload_results
    process_data
