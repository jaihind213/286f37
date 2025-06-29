from datetime import datetime
import tempfile
import yaml
import os
from airflow import DAG
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

# Helper function to get config from ConfigMap
def get_spark_config(config_map_name="spark-config"):
    """Get Spark configuration from ConfigMap"""
    k8s_hook = KubernetesHook(conn_id="kubernetes_default")
    try:
        api_client = k8s_hook.get_conn()
        v1 = client.CoreV1Api(api_client)
        
        config_map = v1.read_namespaced_config_map(
            name=config_map_name,
            namespace="airflow"
        )
        
        return config_map.data
    except Exception:
        # Fallback defaults if ConfigMap doesn't exist
        import traceback
        traceback.print_exc()
        print("xxxxxxxxxxx")
        print("xxxxxxxxxxx")
        return {
            "driver_cores": "1",
            "driver_memory": "1g",
            "driver_core_limit": "1200m",
            "executor_cores": "2",
            "executor_instances": "1",
            "executor_memory": "2g",
            "spark_version": "3.5.2",
            "java_home": "/opt/java/openjdk"
        }

# Helper function to create Spark application YAML file
def create_spark_app_file(task_name, main_file, spark_config):
    """Create a temporary YAML file for Spark application"""
    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"{task_name}-{{{{ ds }}}}",
            "namespace": "airflow"
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": spark_config.get("image", "jaihind213/daily_pipeline_car_crash:0.0.6-0.1"),
            "imagePullPolicy": "Always",
            "mainApplicationFile": f"local:///opt/daily_pipeline_car_crash/{main_file}",
            "arguments": [
                "/opt/daily_pipeline_car_crash/default_job_config.ini",
                "{{ params.date }}"
            ],
            "sparkVersion": spark_config.get("spark_version", "3.5.2"),
            "restartPolicy": {"type": "Never"},
            "sparkConf": {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.jar.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.1,io.github.jaihind213:spark-set-udaf:spark3.5.2-scala2.13-1.0.1-jdk11,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
                "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.local.type": "hadoop",
                "spark.sql.catalog.local.warehouse": "file:///opt/daily_pipeline_car_crash/data/iceberg_crashes",
                "spark.driver.extraClassPath": "/opt/spark_jars/*",
                "spark.executor.extraClassPath": "/opt/spark_jars/*"
            },
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
                "cores": int(spark_config.get("executor_cores", "2")),
                "instances": int(spark_config.get("executor_instances", "1")),
                "memory": spark_config.get("executor_memory", "2g"),
                "env": [
                    {"name": "JAVA_HOME", "value": spark_config.get("java_home", "/opt/java/openjdk")}
                ],
                "envFrom": [
                    {"secretRef": {"name": "car-crash-secret"}}
                ]
            }
        }
    }
    
    # Create temporary file
    templates_dir = "/tmp/dag_templates"
    os.makedirs(templates_dir, exist_ok=True)
    template_file = os.path.join(templates_dir, f"{task_name}_spark_app.yaml")
    
    with open(template_file, 'w') as f:
        yaml.dump(spark_app, f, default_flow_style=False)
    
    return f"{task_name}_spark_app.yaml"  # Return just

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
    start_date=datetime.now(),
    catchup=False,
    template_searchpath=["/tmp/dag_templates"],
    params={
        "date": Param("2024-04-20", type="string"),
    },
    tags=["car_crash", "daily", "spark"],
) as dag:
    
    # Get Spark configuration from ConfigMap
    spark_config = get_spark_config()
    
    # Create application files
    ingest_job_app_file = create_spark_app_file("ingest-job-config-map", "ingest_job.py", spark_config)
    
    # Task A - Pull data using Spark
    ingest_job = SparkKubernetesOperator(
        task_id="ingest_data",
        namespace="airflow",
        application_file=ingest_job_app_file,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
    
   
    # Define task dependencies: a > b > c
    ingest_job
