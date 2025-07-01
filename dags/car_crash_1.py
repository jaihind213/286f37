import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from kubernetes.client import models as k8s
import dag_util as du

# Step 1: DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="car_crash_1",
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

    # get common config
    common_config_volume = k8s.V1Volume(
        name="common-config-volume",
        config_map=k8s.V1ConfigMapVolumeSource(
            name=" common-config-map"  # Replace with your ConfigMap name
        ),
    )

    # Define the volume mount
    common_config_volume_mount = k8s.V1VolumeMount(
        name="common-config-volume",
        mount_path="/opt/daily_pipeline_car_crash/config/",
        read_only=True,
    )

    image_config_map = du.get_config_map_data("image-config-map", namespace="airflow")
    image_tag = image_config_map.get(
        "image", "jaihind213/daily_pipeline_car_crash:0.0.11-0.1"
    )
    logging.info("image being used: %s", image_tag)

    pull_data = KubernetesPodOperator(
        task_id="pull_data",
        name="pull-data",
        namespace="airflow",
        image=image_tag,
        cmds=[
            "python3",
            "pull_data_job.py",
            "/opt/daily_pipeline_car_crash/config/default_job_config.ini",
            "{{ params.date }}",
        ],
        env_from=du.get_env_from_secret("car-crash-secret"),
        get_logs=True,
        is_delete_operator_pod=False,
        on_finish_action=OnFinishAction.KEEP_POD,
        volumes=[common_config_volume],
        volume_mounts=[common_config_volume_mount],
        startup_timeout_seconds=180,
    )

    # # # Create application files
    # ingest_job_main_file = "local:///opt/daily_pipeline_car_crash/ingest_job.py"
    # ingest_job_args = [
    #     "/opt/daily_pipeline_car_crash/config/default_job_config.ini",
    #     "{{ params.date }}",
    # ]
    # ingest_job_spark_config = get_spark_config("ingest-job-config-map")
    # ingest_job_app_file = du.create_py_spark_operator_app_file("ingest_iceberg",
    #                                                            ingest_job_main_file,
    #                                                            ingest_job_args,
    #                                                            ingest_job_spark_config,
    #                                                            image_tag,
    #                                                            "car-crash-secret",
    #                                                            "ingest-job-config-map",
    #                                                            "/opt/daily_pipeline_car_crash/config")
    # ingest_job = SparkKubernetesOperator(
    #     task_id="ingest_iceberg",
    #     namespace="airflow",
    #     application_file=ingest_job_app_file,
    #     kubernetes_conn_id="kubernetes_default",
    #     do_xcom_push=False,
    # )

    pull_data
    #pull_data >> ingest_job

