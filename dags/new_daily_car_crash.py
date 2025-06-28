from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction


#test

# Step 1: Helper method to get environment variables from Kubernetes secrets
def get_env_from_secret():
    """Returns a list of environment variable sources."""
    return [
        {
            'secretRef': {
                'name': 'car-crash-secret'
            }
        }
    ]


# Step 2: DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="new_daily_pipeline_car_crashes",
    default_args=default_args,
    description="Runs daily car crash pipeline with config and date",
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 20),
    catchup=False,
    params={
        "date": Param("2024-04-20", type="string"),
    },
    tags=["car_crash", "daily"],
) as dag:

    pull_data = KubernetesPodOperator(
        task_id="pull_data",
        name="pull-car-crash-job",
        namespace="airflow",
        image="jaihind213/daily_pipeline_car_crash:0.0.4-0.1",
        cmds=[
            "python3",
            "pull_data_job.py",
            "/opt/daily_pipeline_car_crash/default_job_config.ini",
            "{{ params.date }}",
        ],
        env_from=get_env_from_secret(),
        get_logs=True,
        is_delete_operator_pod=False,
        on_finish_action=OnFinishAction.KEEP_POD,
    )

    # Task B - ingest data
    ingest_data = KubernetesPodOperator(
        task_id="ingest_data",
        name="ingest-car-crash-job",
        namespace="airflow",
        image="jaihind213/daily_pipeline_car_crash:0.0.4-0.1",
        cmds=[
            "python3",
            "ingest_job.py",
            "/opt/daily_pipeline_car_crash/default_job_config.ini",
            "{{ params.date }}",
        ],
        env_from=get_env_from_secret(),
        get_logs=True,
        is_delete_operator_pod=False,
        on_finish_action=OnFinishAction.KEEP_POD,
    )

    # Task C - Upload results
    build_cube = KubernetesPodOperator(
        task_id="build_cube",
        name="build-cube-car-crash-job",
        namespace="airflow",
        image="jaihind213/daily_pipeline_car_crash:0.0.4-0.1",
        cmds=[
            "python3",
            "cubes_job.py",
            "/opt/daily_pipeline_car_crash/default_job_config.ini",
            "{{ params.date }}",
        ],
        env_from=get_env_from_secret(),
        get_logs=True,
        is_delete_operator_pod=False,
        on_finish_action=OnFinishAction.KEEP_POD,
    )

    pull_data >> ingest_data >> build_cube
