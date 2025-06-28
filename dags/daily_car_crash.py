from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

#test

# Step 1: Helper method to get environment variables from Kubernetes secrets
def get_env_from_secret():
    """
    Returns a list of Kubernetes Secrets to be mounted as environment variables.
    """
    return [
        Secret(
            deploy_type="env",  # Inject as environment variables
            deploy_target=None,  # If None, all keys in the secret become env vars
            secret="car-crash-secret",  # Name of the Kubernetes secret
        )
    ]


# Step 2: DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="daily_pipeline_car_crashes",
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

    run_car_crash_pipeline = KubernetesPodOperator(
        task_id="pull_data",
        name="pull-car-crash-job",
        namespace="airflow",
        image="jaihind213/daily_pipeline_car_crash:0.0.1-0.1",
        cmds=[
            "python",
            "/opt/daily_pipeline_car_crash/default_job_config.ini",
            "{{ params.date }}",
        ],
        env_from=get_env_from_secret(),
        get_logs=True,
        is_delete_operator_pod=False,
    )

    run_car_crash_pipeline
