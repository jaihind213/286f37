import os

from airflow import DAG
from datetime import datetime, timedelta

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

import airflow_util
import k8util
import simon

SIMON_SAYS_ENGINE = "SIMON_SAYS_ENGINE"
SIMON_CONFIG_CONTENT_VAR = "SIMON_CONFIG_CONTENT_VAR"

class HelmJobPythonOperator(BaseOperator):
    @apply_defaults
    def __init__(self, python_callable, *args, **kwargs):
        super(HelmJobPythonOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable

    def pre_execute(self, context):
        logging.info(f"running pre_execute for {self.task_id}...")
        if context['ti'].try_number > 1:
            logging.warning("this is a retry of the task. will reset engine")
            #Reset the engine variable
            os.environ[SIMON_SAYS_ENGINE] = ""
        else:
            airflow_util.write_airflow_variable_to_file(SIMON_CONFIG_CONTENT_VAR, "/tmp/.simon_config")
            logging.info("this is the first attempt of the task.")
        k8util.download_kube_config()
        # git clone the charts. TODO!!!!
        # call simon now.
        simon.setup("/tmp/.simon_config")
        engine_chosen, reason = simon.decide(context['ti'].task_id, datetime.now())
        logging.info(f"simon says...{engine_chosen} for reason: {reason}")
        os.environ[SIMON_SAYS_ENGINE] = engine_chosen[0]

    def execute(self, context):
        engine = os.environ.get("SIMON_SAYS_ENGINE", "")
        logging.info(f"about to run the job with engine selection...{engine}")
        chart_dir = self.__choose_chart(engine)
        env_vars = {}
        env_vars["env[0].name"] = "SIMON_SAYS_ENGINE"
        env_vars["env[0].value"] = engine
        k8util.install_helm_chart(chart_dir, context['ti'].task_id, set_values=env_vars, kubeconfig=k8util.KUBE_CONFIG_FILE_PATH)
        return self.python_callable()

    def post_execute(self, context, result=None):
        logging.info(f"running post_execute for {self.task_id}...")
        os.environ[SIMON_SAYS_ENGINE] = ""
        #helm uninstall

    def __choose_chart(self, engine):
        chart_dir = "job_charts/"
        if engine == "duckdb":
            chart_dir += "single_pod"
        elif engine.startswith("pyspark"):
            chart_dir += "multi_pod"
        else:
            raise ValueError(f"helm chart not found for engine {engine}")
        return chart_dir

# Define a simple callable function for the custom operator
def job_task():
    engine = os.environ.get("SIMON_SAYS_ENGINE", "")
    print(f"running my job with engine... {engine}")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'data_pipeline_xperimentation_demonstration',
    default_args=default_args,
    description='A simple DAG which can switch the backend engine(duckdb/spark/snowflake) of the job using simonx library',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Create a task using the custom operator
GetPaxByVendorTask = HelmJobPythonOperator(
    task_id='GetPaxByVendor',
    python_callable=job_task,
    dag=dag,
)
