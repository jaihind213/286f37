import logging
import os
from typing import List, Any

import yaml
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client

def get_env_from_secret(secret_name):
    """Returns a list of environment variable sources."""
    return [{"secretRef": {"name": secret_name}}]

# Helper function to create Spark application YAML file
def create_py_spark_operator_app_file(
    task_name, main_file, args: List[Any], spark_config, image, secret_holding_env_vars, app_config_map_name, app_config_map_mount_path, service_account="spark"
):
    """Create a temporary YAML file for Spark application"""
    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": f"{task_name}-{{{{ ds }}}}", "namespace": "airflow"},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "Always",
            "mainApplicationFile": main_file,
            "arguments": args,
            "sparkVersion": spark_config.get("spark_version", "3.5.2"),
            "restartPolicy": {"type": "Never"},
            "sparkConf": {
                #put any common stuff here. rest comes form app config.
                "spark.driver.extraClassPath": "/opt/spark_jars/",
                "spark.executor.extraClassPath": "/opt/spark_jars/",
            },
            "driver": {
                "cores": int(spark_config.get("driver_cores", "1")),
                "coreLimit": spark_config.get("driver_core_limit", "1200m"),
                "memory": spark_config.get("driver_memory", "1g"),
                "serviceAccount": service_account,
                "env": [
                    {
                        "name": "JAVA_HOME",
                        "value": spark_config.get("java_home", "/opt/java/openjdk"),
                    }, {
                        "name": "SPARK_HOME",
                        "value": spark_config.get("java_home", "/opt/spark"),
                    }
                ],
                "envFrom": [{"secretRef": {"name": secret_holding_env_vars}}],
                "volumeMounts": [
                    {
                        "name": "car-crash-config-volume",
                        "mountPath": app_config_map_mount_path
                    }
                ]
            },
            "executor": {
                "cores": int(spark_config.get("executor_cores", "2")),
                "instances": int(spark_config.get("executor_instances", "1")),
                "memory": spark_config.get("executor_memory", "2g"),
                "env": [
                    {
                        "name": "JAVA_HOME",
                        "value": spark_config.get("java_home", "/opt/java/openjdk"),
                    }, {
                        "name": "SPARK_HOME",
                        "value": spark_config.get("java_home", "/opt/spark"),
                    }
                ],
                "envFrom": [{"secretRef": {"name": secret_holding_env_vars}}],
                "volumeMounts": [
                    {
                        "name": "car-crash-config-volume",
                        "mountPath": app_config_map_mount_path
                    }
                ]
            },
            "volumes": [
                {
                    "name": "car-crash-config-volume",
                    "configMap": {
                        "name": app_config_map_name
                    }
                }
            ],
        },
    }
    logging.info("Creating Spark application YAML file for task: %s", task_name)
    logging.info("Spark application configuration: %s", spark_app)

    # Create temporary file
    templates_dir = "/tmp/dag_templates"
    os.makedirs(templates_dir, exist_ok=True)
    template_file = os.path.join(templates_dir, f"{task_name}_spark_app.yaml")

    with open(template_file, "w") as f:
        yaml.dump(spark_app, f, default_flow_style=False)

    return f"{task_name}_spark_app.yaml"


def get_config_map_data(config_map_name, namespace="airflow"):
    k8s_hook = KubernetesHook(conn_id="kubernetes_default")
    try:
        api_client = k8s_hook.get_conn()
        v1 = client.CoreV1Api(api_client)

        config_map = v1.read_namespaced_config_map(
            name=config_map_name, namespace=namespace
        )

        return config_map.data
    except Exception:
        import traceback
        traceback.print_exc()
        raise ValueError(f"failed to get ConfigMap '{config_map_name}' in namespace '{namespace}'.")

