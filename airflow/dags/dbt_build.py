from datetime import datetime

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

project_cfg = ProjectConfig(dbt_project_path="/opt/dbt")
profile_cfg = ProfileConfig(
    profile_name="simple_pipeline",
    target_name="prod",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="bigquery",
        profile_args={
            "dataset": "marts",
            "keyfile": "/opt/airflow/gcp-key.json",
        },
    ),
)
exec_cfg = ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt")


with DAG(
    dag_id="dbt_build",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    DbtTaskGroup(
        group_id="dbt_group",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=exec_cfg,
        render_config=RenderConfig(select=["orders_with_last_touch_timestamp"]),
    )
