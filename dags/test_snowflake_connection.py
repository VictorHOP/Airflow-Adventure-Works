from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)

with DAG(
    "dbt_cloud_provider_eltml",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 70471823415164},
    start_date=datetime(2021, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    extract = DummyOperator(task_id="extract")
    load = DummyOperator(task_id="load")
    ml_training = DummyOperator(task_id="ml_training")

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=70471823418782,
        check_interval=10,
        timeout=300,
    )

    extract >> load >> trigger_dbt_cloud_job_run >> ml_training