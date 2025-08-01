from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_git_2025-08-01"
) as dag:
    start_task = EmptyOperator(
        task_id="start_task",
    )
    end_task = EmptyOperator(
        task_id="end_task",
    )
start_task >> end_task