from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

with DAG(
    dag_id="ms_art_add_2025-08-01",
    description="Заполнение таблицы [mg2].[art] новыми номенклатурами",
    schedule="@daily",
    start_date=datetime(2025, 8, 2),
    tags=['mg'],
) as dag:

    t1 = SQLExecuteQueryOperator(
    task_id='t1',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_add_nom_prop.sql'
    )

t1 