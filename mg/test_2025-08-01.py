from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta

sql_text = "SELECT [pao_status_name] FROM [Angstrem].[mg1].[pao_status]"

with DAG(
    dag_id="test_2025-08-01",
    description="Заполнение таблицы [mg2].[art] новыми номенклатурами",
    schedule="@daily",
    tags=['mg'],
) as dag:

    t1 = SQLExecuteQueryOperator(
    task_id='t1',
    conn_id='mssql_olap_main',
    sql=sql_text 
    )

t1 