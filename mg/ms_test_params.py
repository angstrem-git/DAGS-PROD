from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

with DAG(
    'ms_test_params',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['mg'],
) as dag:

    t1 = MsSqlOperator(
        task_id='t1',
        mssql_conn_id='mssql_olap_test',
        sql='sql/ms_test_insert_params.sql',
        params={"test_int":123}
    )

t1