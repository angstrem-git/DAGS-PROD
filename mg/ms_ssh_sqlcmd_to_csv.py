from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.ssh import SSHOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

with DAG(
    dag_id="ms_sqlcmd_to_csv",
    description="Выгрузка данных в csv-файл",
    schedule=None,	
    start_date=datetime(2025, 8, 24, 8, 0, 0, 0, tz='Europe/Moscow'),
    catchup=False,
    tags=['mg'],
) as dag:

    t1 = SSHOperator(
    task_id='t1',
    ssh_conn_id='mssql_olap_main',
    command='sqlcmd -S {{ conn.mssql_olap_main.host }} -d {{ conn.mssql_olap_main.schema }} -U {{ conn.mssql_olap_main.login }} -P {{ conn.mssql_olap_main.password }} -Q 'SET NOCOUNT ON; SELECT order_id, order_guid, order_numder, phone FROM Angstrem.mgtest.phone' -W -s';' -u -o 'C:\Users\M.Grapenyuk\Documents\mg\test\file_phone_unicode.csv''
    )

    t2 = SSHOperator(
    task_id='t2',
    ssh_conn_id='mssql_olap_main',
    command='powershell -Command 'Get-Content C:\Users\M.Grapenyuk\Documents\mg\test\file_phone_unicode.csv | Set-Content C:\Users\M.Grapenyuk\Documents\mg\test\file_phone_utf8.csv -Encoding utf8''
    )

t1 >> t2