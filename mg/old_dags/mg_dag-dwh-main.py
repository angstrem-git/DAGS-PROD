from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

def hello_world():
    print('Hello Airflow from Python')

with DAG(
    'mg_dag-dwh-main',
    schedule_interval="@hourly",
    start_date=datetime(2024, 3, 8),
    tags=['mg'],
) as dag:

    bash_a = BashOperator(
        task_id='bash_a',
        bash_command='echo Hello Airflow from Bash'
    )

    python_a = PythonOperator(
        task_id='python_a',
        python_callable=hello_world
    )

    mssql_a = MsSqlOperator(
    task_id='mssql_a',
    mssql_conn_id='dwn-main',
    sql=r"""
          INSERT INTO [mg3].[test_airflow] (
	        [col_SYSDATETIME] ,
	        [col_SYSDATETIMEOFFSET] ,
	        [col_SYSUTCDATETIME] ,
	        [col_CURRENT_TIMESTAMP] ,
	        [col_GETDATE] ,
	        [col_GETUTCDATE]
	        )
        VALUES (
	        CONVERT (nvarchar(100), SYSDATETIME()),
	        CONVERT (nvarchar(100), SYSDATETIMEOFFSET()),
	        CONVERT (nvarchar(100), SYSUTCDATETIME()),
	        CONVERT (nvarchar(100), CURRENT_TIMESTAMP),
	        CONVERT (nvarchar(100), GETDATE()),
	        CONVERT (nvarchar(100), GETUTCDATE())
	        )  
    """
    )

    bash_a >> python_a >> mssql_a