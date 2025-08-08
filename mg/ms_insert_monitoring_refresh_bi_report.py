from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

with DAG(
    dag_id="ms_insert_monitoring_refresh_bi_report",
    description="Заполнение таблицы [mg2].[monitoring_refresh_bi_report] - мониторинг автоматического обновления отчетов в Power BI Report Server",
    schedule="@daily",
    start_date=datetime(2025, 8, 8, 8, 0, 0, 0, tz='Europe/Moscow'),
    catchup=True,
    tags=['mg'],
) as dag:

    t1 = SQLExecuteQueryOperator(
    task_id='t1',
    conn_id='mssql_olap_main',
    sql='sql/ms_insert_monitoring_refresh_bi_report.sql'
    )

t1 