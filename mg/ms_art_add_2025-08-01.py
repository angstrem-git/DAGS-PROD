from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

with DAG(
    dag_id="ms_art_add_2025-08-01",
    description="Заполнение таблицы [mg2].[art] новыми номенклатурами",
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    tags=['mg'],
) as dag:

    t1 = MsSqlOperator(
    task_id='t1',
    mssql_conn_id='mssql_olap_main',
    sql='sql/ms_art_add_nom_prop.sql'
    )

    t2 = MsSqlOperator(
    task_id='t2',
    mssql_conn_id='mssql_olap_main',
    sql='sql/ms_art_add_order.sql'
    )

    t3 = MsSqlOperator(
    task_id='t3',
    mssql_conn_id='mssql_olap_main',
    sql='sql/ms_art_add_sale.sql'
    )

    t4 = MsSqlOperator(
    task_id='t4',
    mssql_conn_id='mssql_olap_main',
    sql='sql/ms_art_add_logs.sql'
    )

    t5 = MsSqlOperator(
    task_id='t5',
    mssql_conn_id='mssql_olap_main',
    sql='sql/ms_art_type_add.sql'
    )

    t6 = MsSqlOperator(
    task_id='t6',
    mssql_conn_id='mssql_olap_main',
    sql='sql/ms_art_type_update.sql'
    )

t1 >> t2 >> t3 >> t4 >> t5 >> t6