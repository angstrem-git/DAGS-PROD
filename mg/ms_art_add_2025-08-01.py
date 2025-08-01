from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

with DAG(
    dag_id="ms_art_add_2025-08-01",
    description="Заполнение таблицы [mg2].[art] новыми номенклатурами",
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    tags=['mg'],
) as dag:

    t1 = SQLExecuteQueryOperator(
    task_id='t1',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_add_nom_prop.sql'
    )

    t2 = SQLExecuteQueryOperator(
    task_id='t2',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_add_order.sql'
    )

    t3 = SQLExecuteQueryOperator(
    task_id='t3',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_add_sale.sql'
    )

    t4 = SQLExecuteQueryOperator(
    task_id='t4',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_add_logs.sql'
    )

    t5 = SQLExecuteQueryOperator(
    task_id='t5',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_type_add.sql'
    )

    t6 = SQLExecuteQueryOperator(
    task_id='t6',
    conn_id='mssql_olap_main',
    sql='sql/ms_art_type_update.sql'
    )

t1 >> t2 >> t3 >> t4 >> t5 >> t6