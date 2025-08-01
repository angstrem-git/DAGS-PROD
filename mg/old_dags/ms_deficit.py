from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

with DAG(
    'ms_deficit',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['mg'],
) as dag:

    t1 = MsSqlOperator(
        task_id='t1',
        mssql_conn_id='mssql_olap_test',
        sql='sql/ms_deficit_1_insert_deficit_snapshot.sql'
    )

    t2 = MsSqlOperator(
        task_id='t2',
        mssql_conn_id='mssql_olap_test',
        sql='sql/ms_deficit_2_truncate_csv_sost_reserva_po_paketam.sql'
    )

    t3 = MsSqlOperator(
        task_id='t3',
        mssql_conn_id='mssql_olap_test',
        sql='sql/ms_deficit_3_bulk_insert_csv_sost_reserva_po_paketam.sql'
    )

    
    t4 = MsSqlOperator(
        task_id='t4',
        mssql_conn_id='mssql_olap_test',
        sql='sql/ms_deficit_4_insert_dwh_sost_reserva_po_paketam.sql'
    )

t1 >> t2 >> t3 >> t4