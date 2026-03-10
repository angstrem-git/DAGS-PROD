from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator	# Внешний сервис
import pendulum
from pendulum import datetime

local_tz = pendulum.timezone("Europe/Moscow")

with DAG(
    dag_id="art_add_2026-03-10",
    description="Заполнение таблицы [mg2].[art] новыми номенклатурами",
    schedule="30 3 * * *",  # 03:30 MSK
    start_date=pendulum.datetime(2026, 3, 10, tz=local_tz),
	catchup=False,
    tags=['mg'],
) as dag:

    t01 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_nom_prop',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_add_nom_prop.sql'
    )

    t02 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_order',
    	conn_id='mssql_olap_main',
   	 sql='sql/ms_art_add_order.sql'
    )

    t03 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_sale',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_add_sale.sql'
    )

    t04 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_logs',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_add_logs.sql'
    )

    t05 = SQLExecuteQueryOperator(
    	task_id='ms_art_type_add',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_type_add.sql'
    )

    t06 = SQLExecuteQueryOperator(
    	task_id='ms_art_type_update',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_type_update.sql'
    )

    t07 = SQLExecuteQueryOperator(
    	task_id='ms_art_corporate_add',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_corporate_add.sql'
    )

    t08 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_t08_stocks_snapshot_update',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_add_t08_stocks_snapshot_update.sql'
    )

    t09 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_t09_price_setting_update',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_add_t09_price_setting_update.sql'
    )

    t10 = SQLExecuteQueryOperator(
    	task_id='ms_art_add_t10_price_snapshot_update',
    	conn_id='mssql_olap_main',
    	sql='sql/ms_art_add_t10_price_snapshot_update.sql'
    )

    t11 = ClickHouseOperator(
        task_id='ch_art_insert_rasp2_art',
        clickhouse_conn_id='click_onpremise_airflow',
        sql='sql/ch_art_insert_rasp2_art.sql'			
    )

t01 >> t02 >> t03 >> t04 >> t05 >> t06 >> t07 >> t08 >> t09 >> t10 >> t11 