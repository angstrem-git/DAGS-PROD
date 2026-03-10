from airflow.sdk import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator	# Внешний сервис
import pendulum

local_tz = pendulum.timezone("Europe/Moscow")

with DAG(
	dag_id="add_dim_for_rasp_2026-03-10",
	description="Заполнение справочников для rasp",
	schedule="0 4 * * *",  # 04:00 MSK
	start_date=pendulum.datetime(2026, 3, 10, tz=local_tz),
	catchup=False,
	tags=['rasp'],
) as dag:

	t01 = ClickHouseOperator(
		task_id='t01_truncate_rasp2_open_orders_rn_today',
		clickhouse_conn_id='click_onpremise_airflow',
		sql='sql/t01_truncate_rasp2_open_orders_rn_today.sql'			
	)

	t02 = ClickHouseOperator(
		task_id='t02_insert_rasp2_open_orders_rn_today',
		clickhouse_conn_id='click_onpremise_airflow',
		sql='sql/t02_insert_rasp2_open_orders_rn_today.sql'			
	)

	t03 = ClickHouseOperator(
		task_id='t03_truncate_rasp2_pao_package_classification',
		clickhouse_conn_id='click_onpremise_airflow',
		sql='sql/t03_truncate_rasp2_pao_package_classification.sql'			
	)

	t04 = ClickHouseOperator(
		task_id='t04_insert_rasp2_pao_package_classification',
		clickhouse_conn_id='click_onpremise_airflow',
		sql='sql/t04_insert_rasp2_pao_package_classification.sql'			
	)

	t05 = ClickHouseOperator(
		task_id='t05_truncate_rasp2_unit_sale',
		clickhouse_conn_id='click_onpremise_airflow',
		sql='sql/t05_truncate_rasp2_unit_sale.sql'			
	)

	t06 = ClickHouseOperator(
		task_id='t06_insert_rasp2_unit_sale',
		clickhouse_conn_id='click_onpremise_airflow',
		sql='sql/t06_insert_rasp2_unit_sale.sql'			
	)

t01 >> t02 >> t03 >> t04 >> t05 >> t06 
