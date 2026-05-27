# Устанавливаем значение переменной RELEASE
RELEASE = "v01"

from airflow.sdk import DAG														# Для Airflow v3
from airflow.sdk import Variable												# Для Airflow v3
from airflow.hooks.base import BaseHook												
from airflow.providers.standard.sensors.python import PythonSensor				# Для Airflow v3
from airflow.providers.standard.operators.python import PythonOperator			# Для Airflow v3
from airflow.providers.ssh.operators.ssh import SSHOperator						# Для Airflow v3
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator	# Внешний сервис
from pendulum import datetime													# Лучше from pendulum, чем from datetime			
import pendulum
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path


local_tz = pendulum.timezone("Europe/Moscow")

DB1 = f"rasp1_{RELEASE}"
DB2 = f"rasp2_{RELEASE}"
DB3 = f"rasp3_{RELEASE}"

DB1_test = "dev1"

ch = BaseHook.get_connection("click_onpremise_http_etl")
URL = f"http://{ch.host}:{ch.port}"
USER = ch.login
PASSWORD = ch.password

DAG_DIR = Path(__file__).parent  # Путь к текущей папке DAG


with DAG(
    dag_id=f"test_{RELEASE}_rasp_truncate",
    description="Удаление данных rasp",
    start_date=pendulum.datetime(2024, 12, 31, tz=local_tz),
    schedule = None,
    catchup=False,
    tags=['rasp']
) as dag:
    
    t01 = ClickHouseOperator(
        task_id="truncate_all_goods",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="TRUNCATE TABLE dev2_v02.all_goods"
    )

    t02 = ClickHouseOperator(
        task_id="truncate_all_orders",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="TRUNCATE TABLE dev2_v02.all_orders"
    )

    t03 = ClickHouseOperator(
        task_id="truncate_self_order",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="TRUNCATE TABLE dev3_v02.self_order"
    )

    t04 = ClickHouseOperator(
        task_id="truncate_full_order",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="TRUNCATE TABLE dev3_v02.full_order"
    )

    t05 = ClickHouseOperator(
        task_id="truncate_full_order_has_goods_type_bridge",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="TRUNCATE TABLE dev3_v02.full_order_has_goods_type_bridge"
    )

    t06 = ClickHouseOperator(
        task_id="truncate_full_order_deficit_goods_type_bridge",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="TRUNCATE TABLE dev3_v02.full_order_deficit_goods_type_bridge"
    )

    t01 >> t02 >> t03 >> t04 >> t05 >> t06