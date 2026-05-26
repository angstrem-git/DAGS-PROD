# Устанавливаем значение переменной RELEASE
RELEASE = "v01"

# Используем f-string для динамического формирования пути
from importlib import import_module
# Динамически импортируем функцию из файла, где имя папки зависит от RELEASE
module = import_module(f"mg.rasp_pipeline.rasp_{RELEASE}.py.{RELEASE}_t01_wait_for_batch")
wait_for_batch = getattr(module, "wait_for_batch")
#from rasp_v01.py.v01_t01_wait_for_batch import wait_for_batch  # импортируем функцию wait_for_batch из отдельного файла v01_t01_wait_for_batch
module = import_module(f"mg.rasp_pipeline.rasp_{RELEASE}.py.{RELEASE}_t02_check_sum")
check_sum = getattr(module, "check_sum")
#from rasp_v01.py.v01_t02_check_sum import check_sum  # импортируем функцию check_sum из отдельного файла v01_t02_check_sum


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

ch = BaseHook.get_connection("click_onpremise_http_etl")
URL = f"http://{ch.host}:{ch.port}"
USER = ch.login
PASSWORD = ch.password

DAG_DIR = Path(__file__).parent  # Путь к текущей папке DAG


with DAG(
    dag_id=f"test_{RELEASE}_rasp",
    description="Преобразование данных rasp",
    start_date=pendulum.datetime(2024, 12, 31, tz=local_tz),
    schedule = "30 7 * * *",
    catchup=False,
    tags=['rasp']
) as dag:

    t01 = ClickHouseOperator(
        task_id="insert_into_airflow_dates_test",
        clickhouse_conn_id="click_onpremise_airflow",
        sql="""
            INSERT INTO test.airflow_dates_test(
                run_id 
                ,logical_date 
                ,ds 
                ,data_interval_start 
                ,data_interval_start_ds 
                ,data_interval_end 
                ,data_interval_end_ds 
                ,ts 
            )
            VALUES (
                '{{ run_id }}'
                ,parseDateTimeBestEffort('{{ logical_date }}', 'Europe/Moscow')
                ,toDate('{{ ds }}')
                ,parseDateTimeBestEffort('{{ data_interval_start }}', 'Europe/Moscow')
                ,toDate('{{ data_interval_start | ds }}')
                ,parseDateTimeBestEffort('{{ data_interval_end }}', 'Europe/Moscow')
                ,toDate('{{ data_interval_end | ds }}')
                ,parseDateTimeBestEffort('{{ ts }}', 'Europe/Moscow')
            )
        """
    )


    def debug_dates(**context):
        print("run_id = ", context["run_id"])
        print("ds = ", context["ds"])
        print("logical_date = ", context["logical_date"])
        print("data_interval_start = ", context["data_interval_start"])
        print("data_interval_end = ", context["data_interval_end"])
        print("logical_date tz =", context["logical_date"].tzinfo)
        print("logical_date iso =", context["logical_date"].isoformat())

        from pprint import pprint
        pprint(context)


    t02 = PythonOperator(
        task_id="debug_dates",
        python_callable=debug_dates
    )

    t01 >> t02

