# Устанавливаем значение переменной RELEASE
RELEASE = "v01"

# Используем f-string для динамического формирования пути
from importlib import import_module
# Динамически импортируем функцию из файла, где имя папки зависит от RELEASE
module = import_module(f"mg.2026-03-07_test_dag.{RELEASE}.py.{RELEASE}_01_wait_for_batch")
wait_for_batch = getattr(module, "wait_for_batch")
#from v01.py.v01_01_wait_for_batch import wait_for_batch  # импортируем функцию wait_for_batch из отдельного файла 01_wait_for_batch

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
	dag_id=f"{RELEASE}_rasp_pipeline",
	description="Преобразование данных rasp",
	start_date=pendulum.datetime(2026, 3, 11, tz=local_tz),
	schedule=None,
	catchup=False,
	tags=['rasp']
) as dag:

	t01 = PythonSensor(						
		task_id="wait_for_batch",
		poke_interval=5,
		timeout=60,
		python_callable=wait_for_batch,
		op_kwargs={
			"RELEASE_key": RELEASE,
			"DAG_DIR_key": DAG_DIR,
			"DB1_key": "rasp1",
			"DB3_key": DB3,
			"URL_key": URL,
			"USER_key": USER,
			"PASSWORD_key": PASSWORD
		}
	)

	t02 = PythonOperator(
		task_id="check_sum",
		python_callable=check_sum,
		op_kwargs={
			"RELEASE_key": RELEASE,
			"DAG_DIR_key": DAG_DIR,
			"DB1_key": "rasp1",
			"URL_key": URL,
			"USER_key": USER,
			"PASSWORD_key": PASSWORD
		}
	)

	t03 = ClickHouseOperator(
		task_id="insert_fct_deficit_packet_order",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_t03_insert_fct_deficit_packet_order.sql"			# Относительный путь относительно файла DAG-а
	)

	t04 = SSHOperator(
		task_id="rank_process",
		ssh_conn_id="airflowetl_ssh",
		command=f"""
			export CLICKHOUSE_URL="{URL}"
			export CLICKHOUSE_USER="{USER}"
			export CLICKHOUSE_PASSWORD="{PASSWORD}"							
			export CLICKHOUSE_DATABASE2="{DB2}"
			export CLICKHOUSE_DATABASE3="{DB3}"

			python3 /home/airflowetl/AIRFLOW-ETL-MACHINE/rasp/rasp_v01/{RELEASE}_t04_rank_process.py \
			'{{{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}}}'
		"""
		# Лучше передавать пароль через env, а не через SSH-команду command=""" python3 ... PASSWORD ... """
		# '{{{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}}}': f"{{{{ value }}}}" -> {{ value }} -> (Jinja) -> 
			
		#environment={
		#	"CLICKHOUSE_URL": URL,
		#	"CLICKHOUSE_USER": USER,
		#	"CLICKHOUSE_PASSWORD": PASSWORD,								# Лучше передавать пароль через env, а не через SSH-команду command=""" python3 ... PASSWORD ... """
		#	"CLICKHOUSE_DATABASE": DB2
		#}
		# Сервер принимает environment, только если разрешено AcceptEnv (это так не всегда)
	)

	t05 = ClickHouseOperator(
		task_id="insert_fct_deficit_order_rank",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_t05_insert_fct_deficit_order_rank.sql",			# Относительный путь относительно файла DAG-а
		params={			
			"db2": DB2,					
			"db3": DB3					
		}
	)

	t06 = ClickHouseOperator(
		task_id="insert_fct_deficit_packet_rank",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_t06_insert_fct_deficit_packet_rank.sql",			# Относительный путь относительно файла DAG-а
		params={			
			"db2": DB2									
		}
	)

	t07 = ClickHouseOperator(
		task_id="insert_fct_deficit_packet_array_rank",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_t07_insert_fct_deficit_packet_array_rank.sql",			# Относительный путь относительно файла DAG-а
		params={			
			"db3": DB3									
		}
	)

	t08 = ClickHouseOperator(
		task_id="insert_dim_packet_processed_batches",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_t08_insert_dim_packet_processed_batches.sql"			# Относительный путь относительно файла DAG-а
	)

	t01 >> t02 >> t03 >> t04 >> t05 >> t06 >> t07 >> t08

