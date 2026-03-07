# Устанавливаем значение переменной RELEASE
RELEASE = "v02"

# Используем f-string для динамического формирования пути
from importlib import import_module
# Динамически импортируем функцию из файла, где имя папки зависит от RELEASE
module = import_module(f"{RELEASE}.py.{RELEASE}_01_wait_for_batch")
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
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path


DB1 = f"dev1_{RELEASE}"
DB2 = f"dev2_{RELEASE}"
DB3 = f"dev3_{RELEASE}"

ch = BaseHook.get_connection("click_onpremise_http_etl")
URL = f"http://{ch.host}:{ch.port}"
USER = ch.login
PASSWORD = ch.password

DAG_DIR = Path(__file__).parent  # Путь к текущей папке DAG

#def wait_for_batch(**context):
#
#	sql_path = DAG_DIR / "sql/2026-03-05_test_find_batch.sql"
#	with open(sql_path, encoding="utf-8") as f:
#		sql = f.read()
#	query_text = sql.format(p_db1=DB1, p_db2=DB2)
#
#	query_encoded = urllib.parse.quote(query_text)
#	full_url = f"{URL}/?database={DB1}&query={query_encoded}"
#
#	r = requests.post(
#		full_url,
#		auth=HTTPBasicAuth(USER, PASSWORD),
#		headers={"Content-Type": "text/plain"},
#		timeout=60,
#	)
#	r.raise_for_status()
#
#	d = r.json()["data"]
#	if len(d) == 0:
#		return False
#
#	batch_id_dttm = d[0]["batch_id_dttm"]
#	context["ti"].xcom_push(
#		key="batch_id_dttm",
#		value=batch_id_dttm
#	)

	return True


with DAG(
	dag_id=f"{RELEASE}_2026-03-07_etl_test_pipeline",
	start_date=datetime(2026,3,7),
	schedule=None,
	catchup=False
) as dag:

	sensor01 = PythonSensor(						
		task_id="wait_for_batch",
		poke_interval=5,
		timeout=60,
		python_callable=wait_for_batch
		op_kwargs={
			"DAG_DIR_key": DAG_DIR,
			"DB1_key": DB1,
			"DB2_key": DB2,
			"URL_key": URL,
			"USER_key": USER,
			"PASSWORD_key": PASSWORD
		}
	)

	task02 = SSHOperator(
		task_id="mark_processed",
		ssh_conn_id="airflowetl_ssh",
		command=f"""
			export CLICKHOUSE_URL="{URL}"
			export CLICKHOUSE_USER="{USER}"
			export CLICKHOUSE_PASSWORD="{PASSWORD}"							
			export CLICKHOUSE_DATABASE="{DB2}"

			python3 /home/airflowetl/MG/test_etl/{RELEASE}_02_2026-03-07_test_mark_processed.py \
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

	task03 = ClickHouseOperator(
		task_id="insert_test2",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_03_2026-03-07_test_insert_test2.sql",			# Относительный путь относительно файла DAG-а
		params={
			"db1": DB1,					
			"db2": DB2					
		}
	)

	task04 = ClickHouseOperator(
    	task_id="truncate_test3",
    	clickhouse_conn_id="click_onpremise_airflow",
    	sql=f"sql/{RELEASE}_04_2026-03-07_test_truncate_test3.sql",			# Относительный путь относительно файла DAG-а
    	params={"db3": DB3}
	)

	task05 = ClickHouseOperator(
		task_id="insert_test3",
		clickhouse_conn_id="click_onpremise_airflow",
		sql=f"sql/{RELEASE}_05_2026-03-07_test_insert_test3.sql",			# Относительный путь относительно файла DAG-а
        params={
			"db1": DB1,					
			"db2": DB2,					
			"db3": DB3					
		}
	)

	sensor01 >> task02 >> task03 >> task04 >> task05 