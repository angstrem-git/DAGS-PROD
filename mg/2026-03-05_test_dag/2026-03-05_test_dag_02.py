from airflow.sdk import DAG														# Для Airflow v3
from airflow.sdk import Variable												# Для Airflow v3
from airflow.sdk import BaseHook												# Для Airflow v3
from airflow.providers.standard.sensors.python import PythonSensor				# Для Airflow v3
from airflow.providers.standard.operators.python import PythonOperator			# Для Airflow v3
from airflow.providers.ssh.operators.ssh import SSHOperator						# Для Airflow v3
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator	# Внешний сервис
from pendulum import datetime													# Лучше from pendulum, чем from datetime			
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth

DB1 = Variable.get("DB1")
DB2 = Variable.get("DB2")
DB3 = Variable.get("DB3")

ch = BaseHook.get_connection("click_onpremise_http_etl")
URL = f"http://{ch.host}:{ch.port}"
USER = ch.login
PASSWORD = ch.password


def wait_for_batch(**context):

	with open("sql/2026-03-05_test_find_batch.sql") as f:
		sql = f.read()
	query_text = sql.format(p_db1=DB1, p_db2=DB2)

	query_encoded = urllib.parse.quote(query_text)
	full_url = f"{URL}/?database={DB1}&query={query_encoded}"

	r = requests.post(
		full_url,
		auth=HTTPBasicAuth(USER, PASSWORD),
		headers={"Content-Type": "text/plain"},
		timeout=60,
	)
	r.raise_for_status()

	d = r.json()["data"]
	if len(d) == 0:
		return False

	batch_id_dttm = d[0]["batch_id_dttm"]
	context["ti"].xcom_push(
		key="batch_id_dttm",
		value=batch_id_dttm
	)

	return True


with DAG(
	dag_id="2026-03-05_etl_test_pipeline",
	start_date=datetime(2026,3,5),
	schedule=None,
	catchup=False
) as dag:

	sensor = PythonSensor(						
		task_id="wait_for_batch",
		poke_interval=5,
		timeout=60,
		python_callable=wait_for_batch
	)

	task1 = SSHOperator(
		task_id="mark_processed",
		ssh_conn_id="airflowetl_ssh",
		command=f"""
			export CLICKHOUSE_URL="{URL}"
			export CLICKHOUSE_USER="{USER}"
			export CLICKHOUSE_PASSWORD="{PASSWORD}"							
			export CLICKHOUSE_DATABASE="{DB2}"

			python3 /home/airflowetl/MG/test_etl/2026-03-05_test_mark_processed.py \
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

	task2 = ClickHouseOperator(
		task_id="insert_test2",
		clickhouse_conn_id="click_onpremise_airflow",
		sql="sql/2026-03-05_test_insert_test2.sql",
		params={
			"db1": "{{ var.value.DB1 }}",					# = DB1 = Variable.get("DB1")
			"db2": "{{ var.value.DB2 }}",					# = DB2 = Variable.get("DB2")
			"batch_id_dttm": "{{ ti.xcom_pull(task_ids='wait_for_batch', key='batch_id_dttm') }}"
		}
	)

	task3 = ClickHouseOperator(
		task_id="insert_test3",
		clickhouse_conn_id="click_onpremise_airflow",
		sql="sql/2026-03-05_test_insert_test3.sql",
        params={
			"db1": "{{ var.value.DB1 }}",					# = DB1 = Variable.get("DB1")
			"db2": "{{ var.value.DB2 }}",					# = DB2 = Variable.get("DB2")
			"db3": "{{ var.value.DB3 }}",					# = DB3 = Variable.get("DB3")
			"batch_id_dttm": "{{ ti.xcom_pull(task_ids='wait_for_batch', key='batch_id_dttm') }}"
		}
	)

	sensor >> task1 >> task2 >> task3