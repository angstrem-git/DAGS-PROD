#from airflow.sdk import DAG
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.providers.standard.operators.python import PythonOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from pendulum import datetime

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from decimal import Decimal


def run_query_text(sql: str):
# Результат запроса - в формате текст. Если в запросе указать SELECT ... FORMAT TSV, то в виде (value11\tvalue12\tvalue13\nvalue21\tvalue22\tvalue23\n)
	hook = HttpHook(
		method="POST", 
		http_conn_id="click_onpremise_http"
	)
	response = hook.run(
        endpoint="",
        data=sql,
        headers={"Content-Type": "text/plain"},
    )  
	return response.text

def run_query_json(sql: str):
# Результат запроса - в формате json
	
	hook = HttpHook(
		method="POST",
		http_conn_id="click_onpremise_http",
	)
	response = hook.run(
		endpoint="/",
		data=sql,
		headers={"Content-Type": "text/plain"},
	)
	return response.json()
	# Если response.text, то получишь строку, а не структуру.
	# Если в запросе указать SELECT ... FORMAT JSON и return response.json(), то run_query_json вернет славарь.
	#{
	#  "meta": [
	#    {"name": "column1", "type": "UInt64"},
	#    {"name": "column2", "type": "Decimal(18,2)"}
	#  ],
	#  "data": [
	#    {"column1": 10, "column2": "123.45"},
	#    {"column1": 20, "column2": "678.90"}
	#  ],
	#  "rows": 2,				 # Количество строк в выборке
	#  "statistics": {
	#    "elapsed": 0.003,
	#    "rows_read": 100,
	#    "bytes_read": 2048
	#  }
	#}

def check_new_batch(**context):

    # Проверяем, что появилась хотя бы одна запись (SELECT count() - в сенсоре всегда использовать count() - это золотое правило сенсоров, но нам нужен конкретный batch_id)
	sql_count = """
    	SELECT count()							
		FROM test.sensor_load_batches
		WHERE table_name = 'sensor_fact_table'
  			AND batch_id NOT IN (
      			SELECT batch_id
      			FROM test.sensor_processed_batches
  			)
    """
	check_count = int(run_query_text(sql_count).strip())
	
	# Считываем новый batch_id
	query = """
    	SELECT batch_id								
		FROM test.sensor_load_batches
		WHERE table_name = 'sensor_fact_table'
  			AND batch_id NOT IN (
      			SELECT batch_id
      			FROM test.sensor_processed_batches
  			)
		ORDER BY finished_sourse_datetime
		LIMIT 1
    """
	result = run_query_text(query).strip()
    
	print("(MikGrap) RAW RESULT:", result)   # <-- важно - Это сообщение выводится в логах выполнения DAG

	if not result:
		return False

	# ti — это TaskInstance. «Дай мне экземпляр текущей задачи, которая прямо сейчас выполняется».
	ti = context["ti"]
	# ЯВНО пишем в XCom
	ti.xcom_push(
		key="batch_id",
		value=result
	)

    # ClickHouse по HTTP всегда вернёт число	
    #return int(result) > 0

	#count = result.result_rows[0][0]  # получаем число записей
	return check_count > 0

    # True → Sensor завершился успешно
    # False → Sensor будет ждать следующей попытки

	# **context - в PythonOperator контекст — это словарь со всем, что Airflow знает о текущем запуске task (о TaskInstance)
	# Проверка содержимого:
	for k in sorted(context.keys()):
		print(k)


def compare_checksums(batch_id: str) -> bool:
    # ---------- 1. ожидаемые контрольные суммы ----------
	expected_sql = f"""
    SELECT
        check_type,
        check_value
    FROM test.sensor_check_batches
    WHERE batch_id = '{batch_id}'
      AND table_name = 'sensor_fact_table'
    FORMAT JSON
    """

	expected_json = run_query_json(expected_sql)

	if expected_json["rows"] == 0:
		raise ValueError(f"No checksums found for batch_id={batch_id}")

	expected = {
		row["check_type"]: Decimal(str(row["check_value"]))
		for row in expected_json["data"]
	}

    # ---------- 2. фактические значения ----------
	actual_sql = f"""
    SELECT
        count()              AS row_count,
        sum(qty)             AS sum_qty,
        countDistinct(order) AS distinct_orders
    FROM test.sensor_fact_table
    WHERE batch_id = '{batch_id}'
    FORMAT JSON
    """

	actual_json = run_query_json(actual_sql)
	actual_row = actual_json["data"][0]

	actual = {
		"row_count": Decimal(actual_row["row_count"]),
		"sum_qty": Decimal(actual_row["sum_qty"]),
		"distinct_orders": Decimal(actual_row["distinct_orders"]),
	}

    # ---------- 3. сравнение ----------
	for check_type, expected_value in expected.items():
		if check_type not in actual:
			raise ValueError(f"Unknown check_type: {check_type}")

		actual_value = actual[check_type]
	
		if actual_value != expected_value:
			return False
	
	return True

def insert_into_process_table(**context):
	
	# ti — это TaskInstance. «Дай мне экземпляр текущей задачи, которая прямо сейчас выполняется».
	ti = context["ti"]

	batch_id = ti.xcom_pull(
		task_ids="check_batch",
		key="batch_id"
	)

	print("Обрабатываемый батч:", batch_id)

    # вставляем строку в таблицу
	query = """
    	INSERT INTO test.sensor_processed_batches (batch_id, table_name)
    	VALUES ({batch_id}, 'sensor_fact_table')
    """	
	run_query_text(query)


########## @task
def run_remote_etl():

	ch = BaseHook.get_connection("click_onpremise_http_etl")

	env = f"""
    export CLICKHOUSE_URL="http://{ch.host}:{ch.port}"
    export CLICKHOUSE_USER="{ch.login}"
    export CLICKHOUSE_PASSWORD="{ch.password}"
    export CLICKHOUSE_DATABASE="{ch.extra_dejson.get('database', 'test')}"
    """

	return f"""
    set -e
    {env}
    python3 /home/airflowetl/MG/test_etl/test_my_clickhouse_job.py
    """


@dag(
	dag_id="test_sensor_2026_02_XX",
	start_date=datetime(2026, 2, 5),
	schedule=None,
	#schedule="0 10 * * *",  # запускается каждый день в 10:00
	catchup=False,			# По умолчанию (catchup=True) Airflow пытается “догнать” все пропущенные даты пока не дойдёт до сегодняшнего дня. catchup=False — не догоняем прошлое
	tags=['test'],
)
def test_sensor_2026_02_05():

    # ---------- SENSOR ----------
	@task.sensor(
		poke_interval=30,		# Как часто спрашиваем = через каждые 30 секунд
		timeout=300,			# Сколько вообще готовы ждать = 300 секунд. Если now - start_time > timeout, Sensor падает с ошибкой (SensorTimeout)
					# Sensor будет повторять poke каждые 30 секунд, пока суммарное время с момента первого poke не превысит 300 секунд.
		mode="reschedule"		# Держим ли worker или нет. 
					# mode="poke" - ждать «на месте», worker заблокирован. mode="reschedule" - ждать «в стороне», worker освобождён. mode="reschedule" - лучше!
					# mode="reschedule": Sensor освобождает worker между poke
	)
	def wait_for_new_batch(**context):
		return check_new_batch(**context)	# Scheduler вызывает check_new_batch() каждый poke_interval, пока timeout не истечёт
					# check_data_exists(): Должна быть быстрой, потому что вызывается каждые poke_interval секунд. Возвращает только True/False.

	# TaskFlow API
	@task
	def check_batch(**context):

		# ti — это TaskInstance. «Дай мне экземпляр текущей задачи, которая прямо сейчас выполняется».
		ti = context["ti"]

		batch_id = ti.xcom_pull(
			task_ids="wait_for_new_batch",
			key="batch_id"
		)

		if not batch_id:
			raise ValueError("batch_id не найден в XCom")

		if not compare_checksums(batch_id):
			raise ValueError(f"Несоответствие контрольных сумм для батча {batch_id}")

		print(f"Контрольные суммы в порядке для батча {batch_id}")

		# (опционально) пробросим дальше
		ti.xcom_push(key="batch_id", value=batch_id)


    # ---------- обычная task = PythonOperator ----------
	@task
	def process_data(**context):
		return insert_into_process_table(**context)


	command_x = run_remote_etl()

	ssh_run_etl = SSHOperator(
		task_id="ssh_run_etl",
		ssh_conn_id="airflowetl_ssh",
		command=command_x,
	)


    # зависимости
	wait_for_new_batch() >> check_batch() >> process_data() >> ssh_run_etl

    # wait_for_new_batch() - не выполняет код, а создаёт SensorOperator в DAG, она становится Task объектом DAG.
    # wait_for_new_data() — это Sensor Task. Airflow не выполняет её сразу. Scheduler каждые poke_interval секунд вызывает внутри этой задачи функцию, которая проверяет условие.
    # process_data() - не выполняет код, а создаёт PythonOperator в DAG
    # Scheduler вызывает process_data() после SUCCES прерыдущего Task

# регистрация DAG
dag = test_sensor_2026_02_05()


  
# ti = context["ti"] - ti — это TaskInstance. Полное имя класса: airflow.models.taskinstance.TaskInstance. 

# В Airflow есть три уровня:
# DAG           — схема
# Task          — узел в схеме
# TaskInstance  — реальное выполнение задачи

# Пример:
# DAG:            test_sensor_2026_02_05
# Task:           wait_for_new_batch
# TaskInstance:   wait_for_new_batch @ 2026-02-09T10:00

# XCom живёт на уровне TaskInstance, а не Task.
# Поэтому:
# писать XCom можно только через ti
# читать XCom тоже через ti

# Что лежит в ti
# ti.task_id          # 'wait_for_new_batch'
# ti.dag_id           # 'test_sensor_2026_02_05'
# ti.run_id           # 'manual__2026-02-09T10:00:00+00:00'
# ti.execution_date   # logical_date
# ti.try_number       # номер попытки
# И главное для тебя:
# ti.xcom_push(key, value)
# ti.xcom_pull(...)

# Почему это лежит в context
# Airflow передаёт в **context всё, что может понадобиться задаче:
# ti — текущая TaskInstance
# dag
# task
# даты
# params
# и т.д.

# context = {
#     'dag': DAG(...),
#     'task': BaseOperator(...),
#     'ti': TaskInstance(...),
#     'ds': '2026-02-09',
#     'execution_date': datetime(...),
#     'logical_date': datetime(...),
#     'run_id': 'scheduled__...',
#     'params': {
#         'batch_id': '20260208_001'
#     },
#     ...
# }
# Airflow сам формирует этот словарь и передаёт его в Task.