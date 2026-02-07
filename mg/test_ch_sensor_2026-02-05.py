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


def run_query(query: str):
    hook = HttpHook(method="POST", http_conn_id="click_onpremise_http")
    response = hook.run(
        endpoint="",
        data=query,
        headers={"Content-Type": "text/plain"},
    )
    return response.text

def check_new_batch():

    # пример: проверяем, что появилась хотя бы одна запись за последний час
    query = """
    	SELECT count()			# В сенсоре всегда использовать count() - это золотое правило сенсоров
	FROM test.sensor_load_batches
	WHERE table_name = 'sensor_fact_table'
  	AND batch_id NOT IN (
      		SELECT batch_id
      		FROM test.sensor_processed_batches
  	)
    """
    result = run_query(query).strip()
    
    print("(MikGrap) RAW RESULT:", result)   # <-- важно - Это сообщение выводится в логах выполнения DAG

    if not result:
        return False

    # ClickHouse по HTTP всегда вернёт число	
    return int(result) > 0

    #count = result.result_rows[0][0]  # получаем число записей
    #return count > 0

    # True → Sensor завершился успешно
    # False → Sensor будет ждать следующей попытки


def insert_into_process_table():

    # вставляем строку в таблицу
    query = """
    	INSERT INTO test.sensor_processed_batches (batch_id, table_name)
    	SELECT batch_id, table_name
	FROM test.sensor_load_batches
	WHERE table_name = 'sensor_fact_table'
  	AND batch_id NOT IN (
      		SELECT batch_id
      		FROM test.sensor_processed_batches
  	)
	ORDER BY finished_sourse_datetime
	LIMIT 1
    """	
    run_query(query)


#@task
def run_remote_etl():

    ch = BaseHook.get_connection("click_onpremise_http_etl")

    env = f"""
    export CLICKHOUSE_URL="http://{ch.host}:{ch.port}"
    export CLICKHOUSE_USER="{ch.login}"
    export CLICKHOUSE_PASSWORD="{ch.password}"
    export CLICKHOUSE_DATABASE="{ch.extra_dejson.get('database', 'default')}"
    """

    return f"""
    set -e
    {env}
    python3 /home/airflowetl/MG/test_etl/test_my_clickhouse_job.py
    """


@dag(
    dag_id="test_sensor_2026_02_05",
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
    def wait_for_new_batch():
        return check_new_batch()	# Scheduler вызывает check_new_batch() каждый poke_interval, пока timeout не истечёт
					# check_data_exists(): Должна быть быстрой, потому что вызывается каждые poke_interval секунд. Возвращает только True/False.

    # ---------- обычная task = PythonOperator ----------
    @task
    def process_data():
        return insert_into_process_table()


    command_x = run_remote_etl()

    ssh_run_etl = SSHOperator(
        task_id="ssh_run_etl",
        ssh_conn_id="airflowetl_ssh",
        command=command_x,
    )


    # зависимости
    wait_for_new_batch() >> process_data() >> ssh_run_etl

    # wait_for_new_batch() - не выполняет код, а создаёт SensorOperator в DAG, она становится Task объектом DAG.
    # wait_for_new_data() — это Sensor Task. Airflow не выполняет её сразу. Scheduler каждые poke_interval секунд вызывает внутри этой задачи функцию, которая проверяет условие.
    # process_data() - не выполняет код, а создаёт PythonOperator в DAG
    # Scheduler вызывает process_data() после SUCCES прерыдущего Task

# регистрация DAG
dag = test_sensor_2026_02_05()


  