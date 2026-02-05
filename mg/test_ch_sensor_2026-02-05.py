#from airflow.sdk import DAG
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.providers.standard.operators.python import PythonOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from pendulum import datetime

from airflow.decorators import dag, task
from datetime import datetime
from clickhouse_connect import Client

# параметры подключения к ClickHouse
CLICKHOUSE_HOST = "clickhouse.angstrem.net"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "Bgt%rfc4"
CLICKHOUSE_DATABASE = "test"

def check_new_batch():

    client = Client(
    	host=CLICKHOUSE_HOST,
    	port=CLICKHOUSE_PORT,
    	username=CLICKHOUSE_USER,
      	password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

    # пример: проверяем, что появилась хотя бы одна запись за последний час
    query = """
    	SELECT batch_id
	FROM test.sensor_load_batches
	WHERE table_name = 'sensor_load_batches'
  	AND batch_id NOT IN (
      		SELECT batch_id
      		FROM test.sensor_processed_batches
  	)
	ORDER BY finished_sourse_datetime
	LIMIT 1
    """
    result = client.query(query)
    count = result.result_rows[0][0]  # получаем число записей

    # True → Sensor завершился успешно
    # False → Sensor будет ждать следующей попытки
    return count > 0

def insert_into_process_table():

    client = Client(
    	host=CLICKHOUSE_HOST,
    	port=CLICKHOUSE_PORT,
    	username=CLICKHOUSE_USER,
      	password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

    # вставляем строку в таблицу
    query = """
    	INSERT INTO test.sensor_processed_batches (batch_id, table_name)
    	SELECT batch_id, table_name
	FROM test.sensor_load_batches
	WHERE table_name = 'sensor_load_batches'
  	AND batch_id NOT IN (
      		SELECT batch_id
      		FROM test.sensor_processed_batches
  	)
	ORDER BY finished_sourse_datetime
	LIMIT 1
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

    # зависимости
    wait_for_new_batch() >> process_data()	

    # wait_for_new_batch() - не выполняет код, а создаёт SensorOperator в DAG, она становится Task объектом DAG.
    # wait_for_new_data() — это Sensor Task. Airflow не выполняет её сразу. Scheduler каждые poke_interval секунд вызывает внутри этой задачи функцию, которая проверяет условие.
    # process_data() - не выполняет код, а создаёт PythonOperator в DAG
    # Scheduler вызывает process_data() после SUCCES прерыдущего Task

# регистрация DAG
dag = test_sensor_2026_02_05()


  