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
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import ShortCircuitOperator
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


def check_input_data(**context):
    
    dt = context["data_interval_end"].date()

    query_text = f"""
        SELECT
            (
                SELECT count()
                FROM rasp2.open_orders_goods_history_rn
                WHERE date_id = toDate('{dt}')
            ) AS cnt_orders
            ,
            (
                SELECT count()
                FROM dev1.packet
                WHERE date_id = toDate('{dt}')
            ) AS cnt_packet
    """
    
    # params={
    #     "query": query_text
    # }

    # r = requests.get(
    #         URL,
    #         params=params,
    #         auth=HTTPBasicAuth(USER, PASSWORD)
    #     )
    # r.raise_for_status()
    # result = r.text.strip().split("\t")

    # cnt1 = int(result[0])
    # cnt2 = int(result[1])

    # return (cnt1 > 0) and (cnt2 > 0)

    hook = ClickHouseHook(clickhouse_conn_id="click_onpremise_airflow")     # Порт 9000

    result = hook.execute(query_text)   # Возвращает [(cnt_orders, cnt_packet)]
    cnt1, cnt2 = result[0]

    return (cnt1 > 0) and (cnt2 > 0)


# def get_batch_id_dttm(**context):

#     dt = context["data_interval_end"].date()

#     query_text = f"""
#         SELECT 
#             DISTINCT
#             batch_id_dttm 
#             ,batch_id_str
#             ,create_dttm
#             ,date_id
#         --FROM rasp3_v01.dim_packet_processed_batches 
#         FROM dev1.packet							-- Вернуть FROM rasp3_v01.dim_packet_processed_batches
#         WHERE date_id = toDate('{ dt }')	
#         ORDER BY create_dttm DESC	
#         LIMIT 1
#         FORMAT JSON
#     """

#     query_encoded = urllib.parse.quote(query_text)
#     full_url = f"{URL}/?database={DB1_test}&query={query_encoded}"

#     r = requests.post(
# 		full_url,
# 		auth=HTTPBasicAuth(USER, PASSWORD),
# 		headers={"Content-Type": "text/plain"},
# 		timeout=60,
# 	)
#     r.raise_for_status()

#     d = r.json()["data"]
#     if len(d) == 0:
#         return False
    
#     batch_id_dttm = d[0].get("batch_id_dttm")
#     context["ti"].xcom_push(
#         key="batch_id_dttm",
#         value=batch_id_dttm
#     )
    
#     date_id = d[0].get("date_id")
#     context["ti"].xcom_push(
#         key="date_id",
#         value=date_id
#         )
    
#     return True


def get_batch_id_dttm(**context):

    dt = context["data_interval_end"].date()

    query_text = f"""
        SELECT 
            DISTINCT
            batch_id_dttm 
            ,batch_id_str
            ,create_dttm
            ,date_id
        --FROM rasp3_v01.dim_packet_processed_batches 
        FROM dev1.packet							-- Вернуть FROM rasp3_v01.dim_packet_processed_batches
        WHERE date_id = toDate('{ dt }')	
        ORDER BY create_dttm DESC	
        LIMIT 1
    """
    
    hook = ClickHouseHook(clickhouse_conn_id="click_onpremise_airflow")     # Порт 9000

    result = hook.execute(query_text)   # Возвращает [(batch_id_dttm, batch_id_str, create_dttm, date_id)]
    batch_id_dttm, batch_id_str, create_dttm, date_id = result[0]
    
    context["ti"].xcom_push(
        key="batch_id_dttm",
        value=batch_id_dttm
    )
    
    context["ti"].xcom_push(
        key="date_id",
        value=date_id
        )
    
    return True


def print_xcom(**context):
    batch_id_dttm = context["ti"].xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm")
    print("batch_id_dttm =", batch_id_dttm)
    date_id = context["ti"].xcom_pull(task_ids="wait_for_batch", key="date_id")
    print("date_id =", date_id)


with DAG(
    dag_id=f"test_{RELEASE}_rasp",
    description="Преобразование данных rasp",
    start_date=pendulum.datetime(2024, 12, 31, tz=local_tz),
    schedule = "30 7 * * *",
    catchup=False,
    tags=['rasp']
) as dag:
    
    # ShortCircuitOperator останавливает выполнение следующих task, если условие в нем возвращает False
    # В данном случае: остановить DAG, если нет данных
    t00 = ShortCircuitOperator(
        task_id="check_input_data",
        python_callable=check_input_data
    )

    # Служебный таск: получить batch_id_dttm. Убрать, когда будут вставлять в продовый DAG
    t_001 = PythonOperator(
        task_id="wait_for_batch",
        python_callable=get_batch_id_dttm
    )

    t_002 = PythonOperator(
        task_id="print_xcom",
        python_callable=print_xcom
    )

    t00 >> t_001 >> t_002 

    # test_t01 = ClickHouseOperator(
    #     task_id="insert_into_airflow_dates_test",
    #     clickhouse_conn_id="click_onpremise_airflow",
    #     sql="""
    #         INSERT INTO test.airflow_dates_test(
    #             run_id 
    #             ,logical_date 
    #             ,ds 
    #             ,data_interval_start 
    #             ,data_interval_start_ds 
    #             ,data_interval_end 
    #             ,data_interval_end_ds 
    #             ,ts 
    #         )
    #         VALUES (
    #             '{{ run_id }}'
    #             ,parseDateTimeBestEffort('{{ logical_date }}', 'Europe/Moscow')
    #             ,toDate('{{ ds }}')
    #             ,parseDateTimeBestEffort('{{ data_interval_start }}', 'Europe/Moscow')
    #             ,toDate('{{ data_interval_start | ds }}')
    #             ,parseDateTimeBestEffort('{{ data_interval_end }}', 'Europe/Moscow')
    #             ,toDate('{{ data_interval_end | ds }}')
    #             ,parseDateTimeBestEffort('{{ ts }}', 'Europe/Moscow')
    #         )
    #     """
    # )


    # def debug_dates(**context):
    #     print("run_id = ", context["run_id"])
    #     print("ds = ", context["ds"])
    #     print("logical_date = ", context["logical_date"])
    #     print("data_interval_start = ", context["data_interval_start"])
    #     print("data_interval_end = ", context["data_interval_end"])
    #     print("logical_date tz =", context["logical_date"].tzinfo)
    #     print("logical_date iso =", context["logical_date"].isoformat())

    #     from pprint import pprint
    #     pprint(context)


    # test_t02 = PythonOperator(
    #     task_id="debug_dates",
    #     python_callable=debug_dates
    # )

    # test_t01 >> test_t02


    



  