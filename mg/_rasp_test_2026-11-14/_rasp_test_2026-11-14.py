# Устанавливаем значение переменной RELEASE
RELEASE = "v02"

from importlib import import_module
# Динамически импортируем функцию из файла, где имя папки зависит от RELEASE
#module = import_module(f"mg.rasp_pipeline.rasp_{RELEASE}.py.{RELEASE}_t01_wait_for_batch")
#wait_for_batch = getattr(module, f"{RELEASE}_001_wait_for_batch")  
# = wait_for_batch = module.v02_001_wait_for_batch
module = import_module(f"mg._rasp_test_2026-11-14.{RELEASE}_001_wait_for_batch")
wait_for_batch = getattr(module, "wait_for_batch")  
#module = import_module(f"mg.rasp_pipeline.rasp_{RELEASE}.py.{RELEASE}_t02_check_sum")
#check_sum = getattr(module, "check_sum")    
module = import_module(f"mg._rasp_test_2026-11-14.{RELEASE}_002_check_sum")
check_input_data = getattr(module, "check_sum")  

module = import_module(f"mg._rasp_test_2026-11-14.{RELEASE}_003_check_input_data")
check_input_data = getattr(module, "check_input_data") 


import pendulum
import requests
import urllib.parse
from pathlib import Path
from pendulum import datetime
from requests.auth import HTTPBasicAuth
from airflow.sdk import DAG														
from airflow.sdk import Variable												
from airflow.hooks.base import BaseHook												
from airflow.providers.standard.sensors.python import PythonSensor				
from airflow.providers.standard.operators.python import PythonOperator			
from airflow.providers.ssh.operators.ssh import SSHOperator						
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator	
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator			
from airflow.utils.trigger_rule import TriggerRule	
from airflow.providers.standard.operators.python import BranchPythonOperator	
from airflow.providers.smtp.operators.smtp import EmailOperator                 
													# Лучше from pendulum, чем from datetime			

LOCAL_TZ = pendulum.timezone("Europe/Moscow")
E_MAIL = "M.Grapenyuk@angstrem.net"
SMTP_CONN_ID = "smtp_angstrem"

DB1 = f"rasp1_{RELEASE}"
DB2 = f"rasp2_{RELEASE}"
DB3 = f"rasp3_{RELEASE}"

CH_HOOK = BaseHook.get_connection("click_onpremise_http_etl")
URL = f"http://{CH_HOOK.host}:{CH_HOOK.port}"
USER = CH_HOOK.login
PASSWORD = CH_HOOK.password

DAG_DIR = Path(__file__).parent  # Путь к текущей папке DAG


def get_data_interval_end_msk(context):
    'Перевод в Московское время (в Airflow даты по UTC = -3 часа от Москвы)'
    return context["data_interval_end"].in_timezone(LOCAL_TZ)


def b01_is_work_day_pick_branch_by_day_of_week (**context):

    dt = get_data_interval_end_msk(context)
    weekday = dt.weekday()

    branches = {
        0: "tg_check_data.chk_002_check_sum",           # Понедельник
        1: "tg_check_data.chk_002_check_sum",           # Вторник
        2: "tg_check_data.chk_002_check_sum",           # Среда
        3: "tg_check_data.chk_002_check_sum",           # Четверг
        4: "tg_check_data.chk_002_check_sum",           # Пятница
        5: "e01_not_work_day_empty_id",                              # Суббота
        6: "e01_not_work_day_empty_id",                              # Воскресенье
    }

    return branches[weekday]


def b02_is_monday_pick_branch_by_day_of_week (**context):

    dt = get_data_interval_end_msk(context)
    weekday = dt.weekday()

    branches = {
        0: "tg_saturday_id.chk_003_check_input_data",        # Понедельник
        1: "e02_not_monday_empty_id",                              # Вторник
        2: "e02_not_monday_empty_id",                              # Среда
        3: "e02_not_monday_empty_id",                              # Четверг
        4: "e02_not_monday_empty_id",                              # Пятница
        5: "e02_not_monday_empty_id",                              # Суббота
        6: "e02_not_monday_empty_id",                              # Воскресенье
    }

    return branches[weekday]


# def check_input_data(**context):
#     'Возвращает признак: Есть ли данные ?'

#     dt = get_data_interval_end_msk(context).date()

#     query_text = f"""
#         SELECT
#             (
#                 SELECT count()	
#                 FROM rasp2.open_orders_goods_history_rn
#                 WHERE date_id = toDate('{dt}')
#             )												AS count_open_orders
#             ,
#             (
#                 SELECT count()	
#                 FROM rasp2.open_orders_discounts_in_goods_rn_history
#                 WHERE date_id = toDate('{dt}')
#             )												AS count_orders_discounts
#             ,
#             (
#                 SELECT count()	
#                 FROM rasp2.mg_VPointStatusIndividualOrders
#                 WHERE date_id = subtractDays(toDate('{dt}'), 1)
#             )												AS count_status_mip
#     """
    
#     hook = ClickHouseHook(clickhouse_conn_id="click_onpremise_airflow")     # Порт 9000

#     result = hook.execute(query_text)   # Возвращает [(count_open_orders, count_orders_discounts, count_status_mip)]
#     cnt1, cnt2, cnt3 = result[0]

#     return (cnt1 > 0) and (cnt2 > 0) and (cnt3 > 0)


with DAG(
    dag_id=f"_rasp_{RELEASE}_test_2026-11-14",
    description=f"Преобразование данных rasp {RELEASE}",
    start_date=pendulum.datetime(2025, 12, 31, tz=LOCAL_TZ),
    schedule = "30 7 * * *",
    catchup=False,
    tags=['rasp']
) as dag:

    with TaskGroup("tg_check_data_id") as tg_check_data:

        check_001_wait_for_batch = PythonSensor(						
            task_id="check_001_wait_for_batch_id",
            poke_interval=5,
            timeout=600,
            python_callable=wait_for_batch,
            op_kwargs={
                "RELEASE_key": RELEASE,
                "DAG_DIR_key": DAG_DIR,
                "DB1_key": DB1,
                "DB3_key": DB3,
                "URL_key": URL,
                "USER_key": USER,
                "PASSWORD_key": PASSWORD,
                "DATE_ID_key": "{{ data_interval_end.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}"
            }
        )

        check_002_check_sum = PythonOperator(
            task_id="check_002_check_sum_id",
            python_callable=check_sum,
            op_kwargs={
                "RELEASE_key": RELEASE,
                "DAG_DIR_key": DAG_DIR,
                "DB1_key": DB1,
                "URL_key": URL,
                "USER_key": USER,
                "PASSWORD_key": PASSWORD
            }
        )

        # ShortCircuitOperator останавливает выполнение следующих task, 
        # если условие в нем возвращает False.
        # В данном случае: остановить DAG, если нет данных.
        check_003_check_input_data = ShortCircuitOperator(
            task_id="check_003_check_input_data_id",
            python_callable=check_input_data,
            op_kwargs={
                "RELEASE_key": RELEASE,
                "DAG_DIR_key": DAG_DIR,
                "DATE_ID_key": "{{ data_interval_end.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}"
            }
        )

        check_004_send_email = EmailOperator(
            task_id="check_004_send_email_id",
            conn_id="smtp_angstrem",
            to=E_MAIL,
            subject="Backfill - {{data_interval_end.in_timezone('Europe/Moscow').strftime('%Y-%m-%d')}}",
            html_content="""
            <h2>Test</h2>
            <h2>batch_id_dttm = {{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}</h2>
            <h2>date_id = {{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="date_id") }}</h2>
            """,
        )

        (
            check_001_wait_for_batch
            >> check_002_check_sum
            >> check_003_check_input_data
            >> check_004_send_email
        )

    
    tg_check_data

