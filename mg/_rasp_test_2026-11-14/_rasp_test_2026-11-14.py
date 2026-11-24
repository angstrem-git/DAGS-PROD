# Устанавливаем значение переменной RELEASE
RELEASE = "v02"

from importlib import import_module
# Динамически импортируем функцию из файла, где имя папки зависит от RELEASE
module = import_module(f"mg._rasp_test_2026-11-14.py.{RELEASE}_001_wait_for_batch")
wait_for_batch = getattr(module, "wait_for_batch")      
module = import_module(f"mg._rasp_test_2026-11-14.py.{RELEASE}_002_check_sum")
check_sum = getattr(module, "check_sum")  
module = import_module(f"mg._rasp_test_2026-11-14.py.{RELEASE}_003_check_input_data")
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


def b01_is_work_day_pick_branch_by_day_of_week(**context):

    dt = get_data_interval_end_msk(context)
    weekday = dt.weekday()

    branches = {
        0: "tg_check_data_id.check_001_wait_for_batch_id",           # Понедельник
        1: "tg_check_data_id.check_001_wait_for_batch_id",           # Вторник
        2: "tg_check_data_id.check_001_wait_for_batch_id",           # Среда
        3: "tg_check_data_id.check_001_wait_for_batch_id",           # Четверг
        4: "tg_check_data_id.check_001_wait_for_batch_id",           # Пятница
        5: "e01_not_work_day_empty_id",                              # Суббота
        6: "e01_not_work_day_empty_id",                              # Воскресенье
    }

    return branches[weekday]


def b02_is_monday_pick_branch_by_day_of_week(**context):

    dt = get_data_interval_end_msk(context)
    weekday = dt.weekday()

    branches = {
        0: "tg_saturday_id.sat_003_check_input_data_id",           # Понедельник
        1: "e02_not_monday_empty_id",                              # Вторник
        2: "e02_not_monday_empty_id",                              # Среда
        3: "e02_not_monday_empty_id",                              # Четверг
        4: "e02_not_monday_empty_id",                              # Пятница
        5: "e02_not_monday_empty_id",                              # Суббота
        6: "e02_not_monday_empty_id",                              # Воскресенье
    }

    return branches[weekday]


with DAG(
    dag_id=f"_rasp_{RELEASE}_test_2026-11-14",
    description=f"Преобразование данных rasp {RELEASE}",
    start_date=pendulum.datetime(2025, 12, 31, tz=LOCAL_TZ),
    schedule = "30 7 * * *",
    catchup=False,
    tags=['test']
) as dag:

    with TaskGroup("tg_check_data_id") as tg_check_data:

        check_001_wait_for_batch = PythonSensor(
        #check_001_wait_for_batch = PythonOperator(						
            task_id="check_001_wait_for_batch_id",
            poke_interval=5,
            timeout=20,
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
            subject="Backfill - {{ data_interval_end.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}",
            html_content="""
            <h2>batch_id_dttm = {{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}</h2>
            <h2>batch_id_str = {{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_str") }}</h2>
            <h2>date_id = {{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="date_id") }}</h2>
            """,
        )

        (
            check_001_wait_for_batch
            >> check_002_check_sum
            >> check_003_check_input_data
            >> check_004_send_email
        )


    with TaskGroup("tg_work_day_09_31_id") as tg_work_day_09_31:

        wrk_009_parametry_zakaza = ClickHouseOperator(
            task_id="wrk_009_parametry_zakaza_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_009_insert_parametry_zakaza.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1						
            }
        )

        wrk_010_all_goods = ClickHouseOperator(
            task_id="wrk_010_all_goods_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_010_insert_all_goods.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "days_back": 0							
            }
        )

        wrk_020_all_orders = ClickHouseOperator(
            task_id="wrk_020_all_orders_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_020_insert_all_orders.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2 						
            }
        )

        wrk_031_self_order_monday_friday = ClickHouseOperator(
            task_id="wrk_031_self_order_monday_friday_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_031_insert_self_order_monday_friday.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "db3": DB3 						
            }
        )

        (
            wrk_009_parametry_zakaza 
            >> wrk_010_all_goods 
            >> wrk_020_all_orders 
            >> wrk_031_self_order_monday_friday 
        )


    with TaskGroup("tg_work_day_33_70_id") as tg_work_day_33_70:

        wrk_033_self_order_services = ClickHouseOperator(
            task_id="wrk_033_self_order_services_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_033_insert_self_order_services.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0							
            }
        )

        wrk_034_self_order_exit = ClickHouseOperator(
            task_id="wrk_034_self_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_034_insert_self_order_exit.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0					
            }
        )
    
        wrk_041_full_order = ClickHouseOperator(
            task_id="wrk_041_full_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_041_insert_full_order.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 						
            }
        )

        wrk_042_full_order_exit = ClickHouseOperator(
            task_id="wrk_042_full_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_042_insert_full_order_exit.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 						
            }
        )

        wrk_050_has_bridge = ClickHouseOperator(
            task_id="wrk_050_has_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_050_insert_order_has_goods_type_bridge.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 						
            }
        )

        wrk_060_deficit_bridge = ClickHouseOperator(
            task_id="wrk_060_deficit_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_060_insert_order_deficit_goods_type_bridge.sql",	# Относительный путь
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 					
            } 
        )

        wrk_070_goods_in_order_stage = ClickHouseOperator(
            task_id="wrk_070_goods_in_order_stage_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_070_insert_goods_in_order_stage.sql",	# Относительный путь
            params={			
                "db1": DB1,		
                "db2": DB2,			
                "db3": DB3,
                "days_back": 0 					
            } 
        )

        (
            wrk_033_self_order_services 
            >> wrk_034_self_order_exit 
            >> wrk_041_full_order 
            >> wrk_042_full_order_exit 
            >> wrk_050_has_bridge 
            >> wrk_060_deficit_bridge 
            >> wrk_070_goods_in_order_stage 
        )


    with TaskGroup("tg_work_day_80_86_id") as tg_work_day_80_86:

        wrk_080_fct_deficit_packet_order = ClickHouseOperator(
            task_id="wrk_080_fct_deficit_packet_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_080_insert_fct_deficit_packet_order.sql",			# Относительный путь относительно файла DAG-а
            params={	
                "db1": DB1,			
                "db3": DB3									
            }
        )

        wrk_081_rank_process = SSHOperator(
            task_id="wrk_081_rank_process_id",
            ssh_conn_id="airflowetl_ssh",
            cmd_timeout=3600,
            command=f"""
                export CLICKHOUSE_URL="{URL}"
                export CLICKHOUSE_USER="{USER}"
                export CLICKHOUSE_PASSWORD="{PASSWORD}"							
                export CLICKHOUSE_DATABASE2="{DB2}"
                export CLICKHOUSE_DATABASE3="{DB3}"

                python3 /home/airflowetl/AIRFLOW-ETL-MACHINE/rasp/rasp_{RELEASE}/{RELEASE}_081_rank_process.py \
                '{{{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}}}'
            """
        )

        wrk_082_fct_unit_zayavka_na_otgruzku = ClickHouseOperator(
            task_id="wrk_082_fct_unit_zayavka_na_otgruzku_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_082_insert_fct_unit_zayavka_na_otgruzku.sql",			# Относительный путь относительно файла DAG-а
            params={	
                "db1": DB1,			
                "db3": DB3									
            }
        )

        wrk_083_fct_deficit_order_rank = ClickHouseOperator(
            task_id="wrk_083_fct_deficit_order_rank_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_083_insert_fct_deficit_order_rank.sql",			# Относительный путь относительно файла DAG-а
            params={	
                "db1": DB1,			
                "db2": DB2,					
                "db3": DB3					
            }
        )

        wrk_084_fct_deficit_packet_rank = ClickHouseOperator(
            task_id="wrk_084_fct_deficit_packet_rank_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_084_insert_fct_deficit_packet_rank.sql",			# Относительный путь относительно файла DAG-а
            params={	
                "db1": DB1,		
                "db2": DB2,					
                "db3": DB3								
            }
        )

        wrk_085_fct_deficit_packet_array_rank = ClickHouseOperator(
            task_id="wrk_085_fct_deficit_packet_array_rank_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_085_insert_fct_deficit_packet_array_rank.sql",			# Относительный путь относительно файла DAG-а
            params={
                "db1": DB1,				
                "db3": DB3									
            }
        )

        wrk_086_dim_packet_processed_batches = ClickHouseOperator(
            task_id="wrk_086_dim_packet_processed_batches_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_086_insert_dim_packet_processed_batches.sql",			# Относительный путь относительно файла DAG-а
            params={
                "db1": DB1,				
                "db3": DB3									
            }
        )

        (
            wrk_080_fct_deficit_packet_order 
            >> wrk_081_rank_process 
            >> wrk_082_fct_unit_zayavka_na_otgruzku 
            >> wrk_083_fct_deficit_order_rank 
            >> wrk_084_fct_deficit_packet_rank 
            >> wrk_085_fct_deficit_packet_array_rank 
            >> wrk_086_dim_packet_processed_batches
        )


    with TaskGroup("tg_saturday_id") as tg_saturday:

        # ShortCircuitOperator останавливает выполнение следующих task, 
        # если условие в нем возвращает False.
        # В данном случае: остановить DAG, если нет данных.
        sat_003_check_input_data = ShortCircuitOperator(
            task_id="sat_003_check_input_data_id",
            python_callable=check_input_data,
            op_kwargs={
                "RELEASE_key": RELEASE,
                "DAG_DIR_key": DAG_DIR,
                "DATE_ID_key": "{{ (data_interval_end.in_timezone('Europe/Moscow') - macros.timedelta(days=2)).strftime('%Y-%m-%d') }}"     # Минус 2 дня
            }
        )

        sat_010_all_goods = ClickHouseOperator(
            task_id="sat_010_all_goods_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_010_insert_all_goods.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "days_back": 2							
            }
        )

        sat_032_self_order_saturday_sunday = ClickHouseOperator(
            task_id="sat_032_self_order_saturday_sunday_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_032_insert_self_order_saturday_sunday.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "db3": DB3,
                "days_back": 2	 						
            }
        )

        sat_033_self_order_services = ClickHouseOperator(
            task_id="sat_033_self_order_services_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_033_insert_self_order_services.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2							
            }
        )

        sat_034_self_order_exit = ClickHouseOperator(
            task_id="sat_034_self_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_034_insert_self_order_exit.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2					
            }
        )
    
        sat_041_full_order = ClickHouseOperator(
            task_id="sat_041_full_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_041_insert_full_order.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 						
            }
        )

        sat_042_full_order_exit = ClickHouseOperator(
            task_id="sat_042_full_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_042_insert_full_order_exit.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 						
            }
        )

        sat_050_has_bridge = ClickHouseOperator(
            task_id="sat_050_has_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_050_insert_order_has_goods_type_bridge.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 						
            }
        )

        sat_060_deficit_bridge = ClickHouseOperator(
            task_id="sat_060_deficit_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_060_insert_order_deficit_goods_type_bridge.sql",	# Относительный путь
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 					
            } 
        )

        sat_070_goods_in_order_stage = ClickHouseOperator(
            task_id="sat_070_goods_in_order_stage_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_070_insert_goods_in_order_stage.sql",	# Относительный путь
            params={			
                "db1": DB1,		
                "db2": DB2,			
                "db3": DB3,
                "days_back": 2 					
            } 
        )

        (
            sat_003_check_input_data 
            >> sat_010_all_goods 
            >> sat_032_self_order_saturday_sunday 
            >> sat_033_self_order_services 
            >> sat_034_self_order_exit 
            >> sat_041_full_order 
            >> sat_042_full_order_exit 
            >> sat_050_has_bridge 
            >> sat_060_deficit_bridge 
            >> sat_070_goods_in_order_stage 
        )


    with TaskGroup("tg_sunday_id") as tg_sunday:

        # ShortCircuitOperator останавливает выполнение следующих task, 
        # если условие в нем возвращает False.
        # В данном случае: остановить DAG, если нет данных.
        sun_003_check_input_data = ShortCircuitOperator(
            task_id="sun_003_check_input_data_id",
            python_callable=check_input_data,
            op_kwargs={
                "RELEASE_key": RELEASE,
                "DAG_DIR_key": DAG_DIR,
                "DATE_ID_key": "{{ (data_interval_end.in_timezone('Europe/Moscow') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"     # Минус 1 день
            }
        )

        sun_010_all_goods = ClickHouseOperator(
            task_id="sun_010_all_goods_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_010_insert_all_goods.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "days_back": 1							
            }
        )

        sun_032_self_order_saturday_sunday = ClickHouseOperator(
            task_id="sun_032_self_order_saturday_sunday_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_032_insert_self_order_saturday_sunday.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "db3": DB3,
                "days_back": 1	 						
            }
        )

        sun_033_self_order_services = ClickHouseOperator(
            task_id="sun_033_self_order_services_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_033_insert_self_order_services.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1							
            }
        )

        sun_034_self_order_exit = ClickHouseOperator(
            task_id="sun_034_self_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_034_insert_self_order_exit.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1					
            }
        )
    
        sun_041_full_order = ClickHouseOperator(
            task_id="sun_041_full_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_041_insert_full_order.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 						
            }
        )

        sun_042_full_order_exit = ClickHouseOperator(
            task_id="sun_042_full_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_042_insert_full_order_exit.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 						
            }
        )

        sun_050_has_bridge = ClickHouseOperator(
            task_id="sun_050_has_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_050_insert_order_has_goods_type_bridge.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 						
            }
        )

        sun_060_deficit_bridge = ClickHouseOperator(
            task_id="sun_060_deficit_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_060_insert_order_deficit_goods_type_bridge.sql",	# Относительный путь
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 					
            } 
        )

        sun_070_goods_in_order_stage = ClickHouseOperator(
            task_id="sun_070_goods_in_order_stage_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_070_insert_goods_in_order_stage.sql",	# Относительный путь
            params={			
                "db1": DB1,		
                "db2": DB2,			
                "db3": DB3,
                "days_back": 1 					
            } 
        )

        (
            sun_003_check_input_data 
            >> sun_010_all_goods 
            >> sun_032_self_order_saturday_sunday 
            >> sun_033_self_order_services 
            >> sun_034_self_order_exit 
            >> sun_041_full_order 
            >> sun_042_full_order_exit 
            >> sun_050_has_bridge 
            >> sun_060_deficit_bridge 
            >> sun_070_goods_in_order_stage
        )


    # Функция b01_is_work_day_pick_branch_by_day_of_week() возвращает строку с task_id задачи, 
    # которую должен выбрать BranchPythonOperator.
    # BranchPythonOperator не запускает эту задачу напрямую. Он:
    # 1. Выполняет свою задачу: b01_is_work_day_pick_branch_id
    # 2. Получает результат: "tg_check_data_id.check_001_wait_for_batch_id"
    # 3. Помечает остальные ветки как: skipped.
    # Дальше уже Airflow scheduler запускает выбранную задачу.
    # BranchPythonOperator выбирает следующую ветку по task_id, а не запускает её сам.
    b01_is_work_day_pick_branch = BranchPythonOperator(
        task_id="b01_is_work_day_pick_branch_id",
        python_callable=b01_is_work_day_pick_branch_by_day_of_week,
    )

    b02_is_monday_pick_branch = BranchPythonOperator(
        task_id="b02_is_monday_pick_branch_id",
        python_callable=b02_is_monday_pick_branch_by_day_of_week,
    )

    e01_not_work_day_empty = EmptyOperator(
        task_id="e01_not_work_day_empty_id",
    )

    e02_not_monday_empty = EmptyOperator(
        task_id="e02_not_monday_empty_id",
    )

    # NONE_FAILED_MIN_ONE_SUCCESS означает:
    # Среди всех upstream-задач не должно быть ни одной с состоянием failed или upstream_failed, 
    # и хотя бы одна задача должна завершиться успешно (success).
    # То есть разрешённые состояния:
    #     - success
    #     - skipped
    # Запрещены:
    #     - failed
    #     - upstream_failed
    # И обязательно:
    #     - хотя бы один success

    # Не использовать NONE_FAILED, т.к. для него все skipped - тоже успех!
    j01_join_branch = EmptyOperator(
        task_id="j01_join_branch_id",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    j02_join_branch = EmptyOperator(
        task_id="j02_join_branch_id",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )


    b01_is_work_day_pick_branch >> [e01_not_work_day_empty, tg_check_data] 
    tg_check_data >> tg_work_day_09_31 >> b02_is_monday_pick_branch 
    b02_is_monday_pick_branch >> [e02_not_monday_empty, tg_saturday] 
    tg_saturday >> tg_sunday 
    [e02_not_monday_empty, tg_sunday] >> j02_join_branch 
    j02_join_branch >> tg_work_day_33_70 >> tg_work_day_80_86 
    [e01_not_work_day_empty, tg_work_day_80_86] >> j01_join_branch 

