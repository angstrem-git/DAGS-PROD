# Устанавливаем значение переменной RELEASE
RELEASE = "v02"

# Используем f-string для динамического формирования пути
#from importlib import import_module
# Динамически импортируем функцию из файла, где имя папки зависит от RELEASE
#module = import_module(f"mg.rasp_pipeline.rasp_{RELEASE}.py.{RELEASE}_t01_wait_for_batch")
#wait_for_batch = getattr(module, "wait_for_batch")
#from rasp_v01.py.v01_t01_wait_for_batch import wait_for_batch  # импортируем функцию wait_for_batch из отдельного файла v01_t01_wait_for_batch
#module = import_module(f"mg.rasp_pipeline.rasp_{RELEASE}.py.{RELEASE}_t02_check_sum")
#check_sum = getattr(module, "check_sum")
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
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator			
from airflow.utils.trigger_rule import TriggerRule	
from airflow.providers.standard.operators.python import BranchPythonOperator	# Для Airflow v3
from airflow.providers.smtp.operators.smtp import EmailOperator                 # Для Airflow v3
from pendulum import datetime													# Лучше from pendulum, чем from datetime			
import pendulum
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path


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


def b01_pick_branch_by_day_of_week (**context):

    dt = get_data_interval_end_msk(context)
    weekday = dt.weekday()

    branches = {
        0: "tg_work_day_01_31_id.r00_check_data_id",    # Понедельник
        1: "tg_work_day_01_31_id.r00_check_data_id",    # Вторник
        2: "tg_work_day_01_31_id.r00_check_data_id",    # Среда
        3: "tg_work_day_01_31_id.r00_check_data_id",    # Четверг
        4: "tg_work_day_01_31_id.r00_check_data_id",    # Пятница
        5: "e01_empty_id",                              # Суббота
        6: "e01_empty_id",                              # Воскресенье
    }

    return branches[weekday]


def b02_pick_branch_by_day_of_week (**context):

    dt = get_data_interval_end_msk(context)
    weekday = dt.weekday()

    branches = {
        0: "tg_saturday_id.sat_01_all_goods_id",        # Понедельник
        1: "e02_empty_id",                              # Вторник
        2: "e02_empty_id",                              # Среда
        3: "e02_empty_id",                              # Четверг
        4: "e02_empty_id",                              # Пятница
        5: "e02_empty_id",                              # Суббота
        6: "e02_empty_id",                              # Воскресенье
    }

    return branches[weekday]


def check_input_data(**context):
    'Возвращает признак: Есть ли данные ?'

    dt = get_data_interval_end_msk(context).date()

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
                FROM {DB1}.packet
                WHERE date_id = toDate('{dt}')
            ) AS cnt_packet
    """
    
    hook = ClickHouseHook(clickhouse_conn_id="click_onpremise_airflow")     # Порт 9000

    result = hook.execute(query_text)   # Возвращает [(cnt_orders, cnt_packet)]
    cnt1, cnt2 = result[0]

    return (cnt1 > 0) and (cnt2 > 0)


def set_xcom(**context): 
    'Запись данных в XCom'

    dt = get_data_interval_end_msk(context).date()

    query_text = f"""
        SELECT 
            DISTINCT
            batch_id_dttm 
            ,batch_id_str
            ,create_dttm
            ,date_id
        FROM rasp3_v01.dim_packet_processed_batches 
        --FROM {DB1}.packet				-- Вернуть FROM rasp3_v01.dim_packet_processed_batches
        WHERE date_id = toDate('{ dt }')	
        ORDER BY create_dttm DESC	
        LIMIT 1
    """
    
    hook = ClickHouseHook(clickhouse_conn_id="click_onpremise_airflow")     # Порт 9000

    result = hook.execute(query_text)   # Возвращает [(batch_id_dttm, batch_id_str, create_dttm, date_id)]
    batch_id_dttm, batch_id_str, create_dttm, date_id = result[0]
    
    context["ti"].xcom_push(
        key="batch_id_str",
        value=batch_id_str
    )
    
    context["ti"].xcom_push(
        key="date_id",
        value=str(date_id)
        )
    
    return True


def print_xcom(**context):
    batch_id_str = context["ti"].xcom_pull(task_ids="tg_work_day_01_31_id.r_001_set_xcom_id", key="batch_id_str")
    print("batch_id_str =", batch_id_str)
    date_id = context["ti"].xcom_pull(task_ids="tg_work_day_01_31_id.r_001_set_xcom_id", key="date_id")
    print("date_id =", date_id)


with DAG(
    dag_id=f"test_aaa_{RELEASE}_rasp",
    description="Преобразование данных rasp",
    start_date=pendulum.datetime(2024, 12, 31, tz=LOCAL_TZ),
    schedule = "30 7 * * *",
    catchup=False,
    tags=['rasp']
) as dag:


    with TaskGroup("tg_work_day_01_31_id") as tg_work_day_01_31:

        # ShortCircuitOperator останавливает выполнение следующих task, если условие в нем возвращает False
        # В данном случае: остановить DAG, если нет данных
        r00_check_data = ShortCircuitOperator(
            task_id="r00_check_data_id",
            python_callable=check_input_data
        )

        # Служебный таск: получить batch_id_dttm. Убрать, когда будут вставлять в продовый DAG
        r_001_set_xcom = PythonOperator(
            task_id="r_001_set_xcom_id",
            python_callable=set_xcom
        )

        r_002_print_xcom = PythonOperator(
            task_id="r_002_print_xcom_id",
            python_callable=print_xcom
        )

        # r_003_parametry_zakaza = ClickHouseOperator(
        #     task_id="r_003_parametry_zakaza_id",
        #     clickhouse_conn_id="click_onpremise_airflow",
        #     sql=f"sql/{RELEASE}_t_003_insert_parametry_zakaza.sql"	# Относительный путь относительно файла DAG-а
        # )

        r_01_all_goods = ClickHouseOperator(
            task_id="r_01_all_goods_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_01_insert_all_goods.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "days_back": 0							
            }
        )

        r_02_all_orders = ClickHouseOperator(
            task_id="r_02_all_orders_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_02_insert_all_orders.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2 						
            }
        )

        r_031_self_order_workdays = ClickHouseOperator(
            task_id="r_031_self_order_workdays_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_1_insert_self_order_monday_friday.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "db3": DB3 						
            }
        )

        (
            r00_check_data 
            >> r_001_set_xcom 
            >> r_002_print_xcom 
            >> r_01_all_goods 
            >> r_02_all_orders 
            >> r_031_self_order_workdays
        )


    with TaskGroup("tg_work_day_32_60_id") as tg_work_day_32_60:

        r_032_self_order_services = ClickHouseOperator(
            task_id="r_032_self_order_services_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_2_insert_self_order_services.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0							
            }
        )

        r_033_self_order_exit = ClickHouseOperator(
            task_id="r_033_self_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_3_insert_self_order_exit.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0					
            }
        )
    
        r_04_full_order = ClickHouseOperator(
            task_id="r_04_full_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_04_insert_full_order.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 						
            }
        )

        r_05_has_bridge = ClickHouseOperator(
            task_id="r_05_has_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_05_insert_order_has_goods_type_bridge.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 						
            }
        )

        r_06_deficit_bridge = ClickHouseOperator(
            task_id="r_06_deficit_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_06_insert_order_deficit_goods_type_bridge.sql",	# Относительный путь
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 0 					
            } 
        )

        (
            r_032_self_order_services 
            >> r_033_self_order_exit 
            >> r_04_full_order 
            >> r_05_has_bridge 
            >> r_06_deficit_bridge
        )


    with TaskGroup("tg_saturday_id") as tg_saturday:

        sat_01_all_goods = ClickHouseOperator(
            task_id="sat_01_all_goods_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_01_insert_all_goods.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "days_back": 2							
            }
        )

        sat_031_self_order_weekends = ClickHouseOperator(
            task_id="sat_031_self_order_weekends_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_1_insert_self_order_saturday_sunday.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "db3": DB3,
                "days_back": 2	 						
            }
        )

        sat_032_self_order_services = ClickHouseOperator(
            task_id="sat_032_self_order_services_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_2_insert_self_order_services.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2							
            }
        )

        sat_033_self_order_exit = ClickHouseOperator(
            task_id="sat_033_self_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_3_insert_self_order_exit.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2					
            }
        )
    
        sat_04_full_order = ClickHouseOperator(
            task_id="sat_04_full_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_04_insert_full_order.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 						
            }
        )

        sat_05_has_bridge = ClickHouseOperator(
            task_id="sat_05_has_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_05_insert_order_has_goods_type_bridge.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 						
            }
        )

        sat_06_deficit_bridge = ClickHouseOperator(
            task_id="sat_06_deficit_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_06_insert_order_deficit_goods_type_bridge.sql",	# Относительный путь
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 2 					
            } 
        )

        (
            sat_01_all_goods 
            >> sat_031_self_order_weekends 
            >> sat_032_self_order_services 
            >> sat_033_self_order_exit 
            >> sat_04_full_order 
            >> sat_05_has_bridge 
            >> sat_06_deficit_bridge
        )


    with TaskGroup("tg_sunday_id") as tg_sunday:

        sun_01_all_goods = ClickHouseOperator(
            task_id="sun_01_all_goods_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_01_insert_all_goods.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "days_back": 1							
            }
        )

        sun_031_self_order_weekends = ClickHouseOperator(
            task_id="sun_031_self_order_weekends_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_1_insert_self_order_saturday_sunday.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db2": DB2,
                "db3": DB3,
                "days_back": 1	 						
            }
        )

        sun_032_self_order_services = ClickHouseOperator(
            task_id="sun_032_self_order_services_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_2_insert_self_order_services.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1							
            }
        )

        sun_033_self_order_exit = ClickHouseOperator(
            task_id="sun_033_self_order_exit_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_03_3_insert_self_order_exit.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1					
            }
        )
    
        sun_04_full_order = ClickHouseOperator(
            task_id="sun_04_full_order_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_04_insert_full_order.sql",	# Относительный путь относительно файла DAG-а
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 						
            }
        )

        sun_05_has_bridge = ClickHouseOperator(
            task_id="sun_05_has_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_05_insert_order_has_goods_type_bridge.sql",	# Относительный путь 
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 						
            }
        )

        sun_06_deficit_bridge = ClickHouseOperator(
            task_id="sun_06_deficit_bridge_id",
            clickhouse_conn_id="click_onpremise_airflow",
            sql=f"sql/{RELEASE}_06_insert_order_deficit_goods_type_bridge.sql",	# Относительный путь
            params={			
                "db1": DB1,					
                "db3": DB3,
                "days_back": 1 					
            } 
        )

        (
            sun_01_all_goods 
            >> sun_031_self_order_weekends 
            >> sun_032_self_order_services 
            >> sun_033_self_order_exit 
            >> sun_04_full_order 
            >> sun_05_has_bridge 
            >> sun_06_deficit_bridge
        )

    # Функция pick_branch_by_day_of_week() возвращает строку с task_id задачи, 
    # которую должен выбрать BranchPythonOperator.
    # BranchPythonOperator не запускает эту задачу напрямую. Он:
    # 1. Выполняет свою задачу: pick_branch_id
    # 2. Получает результат: "send_tuesday_email"
    # 3. Помечает остальные ветки как: skipped.
    # Дальше уже Airflow scheduler запускает выбранную задачу.
    # BranchPythonOperator выбирает следующую ветку по task_id, а не запускает её сам.
    b01_pick_branch = BranchPythonOperator(
        task_id="b01_pick_branch_id",
        python_callable=b01_pick_branch_by_day_of_week,
    )

    b02_pick_branch = BranchPythonOperator(
        task_id="b02_pick_branch_id",
        python_callable=b02_pick_branch_by_day_of_week,
    )

    e01_empty = EmptyOperator(
        task_id="e01_empty_id",
    )

    e02_empty = EmptyOperator(
        task_id="e02_empty_id",
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
    #     И обязательно:
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


    b01_pick_branch >> [e01_empty, tg_work_day_01_31]
    tg_work_day_01_31 >> b02_pick_branch
    b02_pick_branch >> [e02_empty, tg_saturday]
    tg_saturday >> tg_sunday
    [e02_empty, tg_sunday] >> j02_join_branch
    j02_join_branch >> tg_work_day_32_60 
    [e01_empty, tg_work_day_32_60] >> j01_join_branch

    

    