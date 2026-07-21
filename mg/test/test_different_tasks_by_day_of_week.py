import pendulum
from pendulum import datetime
from airflow.sdk import DAG			
from airflow.operators.empty import EmptyOperator			
from airflow.utils.trigger_rule import TriggerRule		
from airflow.providers.standard.operators.bash import BashOperator			    # Для Airflow v3
from airflow.providers.standard.operators.python import PythonOperator			# Для Airflow v3
from airflow.providers.standard.operators.python import BranchPythonOperator	# Для Airflow v3
from airflow.providers.smtp.operators.smtp import EmailOperator                 # Для Airflow v3


LOCAL_TZ = pendulum.timezone("Europe/Moscow")
E_MAIL = "M.Grapenyuk@angstrem.net"
SMTP_CONN_ID = "smtp_angstrem"


def get_data_interval_end_msk(context):
    'Перевод в Московское время (в Airflow даты по UTC = -3 часа от Москвы)'
    return context["data_interval_end"].in_timezone(LOCAL_TZ)


def b01_pick_branch_by_day_of_week (**context):

    weekday = context["data_interval_end"].weekday()
    # Заменить на:
    # dt = get_data_interval_end_msk(context)
    # weekday = dt.weekday()

    branches = {
        0: "t2_send_email_id",      # Понедельник
        1: "t2_send_email_id",      # Вторник
        2: "t2_send_email_id",      # Среда
        3: "t2_send_email_id",      # Четверг
        4: "t2_send_email_id",      # Пятница
        5: "e01_empty_id",          # Суббота
        6: "e01_empty_id",          # Воскресенье
    }

    return branches[weekday]


def b02_pick_branch_by_day_of_week (**context):

    weekday = context["data_interval_end"].weekday()
    # Заменить на:
    # dt = get_data_interval_end_msk(context)
    # weekday = dt.weekday()

    branches = {
        0: "t3_send_email_id",      # Понедельник
        1: "e02_empty_id",          # Вторник
        2: "e02_empty_id",          # Среда
        3: "e02_empty_id",          # Четверг
        4: "e02_empty_id",          # Пятница
        5: "e02_empty_id",          # Суббота
        6: "e02_empty_id",          # Воскресенье
    }

    return branches[weekday]


with DAG(
    dag_id="test_different_tasks_by_day_of_week",
    description="Тестовый DAG по выбору ветки по дню недели",
    start_date=pendulum.datetime(2026, 7, 13, tz=LOCAL_TZ),
    schedule="30 2 * * *",
    catchup=False,
    tags=["test"],
):

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

    t1_send_email = EmailOperator(
        task_id="t1_send_email_id",
        conn_id=SMTP_CONN_ID,
        to=E_MAIL,
        subject="Сегодня - {{data_interval_end.in_timezone('Europe/Moscow').strftime('%d.%m.%Y')}}. Задача-1",
        html_content="""
        <h2>mg: Сегодня -   {{
                                data_interval_end
                                    .in_timezone('Europe/Moscow')
                                    .strftime('%d.%m.%Y')
                            }}.  Задача-1 !</h2>
        """,
    )
    
    t2_send_email = EmailOperator(
        task_id="t2_send_email_id",
        conn_id=SMTP_CONN_ID,
        to=E_MAIL,
        subject="Сегодня - {{data_interval_end.in_timezone('Europe/Moscow').strftime('%d.%m.%Y')}}. Задача-2",
        html_content="""
        <h2>mg: Сегодня -   {{
                                data_interval_end
                                    .in_timezone('Europe/Moscow')
                                    .strftime('%d.%m.%Y')
                            }}.  Задача-2 !</h2>
        """,
    )

    t3_send_email = EmailOperator(
        task_id="t3_send_email_id",
        conn_id=SMTP_CONN_ID,
        to=E_MAIL,
        subject="Позавчера - {{data_interval_end.in_timezone('Europe/Moscow').subtract(days=2).strftime('%d.%m.%Y')}}. Задача-2",
        html_content="""
        <h2>mg: Позавчера - {{
                                data_interval_end
                                    .in_timezone('Europe/Moscow')
                                    .subtract(days=2)
                                    .strftime('%d.%m.%Y')
                            }}.  Задача-2 !</h2>
        """,
    )

    t4_send_email = EmailOperator(
        task_id="t4_send_email_id",
        conn_id=SMTP_CONN_ID,
        to=E_MAIL,
        subject="Вчера - {{data_interval_end.in_timezone('Europe/Moscow').subtract(days=1).strftime('%d.%m.%Y')}}. Задача-2",
        html_content="""
        <h2>mg: Вчера - {{
                            data_interval_end
                                .in_timezone('Europe/Moscow')
                                .subtract(days=1)
                                .strftime('%d.%m.%Y')
                        }}.  Задача-2 !</h2>
        """,
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
    t_join_branch = EmptyOperator(
        task_id="join_branch_id",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    
    t1_send_email >> b01_pick_branch
    b01_pick_branch >> [e01_empty, t2_send_email]
    t2_send_email >> b02_pick_branch
    b02_pick_branch >> [e02_empty, t3_send_email]
    t3_send_email >> t4_send_email
    [e01_empty, e02_empty, t4_send_email] >> t_join_branch
