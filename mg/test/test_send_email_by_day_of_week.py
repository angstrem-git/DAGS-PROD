import pendulum
from pendulum import datetime
from airflow.sdk import DAG			
from airflow.operators.empty import EmptyOperator			
from airflow.utils.trigger_rule import TriggerRule		
from airflow.providers.standard.operators.bash import BashOperator			    # Для Airflow v3
from airflow.providers.standard.operators.python import PythonOperator			# Для Airflow v3
from airflow.providers.standard.operators.python import BranchPythonOperator	# Для Airflow v3
from airflow.providers.smtp.operators.smtp import EmailOperator                 # Для Airflow v3


local_tz = pendulum.timezone("Europe/Moscow")
E_MAIL = "M.Grapenyuk@angstrem.net"

def pick_branch_by_day_of_week(**context):

    weekday = context["data_interval_end"].weekday()

    branches = {
        0: "send_monday_email",
        1: "send_tuesday_email",
        2: "send_wednesday_email",
        3: "send_thursday_email",
        4: "send_friday_email",
        5: "send_saturday_email",
        6: "send_sunday_email",
    }

    return branches[weekday]


with DAG(
    dag_id="test_send_email_by_day_of_week",
    description="Тестовый DAG по выбору ветки по дню недели",
    start_date=pendulum.datetime(2026, 7, 13, tz=local_tz),
    schedule="30 1 * * *",
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
    t_pick_branch = BranchPythonOperator(
        task_id="pick_branch_id",
        python_callable=pick_branch_by_day_of_week,
    )

    t_01_send_monday_email = EmailOperator(
        task_id="send_monday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня понедельник, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Понедельник !</h2>

        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    t_02_send_tuesday_email = EmailOperator(
        task_id="send_tuesday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня вторник, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Вторник !</h2>

        {% include 'test_by_day_of_week_header.html' %}

        {# Так писать комментарии в Jinja; #} 
        """,
    )

    t_03_send_wednesday_email = EmailOperator(
        task_id="send_wednesday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня среда, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Среда !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    t_04_send_thursday_email = EmailOperator(
        task_id="send_thursday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня четверг, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Четверг !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    t_05_send_friday_email = EmailOperator(
        task_id="send_friday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня пятница, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Пятница !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    t_06_send_saturday_email = EmailOperator(
        task_id="send_saturday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня суббота, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Суббота !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    t_07_send_sunday_email = EmailOperator(
        task_id="send_sunday_email",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня воскресенье, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Воскресенье !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
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
        task_id="join_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_pick_branch >> [
        t_01_send_monday_email, 
        t_02_send_tuesday_email, 
        t_03_send_wednesday_email, 
        t_04_send_thursday_email, 
        t_05_send_friday_email, 
        t_06_send_saturday_email, 
        t_07_send_sunday_email
    ] >> t_join_branch
