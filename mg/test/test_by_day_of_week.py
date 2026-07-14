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
        0: "branch_monday",
        1: "branch_tuesday",
        2: "branch_wednesday",
        3: "branch_thursday",
        4: "branch_friday",
        5: "branch_saturday",
        6: "branch_sunday",
    }

    return branches[weekday]


with DAG(
    dag_id="test_by_day_of_week",
    description="Тестовый DAG по выбору ветки по дню недели",
    start_date=pendulum.datetime(2026, 7, 13, tz=local_tz),
    schedule="0 8 * * *",
    catchup=False,
    tags=["test"]
):

    pick_branch = BranchPythonOperator(
        task_id="pick_branch_id",
        python_callable=pick_branch_by_day_of_week
    )

    branch_monday = EmailOperator(
        task_id="branch_monday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня понедельник, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Понедельник !</h2>

        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    branch_tuesday = EmailOperator(
        task_id="branch_tuesday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня вторник, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Вторник !</h2>

        {% include 'test_by_day_of_week_header.html' %}

        {# Так писать комментарии в Jinja; #} 
        """,
    )

    branch_wednesday = EmailOperator(
        task_id="branch_wednesday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня среда, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Среда !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    branch_thursday = EmailOperator(
        task_id="branch_thursday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня четверг, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Четверг !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    branch_friday = EmailOperator(
        task_id="branch_friday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня пятница, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Пятница !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    branch_saturday = EmailOperator(
        task_id="branch_saturday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня суббота, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Суббота !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )

    branch_sunday = EmailOperator(
        task_id="branch_sunday_id",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня воскресенье, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Воскресенье !</h2>
        
        {% include 'test_by_day_of_week_header.html' %}

        """,
    )
    
    join_branch = EmptyOperator(
        task_id="join_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    pick_branch >> [
        branch_monday, 
        branch_tuesday, 
        branch_wednesday, 
        branch_thursday, 
        branch_friday, 
        branch_saturday, 
        branch_sunday
    ] >> join_branch
