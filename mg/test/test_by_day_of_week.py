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
        task_id="pick_branch",
        python_callable=pick_branch_by_day_of_week
    )

    branch_monday = EmailOperator(
        task_id="branch_monday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня понедельник",
        html_content="<h2>mg: Сегодня Понедельник !</h2>",
    )

    branch_tuesday = EmailOperator(
        task_id="branch_tuesday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня вторник, {{ds}}",
        html_content="""
        <h2>mg: Сегодня Вторник !</h2>

        <p><b>Дата выполнения:</b>
            {{ data_interval_end.strftime('%d.%m.%Y') }}</p>

        <p><b>Время выполнения:</b>
            {{ data_interval_end.strftime('%H:%M:%S') }}</p>    

        <p><b>data_interval_start :</b>
            {{ data_interval_start }}: {{ data_interval_start.__class__.__name__ }}</p>  
        
        <p><b>data_interval_end :</b>
            {{ data_interval_end }}</p> 

        <p><b>ds :</b>
            {{ ds }}</p>      
        
        <p><b>logical_date :</b>
            {{ logical_date }}</p>  
        
        <p><b>ts :</b>
            {{ ts }}</p> 
        
        <p><b>run_id :</b>
            {{ run_id }}</p> 
        
        <p><b>dag.dag_id :</b>
            {{ dag.dag_id }}</p> 

        <p><b>dag_run.run_type :</b>
            {{ dag_run.run_type }}</p> 
            
        <p><b>task.task_id :</b>
            {{ task.task_id }}</p> 
        
        <p><b>ti.try_number (Task Instance) :</b>
            {{ ti.try_number }}</p> 
        
        <p><b>task.owner :</b>
            {{ task.owner }}</p>   

        <p><b>ti.hostname :</b>
            {{ ti.hostname }}</p>

        <p><b>ti.map_index :</b>
            {{ ti.map_index }}</p>       
        
        <p><b>macros.datetime.now() :</b>
            {{ macros.datetime.now() }}</p> 
        
        <p><b>var.key.dwh_connection_string :</b>
            {{ var.key.dwh_connection_string }}</p> 
        
        <p><b>conn.smtp_angstrem.host :</b>
            {{ conn.smtp_angstrem.host }}</p> 

        """,
    )

    branch_wednesday = EmailOperator(
        task_id="branch_wednesday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня среда",
        html_content="<h2>mg: Сегодня Среда !</h2>",
    )

    branch_thursday = EmailOperator(
        task_id="branch_thursday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня четверг",
        html_content="<h2>mg: Сегодня Четверг !</h2>",
    )

    branch_friday = EmailOperator(
        task_id="branch_friday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня пятница",
        html_content="<h2>mg: Сегодня Пятница !</h2>",
    )

    branch_saturday = EmailOperator(
        task_id="branch_saturday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня суббота",
        html_content="<h2>mg: Сегодня Суббота !</h2>",
    )

    branch_sunday = EmailOperator(
        task_id="branch_sunday",
        conn_id="smtp_angstrem",
        to=E_MAIL,
        subject="Сегодня воскресенье",
        html_content="<h2>mg: Сегодня Воскресенье !</h2>",
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
