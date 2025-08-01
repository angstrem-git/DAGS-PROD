import textwrap
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

with DAG(
    "etl_test_dag",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # Run Extract
    t1=SSHOperator(
        task_id="ssh_run_extract_script",
        ssh_conn_id='ssh-airflowetl',
        #command='$HOME/.local/bin ;poetry --version'
        command='cd /home/***etl/AIRFLOW-ETL-MACHINE/gr_test_dag/ ; poetry run python /home/airflowetl/AIRFLOW-ETL-MACHINE/gr_test_dag/gr_test_dag_extract.py \'{{ ds }}\' {{ run_id }}'
    )
    # Run Transform
    t2=SSHOperator(
        task_id="ssh_run_transform_script",
        ssh_conn_id='ssh-airflowetl',
        command='cd /home/***etl/AIRFLOW-ETL-MACHINE/gr_test_dag/ ; poetry run python /home/airflowetl/AIRFLOW-ETL-MACHINE/gr_test_dag/gr_test_dag_transform.py {{ run_id }}'
    )
    # Run Load
    t3=SSHOperator(
        task_id="ssh_run_load_script",
        ssh_conn_id='ssh-airflowetl',
        command='cd /home/***etl/AIRFLOW-ETL-MACHINE/gr_test_dag/ ; poetry run python /home/airflowetl/AIRFLOW-ETL-MACHINE/gr_test_dag/gr_test_dag_load.py {{ run_id }}'
    )

t1>>t2>>t3