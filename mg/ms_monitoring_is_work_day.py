import pendulum

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


LOCAL_TZ = pendulum.timezone("Europe/Moscow")

E_MAIL = "M.Grapenyuk@angstrem.net"
SMTP_CONN_ID = "smtp_angstrem"
MSSQL_CONN_ID = "mssql_olap_main"


def check_calendar(**context):
    result = context["ti"].xcom_pull(task_ids="t_check_calendar")

    if result and result[0][0] is not None:
        return "t_send_email"

    return "t_no_reminder"


with DAG(
    dag_id="ms_monitoring_is_work_day",
    description="Напоминание о незаполненных рабочих днях календаря",
    start_date=pendulum.datetime(2026, 10, 16, tz=LOCAL_TZ),
    schedule="0 9 * * 5",  # Каждую пятницу в 09:00
    catchup=False,
    tags=["mg", "calendar"],
) as dag:

    t_check_calendar = SQLExecuteQueryOperator(
        task_id="t_check_calendar",
        conn_id=MSSQL_CONN_ID,
        sql="""
            SELECT MIN(date_id) AS min_date_id
            FROM [for_click].[vw_calendar]
            WHERE is_work_day IS NULL;
        """,
        # Airflow сохраняет возвращённый результат в XCom как список кортежей (list из tuple):
        do_xcom_push=True, 
    )

    t_branch = BranchPythonOperator(
        task_id="t_branch",
        python_callable=check_calendar,
    )

    t_send_email = EmailOperator(
        task_id="t_send_email",
        conn_id=SMTP_CONN_ID,
        to=E_MAIL,
        subject="Напоминание: не заполнены рабочие дни календаря",
        html_content="""
        <h2>Напоминание о производственном календаре</h2>

        <p>
            В таблице <b>[mg2].[calendar]</b>
            не заполнен признак рабочего дня
            для некоторых дат.
        </p>

        <p>
            Первая незаполненная дата:
            <b>{{ ti.xcom_pull(task_ids='t_check_calendar')[0][0]
                .strftime('%d.%m.%Y') }}</b>
        </p>

        <p>Необходимо заполнить поле <b>is_work_day</b>,
        начиная с указанной даты.</p>
        """,
    )

    t_no_reminder = EmptyOperator(
        task_id="t_no_reminder",
    )

    t_check_calendar >> t_branch
    t_branch >> [t_send_email, t_no_reminder]
