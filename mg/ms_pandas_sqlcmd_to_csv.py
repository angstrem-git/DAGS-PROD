from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import pandas as pd
import pyodbc

# Функция для извлечения данных и сохранения в CSV
def export_to_csv():
    # Подключение к MSSQL
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=OLAP,1433;"           # Host + порт
        "DATABASE=Angstrem;"          # Название базы
        "UID=sa;"                     # Логин
        "PWD=:lkj7Fdsa;"              # Пароль
        "TrustServerCertificate=yes;" # Для безопасного соединения
    )
    conn = pyodbc.connect(conn_str)
    
    # SQL-запрос
    query = """
        SELECT order_id, order_guid, order_number, phone
        FROM Angstrem.mgtest.phone
    """
    
    # Чтение в DataFrame
    df = pd.read_sql(query, conn)
    
    # Сохранение в CSV
    df.to_csv(
        r"C:\Users\M.Grapenyuk\Documents\mg\test\file_phone_unicode.csv",
        sep=';', index=False, encoding='utf-8'
    )

# Определяем DAG
dag = DAG(
    'mssql_export_csv',
    start_date=datetime(2025, 8, 25),
    schedule_interval=None,  # Однократный запуск
    catchup=False
)

# PythonOperator для выполнения функции
export_task = PythonOperator(
    task_id='export_phone_to_csv',
    python_callable=export_to_csv,
    dag=dag
)

export_task 