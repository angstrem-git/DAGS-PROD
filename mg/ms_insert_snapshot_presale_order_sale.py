from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

with DAG(
    dag_id="ms_insert_snapshot_presale_order_sale",
    description="Заполнение таблиц [mgtest].[presale_snapshot], [mgtest].[order_snapshot], [mgtest].[sale_snapshot]",
    schedule="@daily",	# каждый день в 0:00 утра
    start_date=datetime(2025, 8, 26, 0, 0, 0, 0, tz='Europe/Moscow'),
    catchup=True,
    tags=['mg'],
) as dag:

    t_presale = SQLExecuteQueryOperator(
    	task_id='t_presale',
    	conn_id='mssql_olap_main',
    	sql=r"""
		INSERT INTO [Angstrem].[mgtest].[presale_snapshot](
			[date_id],
			[presale_id],
			[presale_doc_num],
			[presale_doc_date],
			[presale_comment],
			[presale_phone],
			[presale_status_id],
			[presale_sum],
			[client_id],
			[employee_id],
			[unit_id],
			[presale_guid],
			[presale_base_1C],
			[presale_is_deleted],
			[presale_create_date],
			[presale_modify_date],
			[presale_guid_1C],
			[Order_id],
			[Presale_client_name],
			[presale_reasons_failure_presale_id],
			[presale_client_type_presale_id]
		) 
		SELECT 
			CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -1) AS DATE),
			[presale_id],
			[presale_doc_num],
			[presale_doc_date],
			[presale_comment],
			[presale_phone],
			[presale_status_id],
			[presale_sum],
			[client_id],
			[employee_id],
			[unit_id],
			[presale_guid],
			[presale_base_1C],
			[presale_is_deleted],
			[presale_create_date],
			[presale_modify_date],
			[presale_guid_1C],
			[Order_id],
			[Presale_client_name],
			[presale_reasons_failure_presale_id],
			[presale_client_type_presale_id]
		FROM   
			[Angstrem].[core].[presale]
		WHERE
			[presale_modify_date] >= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -1)
			AND [presale_modify_date] < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
	"""
    )

    t_order = SQLExecuteQueryOperator(
    	task_id='t_order',
    	conn_id='mssql_olap_main',
    	sql=r"""
		INSERT INTO [Angstrem].[mgtest].[order_snapshot](
			[date_id],
			[order_id],
			[order_doc_date],
			[order_doc_num],
			[order_total_sum],
			[unit_id],
			[order_guid],
			[order_base_1C],
			[order_is_deleted],
			[order_create_date],
			[order_modify_date],
			[order_guid_1C],
			[operation_type_id],
			[order_goods_count],
			[order_closed_order_id] 	
		) 
		SELECT 
			CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -1) AS DATE),
			[order_id],
			[order_doc_date],
			[order_doc_num],
			[order_total_sum],
			[unit_id],
			[order_guid],
			[order_base_1C],
			[order_is_deleted],
			[order_create_date],
			[order_modify_date],
			[order_guid_1C],
			[operation_type_id],
			[order_goods_count],
			[order_closed_order_id]
		FROM 
			[Angstrem].[core].[order] 
		WHERE
			[order_modify_date] >= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -1)
			AND [order_modify_date] < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
	"""
    )

    t_sale = SQLExecuteQueryOperator(
    	task_id='t_sale',
    	conn_id='mssql_olap_main',
    	sql=r"""
		INSERT INTO [Angstrem].[mgtest].[sale_snapshot](
			[date_id],
			[sale_id],
			[sale_doc_date],
			[sale_doc_num],
			[sale_first_sale],
			[sale_total_sum],
			[client_id],
			[unit_id],
			[sale_guid],
			[sale_base_1C],
			[sale_is_deleted],
			[sale_create_date],
			[sale_modify_date],
			[sale_guid_1C],
			[operation_type_id],
			[sale_goods_count]
		)
		SELECT 
			CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -1) AS DATE),
			[sale_id],
			[sale_doc_date],
			[sale_doc_num],
			[sale_first_sale],
			[sale_total_sum],
			[client_id],
			[unit_id],
			[sale_guid],
			[sale_base_1C],
			[sale_is_deleted],
			[sale_create_date],
			[sale_modify_date],
			[sale_guid_1C],
			[operation_type_id],
			[sale_goods_count]
		FROM 
			[Angstrem].[core].[sale] 
		WHERE
			[sale_modify_date] >= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -1)
			AND [sale_modify_date] < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
	"""
    )

t_presale >> t_order >> t_sale 