from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

with DAG(
    'mg-clickhouse_test',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['mg'],
) as dag:

    ch_test = ClickHouseOperator(
    	task_id='ch_test',
        database='default',
        sql=(
            '''
                INSERT INTO mg_test_airflow (dt_utc) VALUES ( now() );
            ''',
        ),
        clickhouse_conn_id='ch_airflow',
    )

    ch_test
