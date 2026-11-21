# v02_003_check_input_data.py
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def check_input_data(
    RELEASE_key,
    DAG_DIR_key,
    DATE_ID_key,
    **context
):
    """Возвращает признак наличия данных."""

    sql_path = DAG_DIR_key / f"sql/{RELEASE_key}_003_check_input_data.sql"

    with open(sql_path, encoding="utf-8-sig") as f:
        sql = f.read()

    query_text = sql.format(p_date_id=DATE_ID_key)

    hook = ClickHouseHook(clickhouse_conn_id="click_onpremise_airflow")

    result = hook.execute(query_text)
    
    cnt1, cnt2, cnt3 = result[0]

    return (cnt1 > 0) and (cnt2 > 0) and (cnt3 > 0)
