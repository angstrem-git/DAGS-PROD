# v01_t02_check_sum.py
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth
from airflow.exceptions import AirflowException
import logging

def check_sum(RELEASE_key, DAG_DIR_key, DB1_key, URL_key, USER_key, PASSWORD_key, **context):

    batch_id_dttm = context["ti"].xcom_pull(
        task_ids="wait_for_batch",
        key="batch_id_dttm"
    )

    sql_path = DAG_DIR_key / f"sql/{RELEASE_key}_t02_check_sum.sql"
    with open(sql_path, encoding="utf-8-sig") as f:
        sql = f.read()

    query_text = sql.format(
        p_db1=DB1_key,
        p_batch_id_dttm=batch_id_dttm
    )

    query_encoded = urllib.parse.quote(query_text)
    full_url = f"{URL_key}/?database={DB1_key}&query={query_encoded}"

    r = requests.post(
        full_url,
        auth=HTTPBasicAuth(USER_key, PASSWORD_key),
        headers={"Content-Type": "text/plain"},
        timeout=60,
    )

    r.raise_for_status()

    d = r.json()["data"][0]["total_check"]

    logging.info(f"batch_id_dttm={batch_id_dttm}, diff_count={d}")

    if d != 0:
        raise AirflowException(
            f"MG: Не пройдена проверка контрольных значений. batch_id_dttm={batch_id_dttm}, Количество непройденных={d}"
        )

    return True
