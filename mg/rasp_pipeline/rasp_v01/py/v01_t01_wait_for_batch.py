# v01_t01_wait_for_batch.py
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth

def wait_for_batch(RELEASE_key, DAG_DIR_key, DB1_key, DB3_key, URL_key, USER_key, PASSWORD_key, **context):

	sql_path = DAG_DIR_key / f"sql/{RELEASE_key}_t01_find_batch.sql"
	with open(sql_path, encoding="utf-8") as f:
		sql = f.read()
	query_text = sql.format(p_db1=DB1_key, p_db3=DB3_key)

	query_encoded = urllib.parse.quote(query_text)
	full_url = f"{URL_key}/?database={DB1_key}&query={query_encoded}"

	r = requests.post(
		full_url,
		auth=HTTPBasicAuth(USER_key, PASSWORD_key),
		headers={"Content-Type": "text/plain"},
		timeout=60,
	)
	r.raise_for_status()

	d = r.json()["data"]
	if len(d) == 0:
		return False

	batch_id_dttm = d[0].get("batch_id_dttm")
	context["ti"].xcom_push(
		key="batch_id_dttm",
		value=batch_id_dttm
	)

	date_id = d[0].get("date_id")
	context["ti"].xcom_push(
		key="date_id",
		value=date_id
	)

	return True