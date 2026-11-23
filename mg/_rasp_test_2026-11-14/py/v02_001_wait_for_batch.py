# v02_001_wait_for_batch.py
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth

def wait_for_batch(
		RELEASE_key, 
		DAG_DIR_key, 
		DB1_key, 
		DB3_key, 
		URL_key, 
		USER_key, 
		PASSWORD_key, 
		DATE_ID_key, 
		**context
):

	sql_path = DAG_DIR_key / f"sql/{RELEASE_key}_001_wait_for_batch.sql"
	with open(sql_path, encoding="utf-8-sig") as f:
		sql = f.read()
	query_text = sql.format(p_db1=DB1_key, p_db3=DB3_key, p_date_id=DATE_ID_key)

	query_encoded = urllib.parse.quote(query_text)
	full_url = f"{URL_key}/?database={DB1_key}&query={query_encoded}"

	response = requests.post(
		full_url,
		auth=HTTPBasicAuth(USER_key, PASSWORD_key),
		headers={"Content-Type": "text/plain"},
		timeout=60,
	)
	response.raise_for_status()

	data = response.json()["data"]
	if not data:
		return False

	batch_id_dttm = data[0]["batch_id_dttm"]
	context["ti"].xcom_push(
		key="batch_id_dttm",
		value=batch_id_dttm
	)

	batch_id_str = data[0]["batch_id_str"]
	context["ti"].xcom_push(
		key="batch_id_str",
		value=batch_id_str
	)

	date_id = data[0]["date_id"]
	context["ti"].xcom_push(
		key="date_id",
		value=date_id
	)

	return True