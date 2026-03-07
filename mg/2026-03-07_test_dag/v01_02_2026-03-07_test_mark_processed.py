import os
import sys
import requests
from requests.auth import HTTPBasicAuth

batch_id_dttm_p	= sys.argv[1]
db2_p			= os.getenv("CLICKHOUSE_DATABASE")
url_p			= os.getenv("CLICKHOUSE_URL")
user_p			= os.getenv("CLICKHOUSE_USER")				
password_p		= os.getenv("CLICKHOUSE_PASSWORD")	

query_text = f"""
	INSERT INTO {db2_p}.test1_processed
	(batch_id_dttm, status)
	VALUES ('{batch_id_dttm_p}', 1)
"""

r = requests.post(
	url_p,
	params  = {
		"database": db2_p,
		"query": query_text
	},
	auth    = HTTPBasicAuth(user_p, password_p),
	headers = {"Content-Type": "text/plain"},	
	timeout = 30,
)

r.raise_for_status()
