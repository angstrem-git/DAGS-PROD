-- v02_086_insert_dim_packet_processed_batches.sql
-- {{ params.db1 }}: "db1": DB1
-- {{ params.db3 }}: "db3": DB3
WITH 
cte_dt_rasp AS 
(
	SELECT 
	 	batch_id_dttm										AS batch_id_dttm	
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id)								AS date_id
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-10-13')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}'
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
INSERT INTO {{ params.db3 }}.dim_packet_processed_batches
--INSERT INTO rasp3_v02.dim_packet_processed_batches
(
	batch_id_dttm,
	batch_id_str,
	date_id,
	datetime_id,
	doc_raspredelenie,
	doc_raspredelenie_guid_1C_uid,
	doc_raspredelenie_guid_1C_str,
	schema_name,
	table_id,
	table_name
)
SELECT 
	batch_id_dttm,
	batch_id_str,
	toDate(datetime_id),
	datetime_id,
	doc_raspredelenie,
	doc_raspredelenie_guid_1C_uid,
	doc_raspredelenie_guid_1C_str,
	schema_name,
	table_id,
	table_name
FROM
	--rasp1.packet_load_batch
	{{ params.db1 }}.packet_load_batch
WHERE
	batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)