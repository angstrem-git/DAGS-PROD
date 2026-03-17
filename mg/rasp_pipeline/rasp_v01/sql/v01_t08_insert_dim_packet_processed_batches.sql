INSERT INTO {{ params.db3 }}.dim_packet_processed_batches
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
	rasp1.packet_load_batch
WHERE
	batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'