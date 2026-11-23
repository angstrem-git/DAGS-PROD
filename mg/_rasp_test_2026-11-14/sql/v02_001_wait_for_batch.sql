-- v02_003_check_input_data
SELECT 
	lb.batch_id_dttm AS batch_id_dttm,
	lb.batch_id_str AS batch_id_str,
	toDate(lb.datetime_id) AS date_id
FROM 
	{p_db1}.packet_load_batch AS lb	
		LEFT ANTI JOIN {p_db3}.dim_packet_processed_batches AS pb
		ON lb.batch_id_dttm = pb.batch_id_dttm
WHERE 
	--toDate(lb.datetime_id) = toDate(now('Europe/Moscow'))
	toDate(lb.datetime_id) = toDate('{p_date_id}')
ORDER BY 
	lb.batch_id_dttm
LIMIT 1
FORMAT JSON



 