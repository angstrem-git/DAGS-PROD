SELECT 
	lb.batch_id_dttm AS batch_id_dttm,
	toDate(lb.datetime_id) AS date_id
FROM 
	{p_db1}.packet_load_batch AS lb	
		LEFT ANTI JOIN {p_db3}.dim_packet_processed_batches AS pb
		ON lb.batch_id_dttm = pb.batch_id_dttm
ORDER BY 
	lb.batch_id_dttm
LIMIT 1
FORMAT JSON


 