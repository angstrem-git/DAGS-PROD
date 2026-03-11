INSERT INTO rasp3_v01.fct_deficit_order_rank 
(
	batch_id_dttm,
	batch_id_str,
	date_id,
	rank_id,
	total_order_id,
	date_total_order_id,
	doc_order_rn_number,
	doc_order_rn_datetime,
	order_roznica_guid_uid,
	unit_id, 
	unit_name,
	unit_guid_OPN_uid,
	total_sum,
	sort_rank
)
SELECT 
	drd.batch_id_dttm
	,ps.batch_id_str
	,ps.date_id
	,drd.rank_id
	,drd.total_order_id
	,cityHash64(ps.date_id, drd.total_order_id, ps.order_roznica_guid_uid) 	
	,ps.doc_order_rn_number
	,ps.doc_order_rn_datetime
	,ps.order_roznica_guid_uid
	,us.unit_id
	,ps.unit_name
	,ps.unit_guid_OPN_uid
	,drd.total_sum
	,drd.sort_rank	
FROM
	{{ params.db2 }}.deficit_order_rank AS drd
	ANY LEFT JOIN {{ params.db3 }}.fct_deficit_packet_order AS ps 
		ON drd.batch_id_dttm = ps.batch_id_dttm
		AND drd.total_order_id = ps.total_order_id
	LEFT JOIN rasp2.unit_sale AS us
		ON ps.unit_guid_OPN_uid = us.unit_guid_uid
WHERE 
	drd.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'