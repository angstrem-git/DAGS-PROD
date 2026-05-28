-- "db1_source": DB1_source
-- "db3": DB3
WITH 
cte_dt AS 
	(
	SELECT 
	 	batch_id_dttm			AS batch_id_dttm	
		,batch_id_str			AS batch_id_str
		,create_dttm			AS create_dttm
		,toDate(datetime_id)	AS date_id
	FROM {{ params.db1_source }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-05-15')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	ORDER BY create_dttm DESC
	LIMIT 1
	)
INSERT INTO {{ params.db3 }}.full_order(
	batch_id_dttm							
	,batch_id_str
	,create_dttm
	,date_id 
	--,order_roznica_guid_uid 
	,full_order_id
	,full_order_roznica_guid_uid 
	,full_order_roznica_number
	,full_order_roznica_datetime
	,full_unit_guid_uid 
	,full_unit_name 
	,full_city_guid_uid 
	,full_city_name 
	,is_link_order 
	,total_sum_total 
	,source_stage_id 
	,source_stage_name 
	,order_stage_id 
	,order_stage_name 
	,order_sub_stage_id 
	,order_sub_stage_name 
	,norma_days_dostavki_iz_voronezha
	,has_std1 
	,is_deficit_std1 	
	,total_sum_std1 
	,has_std2 
	,is_deficit_std2 
	,total_sum_std2 
	,has_mip 
	,is_deficit_mip 
	,total_sum_mip 
	,has_kich 
	,is_deficit_kich 
	,total_sum_kich 
	,has_stor 
	,is_deficit_stor 
	,total_sum_stor 
	,has_matr 
	,is_deficit_matr 
	,total_sum_matr 
	,has_tech 
	,is_deficit_tech 
	,total_sum_tech 
	,has_other 
	,is_deficit_other 
	,total_sum_other 
	,has_serv 
	,total_sum_serv 	
)
SELECT
	batch_id_dttm							
	,batch_id_str
	,now('Europe/Moscow')
	,date_id 
	--,order_roznica_guid_uid 
	,full_order_id
	,argMin(full_order_roznica_guid_uid, full_order_roznica_datetime) 
	,argMin(full_order_roznica_number, full_order_roznica_datetime)
	,min(full_order_roznica_datetime)
	,argMin(unit_guid_uid, full_order_roznica_datetime)
	,argMin(unit_name, full_order_roznica_datetime)
	,argMin(city_guid_uid,  full_order_roznica_datetime)
	,argMin(city_name, full_order_roznica_datetime)
	,max(is_link_order) 
	,sum(total_sum_total) 
	,min(source_stage_id) 
	,argMin(source_stage_name, source_stage_id)
	,min(order_stage_id) 
	,argMin(order_stage_name, order_stage_id) 
	,min(order_sub_stage_id)
	,argMin(order_sub_stage_name, order_sub_stage_id) 
	,max(norma_days_dostavki_iz_voronezha)
	,max(has_std1) 
	,max(is_deficit_std1) 	
	,sum(total_sum_std1) 
	,max(has_std2) 
	,max(is_deficit_std2) 
	,sum(total_sum_std2) 
	,max(has_mip) 
	,max(is_deficit_mip) 
	,sum(total_sum_mip) 
	,max(has_kich) 
	,max(is_deficit_kich) 
	,sum(total_sum_kich) 
	,max(has_stor) 
	,max(is_deficit_stor) 
	,sum(total_sum_stor) 
	,max(has_matr) 
	,max(is_deficit_matr) 
	,sum(total_sum_matr) 
	,max(has_tech) 
	,max(is_deficit_tech) 
	,sum(total_sum_tech) 
	,max(has_other) 
	,max(is_deficit_other) 
	,sum(total_sum_other) 
	,max(has_serv) 
	,sum(total_sum_serv) 	
FROM 
	{{ params.db3 }}.self_order
WHERE
	--date_id = toDate('2026-04-17')
	batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
GROUP BY  
	batch_id_dttm							
	,batch_id_str
	,date_id
	,full_order_id

