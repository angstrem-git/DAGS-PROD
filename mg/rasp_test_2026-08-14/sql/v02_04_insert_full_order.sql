-- "db1": DB1
-- "db3": DB3
-- "days_back": 0, 1 или 2
WITH 
--	0 AS days_back
--	--,toDate('2026-07-31') AS cte_current_snapshot_date
--, 
cte_dt_rasp AS 
(
	SELECT 
	 	batch_id_dttm										AS batch_id_dttm	
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id)								AS date_id
		-- days_back = 0 (Пн,Вт,Ср,Чт,Пт), days_back = 1 (Вс), days_back = 2 (Сб)
		,subtractDays(
			toDate(datetime_id),
			{{ params.days_back }} 
		) 													AS current_date 
		--,subtractDays(toDate(datetime_id), days_back) 		AS current_date 		-- Текущий день (для которого обрабатываем данные)
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-08-03')
	WHERE batch_id_str = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_str") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
--
INSERT INTO {{ params.db3 }}.full_order(
--INSERT INTO rasp3_v02.full_order(	
	--batch_id_dttm							
	--,batch_id_str
	--,create_dttm
	date_id 
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
	,has_total_products
	,has_total_deficit
)
SELECT
-- DEFAULT	--batch_id_dttm							
-- DEFAULT	--,batch_id_str
-- DEFAULT	--now('Europe/Moscow')
	date_id 
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
	,max(has_total_products)
	,max(has_total_deficit)
FROM 
	{{ params.db3 }}.self_order
	--rasp3_v02.self_order
WHERE
	date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	--date_id = cte_current_snapshot_date
	--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
	AND order_stage_id < 90
GROUP BY  
	date_id
	,full_order_id

