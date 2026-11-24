-- v02_041_insert_full_order.sql
-- {{ params.db1 }}: "db1": DB1
-- {{ params.db3 }}: "db3": DB3
-- {{ params.days_back }}: "days_back": 0, 1 или 2
WITH 
--	0 AS temp_days_back
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
		--,subtractDays(toDate(datetime_id), temp_days_back) 		AS current_date 		-- Текущий день (для которого обрабатываем данные)
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-09-28')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}'
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
INSERT INTO {{ params.db3 }}.full_order(
--INSERT INTO rasp3_v02.full_order(	
	--batch_id_dttm							
	--,batch_id_str
	--,create_dttm
	-- mg-07.11.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	key_pk_dec_date_id_full_order_id
	-- mg-07.11.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,date_id 
	--,order_roznica_guid_uid 
	,full_order_id
	,full_order_roznica_guid_uid 
	,full_order_roznica_number
	,full_order_roznica_datetime
	-- mg-28.09.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,full_unit_id
	,full_unit_guid_uid 
	,full_unit_name 
	-- mg-28.09.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,full_city_id
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
	,full_order_deficit_info
)
SELECT
-- DEFAULT	--batch_id_dttm							
-- DEFAULT	--,batch_id_str
-- DEFAULT	--now('Europe/Moscow')
	-- mg-07.11.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	toInt64(toYYYYMMDD(sf.date_id)) * 100_000_000_000 + toInt64(sf.full_order_id)	AS key_pk_dec_date_id_full_order_id
	-- mg-07.11.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,sf.date_id 
	--,order_roznica_guid_uid 
	,sf.full_order_id
	,argMin(sf.full_order_roznica_guid_uid, sf.full_order_roznica_datetime) 
	,argMin(sf.full_order_roznica_number, sf.full_order_roznica_datetime)
	,min(sf.full_order_roznica_datetime)
	-- mg-28.09.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,argMin(sf.unit_id, sf.full_order_roznica_datetime)
	,argMin(sf.unit_guid_uid, sf.full_order_roznica_datetime)
	,argMin(sf.unit_name, sf.full_order_roznica_datetime)
	-- mg-28.09.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,argMin(sf.city_id, sf.full_order_roznica_datetime)
	,argMin(sf.city_guid_uid, sf.full_order_roznica_datetime)
	,argMin(sf.city_name, sf.full_order_roznica_datetime)
	,max(sf.is_link_order) 
	,sum(sf.total_sum_total) 
	,min(sf.source_stage_id) 
	,argMin(sf.source_stage_name, sf.source_stage_id)
	,min(sf.order_stage_id) 
	,argMin(sf.order_stage_name, sf.order_stage_id) 
	,min(sf.order_sub_stage_id)
	,argMin(sf.order_sub_stage_name, sf.order_sub_stage_id) 
	,max(sf.norma_days_dostavki_iz_voronezha)
	,max(sf.has_std1) 
	,max(sf.is_deficit_std1) 	
	,sum(sf.total_sum_std1) 
	,max(sf.has_std2) 
	,max(sf.is_deficit_std2) 
	,sum(sf.total_sum_std2) 
	,max(sf.has_mip) 
	,max(sf.is_deficit_mip) 
	,sum(sf.total_sum_mip) 
	,max(sf.has_kich) 
	,max(sf.is_deficit_kich) 
	,sum(sf.total_sum_kich) 
	,max(sf.has_stor) 
	,max(sf.is_deficit_stor) 
	,sum(sf.total_sum_stor) 
	,max(sf.has_matr) 
	,max(sf.is_deficit_matr) 
	,sum(sf.total_sum_matr) 
	,max(sf.has_tech) 
	,max(sf.is_deficit_tech) 
	,sum(sf.total_sum_tech) 
	,max(sf.has_other) 
	,max(sf.is_deficit_other) 
	,sum(sf.total_sum_other) 
	,max(sf.has_serv) 
	,sum(sf.total_sum_serv) 
	,max(sf.has_total_products)
	,max(sf.has_total_deficit)
	-- mg-28.09.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,arrayStringConcat(
        arraySort(	
            groupArrayIf(	
                sf.self_order_deficit_info,
                sf.self_order_deficit_info != ''
            ) --OVER ( PARTITION BY order_guid_str )	
        ),	
        '\n'	
    ) 												
	-- mg-28.09.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
FROM 
	{{ params.db3 }}.self_order AS sf
	--rasp3_v02.self_order AS sf
WHERE
	sf.date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	AND sf.order_stage_id != 90		-- Чтобы не складывать открытые заказы с завершенными заказами!!!
GROUP BY  
	sf.date_id
	,sf.full_order_id

