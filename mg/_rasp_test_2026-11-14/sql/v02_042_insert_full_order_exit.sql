-- v02_042_insert_full_order_exit.sql
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
		,subtractDays(
			toDate(datetime_id),
			{{ params.days_back }} + 1
		) 													AS previous_date		-- Вчерашний день
		--,subtractDays(toDate(datetime_id), temp_days_back + 1) 	AS previous_date		-- Вчерашний день
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-09-28')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
-- Открытые Полные заказы на ТЕКУЩУЮ дату
cte_full_order_current_all AS				-- 5216 строк (на '2026-09-28')
(
	SELECT DISTINCT
		fd.full_order_id			AS full_order_id
	FROM 
		{{ params.db3 }}.full_order AS fd
		--rasp3_v02.full_order AS fd
	WHERE
		fd.date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		AND fd.order_stage_id != 90	 	-- Не анализируем завершенные заказы!!! 
)
,
-- Открытые Полные заказы на ПРЕДЫДУЩУЮ дату
cte_full_order_previous_exit AS				-- 66 строк (на '2026-09-28' - INTERVAL 1 DAY)
(
	SELECT
		fd.date_id 																AS date_id
		,fd.full_order_id														AS full_order_id
		,argMax(fd.full_order_roznica_guid_uid			,fd.create_dttm) 		AS full_order_roznica_guid_uid
		,argMax(fd.full_order_roznica_number			,fd.create_dttm)		AS full_order_roznica_number
		,argMax(fd.full_order_roznica_datetime			,fd.create_dttm)		AS full_order_roznica_datetime
		,argMax(fd.full_unit_id							,fd.create_dttm)		AS full_unit_id
		,argMax(fd.full_unit_guid_uid					,fd.create_dttm)		AS full_unit_guid_uid
		,argMax(fd.full_unit_name						,fd.create_dttm)		AS full_unit_name
		,argMax(fd.full_city_id							,fd.create_dttm)		AS full_city_id
		,argMax(fd.full_city_guid_uid					,fd.create_dttm)		AS full_city_guid_uid
		,argMax(fd.full_city_name						,fd.create_dttm)		AS full_city_name
		,argMax(fd.is_link_order						,fd.create_dttm)		AS is_link_order 
		,argMax(fd.total_sum_total						,fd.create_dttm) 		AS total_sum_total
		,argMax(fd.source_stage_id						,fd.create_dttm) 		AS source_stage_id
		,argMax(fd.source_stage_name					,fd.create_dttm)		AS source_stage_name
		,argMax(fd.order_stage_id						,fd.create_dttm) 		AS order_stage_id
		,argMax(fd.order_stage_name						,fd.create_dttm) 		AS order_stage_name
		,argMax(fd.order_sub_stage_id					,fd.create_dttm)		AS order_sub_stage_id
		,argMax(fd.order_sub_stage_name					,fd.create_dttm) 		AS order_sub_stage_name
		,argMax(fd.norma_days_dostavki_iz_voronezha		,fd.create_dttm)		AS norma_days_dostavki_iz_voronezha
		,argMax(fd.has_std1								,fd.create_dttm) 		AS has_std1
		,argMax(fd.is_deficit_std1						,fd.create_dttm) 		AS is_deficit_std1	
		,argMax(fd.total_sum_std1						,fd.create_dttm) 		AS total_sum_std1
		,argMax(fd.has_std2								,fd.create_dttm) 		AS has_std2
		,argMax(fd.is_deficit_std2						,fd.create_dttm) 		AS is_deficit_std2
		,argMax(fd.total_sum_std2						,fd.create_dttm)		AS total_sum_std2 
		,argMax(fd.has_mip								,fd.create_dttm) 		AS has_mip
		,argMax(fd.is_deficit_mip						,fd.create_dttm) 		AS is_deficit_mip
		,argMax(fd.total_sum_mip						,fd.create_dttm) 		AS total_sum_mip
		,argMax(fd.has_kich								,fd.create_dttm) 		AS has_kich
		,argMax(fd.is_deficit_kich						,fd.create_dttm) 		AS is_deficit_kich
		,argMax(fd.total_sum_kich						,fd.create_dttm) 		AS total_sum_kich
		,argMax(fd.has_stor								,fd.create_dttm) 		AS has_stor
		,argMax(fd.is_deficit_stor						,fd.create_dttm) 		AS is_deficit_stor
		,argMax(fd.total_sum_stor						,fd.create_dttm) 		AS total_sum_stor
		,argMax(fd.has_matr								,fd.create_dttm) 		AS has_matr
		,argMax(fd.is_deficit_matr						,fd.create_dttm) 		AS is_deficit_matr
		,argMax(fd.total_sum_matr						,fd.create_dttm) 		AS total_sum_matr
		,argMax(fd.has_tech								,fd.create_dttm) 		AS has_tech
		,argMax(fd.is_deficit_tech						,fd.create_dttm)		AS is_deficit_tech 
		,argMax(fd.total_sum_tech						,fd.create_dttm) 		AS total_sum_tech
		,argMax(fd.has_other							,fd.create_dttm) 		AS has_other
		,argMax(fd.is_deficit_other						,fd.create_dttm) 		AS is_deficit_other
		,argMax(fd.total_sum_other						,fd.create_dttm)		AS total_sum_other 
		,argMax(fd.has_serv								,fd.create_dttm) 		AS has_serv
		,argMax(fd.total_sum_serv						,fd.create_dttm)		AS total_sum_serv 
		,argMax(fd.has_total_products					,fd.create_dttm)		AS has_total_products
		,argMax(fd.has_total_deficit					,fd.create_dttm)		AS has_total_deficit	
		,argMax(fd.full_order_deficit_info				,fd.create_dttm)  		AS full_order_deficit_info
	FROM 
		{{ params.db3 }}.full_order AS fd
		--rasp3_v02.full_order AS fd
	WHERE
		fd.date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		AND fd.order_stage_id != 90	 	-- Не анализируем завершенные заказы!!! 
		AND fd.full_order_id NOT IN (SELECT full_order_id FROM cte_full_order_current_all)
	GROUP BY  
		fd.date_id
		,fd.full_order_id	
)
,
-- Статусы заказов в ТЕКУЩУЮ дату
cte_self_order_current_all AS		-- 6 303 строк (на '2026-09-28')
(
	SELECT
		slf.order_id											AS self_order_id
		,slf.full_order_id										AS full_order_id
		,argMax(slf.order_sub_stage_id, slf.create_dttm)  		AS self_order_sub_stage_id
	FROM	
		{{ params.db3 }}.self_order AS slf
		--rasp3_v02.self_order AS slf
	WHERE
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		slf.order_id
		,slf.full_order_id
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
SELECT							-- 66 строк (на '2026-09-28' - INTERVAL 1 DAY)
	-- mg-07.11.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	toInt64(toYYYYMMDD(addDays(ex.date_id, 1))) * 100_000_000_000 + toInt64(ex.full_order_id)	AS key_pk_dec_date_id_full_order_id
	-- mg-07.11.2026 - Новое поле -----------------------------------------------------------------------------------------------------------
	,addDays(ex.date_id, 1)							AS date_id
	,ex.full_order_id								AS full_order_id		
	,any(ex.full_order_roznica_guid_uid) 			AS full_order_roznica_guid_uid
	,any(ex.full_order_roznica_number)				AS full_order_roznica_number
	,any(ex.full_order_roznica_datetime)			AS full_order_roznica_datetime
	,any(ex.full_unit_id)							AS full_unit_id
	,any(ex.full_unit_guid_uid)						AS full_unit_guid_uid
	,any(ex.full_unit_name)							AS full_unit_name
	,any(ex.full_city_id)							AS full_city_id
	,any(ex.full_city_guid_uid)						AS full_city_guid_uid
	,any(ex.full_city_name)							AS full_city_name
	,any(ex.is_link_order)							AS is_link_order 
	,any(ex.total_sum_total) 						AS total_sum_total
	,4 												AS source_stage_id  -- 4 - Завершение заказа (1С УТ)
	,''												AS source_stage_name
	,90												AS order_stage_id	-- 90 - Завершение заказа
	,'' 											AS order_stage_name
	,max(sf.self_order_sub_stage_id)				AS order_sub_stage_id
	,''												AS order_sub_stage_name
	,any(ex.norma_days_dostavki_iz_voronezha)		AS norma_days_dostavki_iz_voronezha
	,any(ex.has_std1) 								AS has_std1
	,any(ex.is_deficit_std1) 						AS is_deficit_std1	
	,any(ex.total_sum_std1) 						AS total_sum_std1
	,any(ex.has_std2) 								AS has_std2
	,any(ex.is_deficit_std2) 						AS is_deficit_std2
	,any(ex.total_sum_std2)							AS total_sum_std2 
	,any(ex.has_mip) 								AS has_mip
	,any(ex.is_deficit_mip) 						AS is_deficit_mip
	,any(ex.total_sum_mip) 							AS total_sum_mip
	,any(ex.has_kich) 								AS has_kich
	,any(ex.is_deficit_kich) 						AS is_deficit_kich
	,any(ex.total_sum_kich) 						AS total_sum_kich
	,any(ex.has_stor) 								AS has_stor
	,any(ex.is_deficit_stor) 						AS is_deficit_stor
	,any(ex.total_sum_stor) 						AS total_sum_stor
	,any(ex.has_matr) 								AS has_matr
	,any(ex.is_deficit_matr) 						AS is_deficit_matr
	,any(ex.total_sum_matr) 						AS total_sum_matr
	,any(ex.has_tech) 								AS has_tech
	,any(ex.is_deficit_tech)						AS is_deficit_tech 
	,any(ex.total_sum_tech) 						AS total_sum_tech
	,any(ex.has_other) 								AS has_other
	,any(ex.is_deficit_other) 						AS is_deficit_other
	,any(ex.total_sum_other)						AS total_sum_other 
	,any(ex.has_serv) 								AS has_serv
	,any(ex.total_sum_serv)							AS total_sum_serv 
	,any(ex.has_total_products)						AS has_total_products
	,any(ex.has_total_deficit)						AS has_total_deficit	
	,any(ex.full_order_deficit_info)  				AS full_order_deficit_info
FROM 
	cte_self_order_current_all AS sf
	RIGHT JOIN cte_full_order_previous_exit AS ex
		ON sf.full_order_id = ex.full_order_id
GROUP BY
	addDays(ex.date_id, 1)
	,ex.full_order_id

--SELECT * FROM cte_full_order_previous_exit
--SELECT * FROM cte_full_order_current_all