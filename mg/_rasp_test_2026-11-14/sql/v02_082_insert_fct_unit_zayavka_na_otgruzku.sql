-- v02_082_insert_fct_unit_zayavka_na_otgruzku.sql
-- {{ params.db1 }}: "db1": DB1
-- {{ params.db3 }}: "db3": DB3
WITH 
-- Убрать после отладки -------------------------------------------------------------------------------------------------
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
,
-- Убрать после отладки -------------------------------------------------------------------------------------------------
unq AS
	(
	SELECT
		DISTINCT
		pkt.batch_id_dttm										AS batch_id_dttm
		,pkt.batch_id_str										AS batch_id_str
		,pkt.date_id											AS date_id
		--,toDate(pkt.datetime_id) 								AS date_id
		,pkt.unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_na_otgruzku_all_guid_OPN_uid
		,pkt.unit_zayavka_na_otgruzku_name 						AS unit_na_otgruzku_all_name
		,pkt.city_zayavka_na_otgruzku_guid_OPN_uid				AS city_na_otgruzku_all_guid_OPN_uid
		,pkt.city_zayavka_na_otgruzku_name						AS city_na_otgruzku_all_name
	FROM 
		--rasp1.packet AS pkt
		{{ params.db1 }}.packet AS pkt
		-- mg-20.03.2026 - Новый фильтр Розничных заказлв ---------------------------------------------------
		INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn
			ON pkt.unit_guid_OPN_uid = urn.unit_guid_uid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	WHERE
		pkt.unit_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.city_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)		
	UNION DISTINCT	
	SELECT
		DISTINCT
		pkt.batch_id_dttm										
		,pkt.batch_id_str	
		,pkt.date_id									
		--,toDate(pkt.datetime_id)
		,pkt.unit_otgruzki_guid_OPN_uid	
		,pkt.unit_otgruzki_name
		,if(
			pkt.unit_otgruzki_guid_OPN_uid = toUUID('df928c3a-ec8b-4051-9d9d-3c85d182bec5'),	-- Кострома Офис-Склад
        	toUUID('b6192540-98b1-11e0-856e-000423d2fac4'),										-- г. Кострома
         	pkt.city_otgruzki_guid_OPN_uid
		)								AS city_otgruzki_guid_OPN_uid
		--,city_otgruzki_guid_OPN_uid
		,if(
			pkt.unit_otgruzki_guid_OPN_uid = toUUID('df928c3a-ec8b-4051-9d9d-3c85d182bec5'),	-- Кострома Офис-Склад
        	'г. Кострома',																		-- г. Кострома
        	pkt.city_otgruzki_name
		)								AS city_otgruzki_name			
		--,city_otgruzki_name					
	FROM 
		--rasp1.packet AS pkt
		{{ params.db1 }}.packet AS pkt
		-- mg-20.03.2026 - Новый фильтр Розничных заказлв ---------------------------------------------------
		INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn
			ON pkt.unit_guid_OPN_uid = urn.unit_guid_uid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	WHERE
		pkt.unit_otgruzki_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
	)	
,
rd AS
	(
	SELECT 
		pkt.batch_id_dttm										AS batch_id_dttm
		,pkt.batch_id_str										AS batch_id_str
		,pkt.date_id											AS date_id
		,pkt.unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
		,pkt.unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
		,pkt.city_zayavka_na_otgruzku_guid_OPN_uid				AS city_zayavka_na_otgruzku_guid_OPN_uid
		,pkt.city_zayavka_na_otgruzku_name						AS city_zayavka_na_otgruzku_name
		,toDate(pkt.data_otgruzki_zayavka_na_otgruzku_datetime)	AS data_otgruzki_zayavka_na_otgruzku_date
		,pkt.doc_zayavka_na_otgruzku_rasp_guid_1C_uid			AS doc_zayavka_na_otgruzku_rasp_guid_1C_uid			
		,MIN(pkt.ves_itogo_zayavka_na_otgruzku)					AS ves_itogo_zayavka_na_otgruzku
		,MIN(pkt.obyom_itogo_zayavka_na_otgruzku)				AS obyom_itogo_zayavka_na_otgruzku
	FROM	
		--rasp1.packet AS pkt
		{{ params.db1 }}.packet AS pkt
		-- mg-20.03.2026 - Новый фильтр Розничных заказлв ---------------------------------------------------
		INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn		-- Включить, если не хотим, чтобы добавлялись рекламации!
			ON pkt.unit_guid_OPN_uid = urn.unit_guid_uid		-- Но тогда будут попадать отгрузки на Франчайзи, если у них указано "Ярославль Офис-склад" (ИП Волкова...)
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	WHERE
		pkt.unit_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.city_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		pkt.batch_id_dttm								
		,pkt.batch_id_str
		,pkt.date_id													
		,pkt.unit_zayavka_na_otgruzku_guid_OPN_uid		
		,pkt.unit_zayavka_na_otgruzku_name				
		,pkt.city_zayavka_na_otgruzku_guid_OPN_uid		
		,pkt.city_zayavka_na_otgruzku_name				
		,toDate(pkt.data_otgruzki_zayavka_na_otgruzku_datetime)
		,pkt.doc_zayavka_na_otgruzku_rasp_guid_1C_uid																
	) 
,
za AS
	(
	SELECT 
		batch_id_dttm										AS batch_id_dttm
		,batch_id_str										AS batch_id_str
		,date_id											AS date_id
		,unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
		,city_zayavka_na_otgruzku_guid_OPN_uid				AS city_zayavka_na_otgruzku_guid_OPN_uid
		,city_zayavka_na_otgruzku_name						AS city_zayavka_na_otgruzku_name
		,data_otgruzki_zayavka_na_otgruzku_date				AS data_otgruzki_zayavka_na_otgruzku_date
		,SUM(ves_itogo_zayavka_na_otgruzku)					AS ves_itogo_zayavka_na_otgruzku
		,SUM(obyom_itogo_zayavka_na_otgruzku)				AS obyom_itogo_zayavka_na_otgruzku
	FROM
		rd
	GROUP BY
		batch_id_dttm										
		,batch_id_str										
		,date_id													
		,unit_zayavka_na_otgruzku_guid_OPN_uid				
		,unit_zayavka_na_otgruzku_name										
		,city_zayavka_na_otgruzku_guid_OPN_uid				
		,city_zayavka_na_otgruzku_name	
		,data_otgruzki_zayavka_na_otgruzku_date
	)
,
cte_total AS 
(	
	SELECT 
		unq.batch_id_dttm										AS batch_id_dttm
		,unq.batch_id_str										AS batch_id_str
		,unq.date_id											AS date_id
		,unq.unit_na_otgruzku_all_guid_OPN_uid					AS unit_na_otgruzku_all_guid_OPN_uid
		,unq.unit_na_otgruzku_all_name							AS unit_na_otgruzku_all_name
		,unq.city_na_otgruzku_all_guid_OPN_uid					AS city_na_otgruzku_all_guid_OPN_uid
		,unq.city_na_otgruzku_all_name							AS city_na_otgruzku_all_name 
		,za.data_otgruzki_zayavka_na_otgruzku_date				AS data_otgruzki_zayavka_na_otgruzku_date
		,za.ves_itogo_zayavka_na_otgruzku						AS ves_itogo_zayavka_na_otgruzku
		,za.obyom_itogo_zayavka_na_otgruzku						AS obyom_itogo_zayavka_na_otgruzku
		,reinterpretAsInt64(		-- reinterpretAsInt64() меняет интерпретацию UInt64 на Int64, не меняя 64 бита.
			cityHash64(
				tuple(
					unq.date_id
					, unq.unit_na_otgruzku_all_guid_OPN_uid
					, unq.city_na_otgruzku_all_guid_OPN_uid
					, coalesce(za.data_otgruzki_zayavka_na_otgruzku_date, toDate('1970-01-01'))
				)
			) 
		)														AS date_unit_otgruzki_id
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
		,tr.truck_rejs_rn_sort_1								AS truck_rejs_rn_sort_1
		,tr.truck_rejs_rn_name_1								AS truck_rejs_rn_name_1
		,tr.truck_rejs_rn_sort_2								AS truck_rejs_rn_sort_2
		,tr.truck_rejs_rn_name_2								AS truck_rejs_rn_name_2
		,tr.truck_rejs_rn_sort_3								AS truck_rejs_rn_sort_3
		,tr.truck_rejs_rn_name_3								AS truck_rejs_rn_name_3
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	FROM
		unq
		LEFT JOIN za
			ON unq.batch_id_dttm = za.batch_id_dttm 
			AND unq.date_id = za.date_id
			AND unq.unit_na_otgruzku_all_guid_OPN_uid = za.unit_zayavka_na_otgruzku_guid_OPN_uid
			AND unq.city_na_otgruzku_all_guid_OPN_uid = za.city_zayavka_na_otgruzku_guid_OPN_uid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
		LEFT JOIN rasp2.truck_rejs_rn as tr 
			ON unq.city_na_otgruzku_all_guid_OPN_uid = tr.city_guid_OPN_uuid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
)
INSERT INTO {{ params.db3 }}.fct_unit_zayavka_na_otgruzku
--INSERT INTO rasp3_v02.fct_unit_zayavka_na_otgruzku
(
	batch_id_dttm 
	,batch_id_str 
	,date_id 
	,unit_zayavka_na_otgruzku_guid_OPN_uid 
	,unit_zayavka_na_otgruzku_name 
	,city_zayavka_na_otgruzku_guid_OPN_uid 
	,city_zayavka_na_otgruzku_name 
	,data_otgruzki_zayavka_na_otgruzku_date 
	,ves_itogo_zayavka_na_otgruzku 
	,obyom_itogo_zayavka_na_otgruzku 
	,date_unit_otgruzki_id 	
	-- mg-20.03.2026 ------------------------------------------------------------------------------------
	,truck_rejs_rn_sort_1
	,truck_rejs_rn_name_1
	,truck_rejs_rn_sort_2
	,truck_rejs_rn_name_2
	,truck_rejs_rn_sort_3
	,truck_rejs_rn_name_3
	-- mg-20.03.2026 ------------------------------------------------------------------------------------
)
SELECT 
	any(cte.batch_id_dttm) 								AS batch_id_dttm 
	,any(cte.batch_id_str) 								AS batch_id_str
	,any(cte.date_id) 									AS date_id
	,any(cte.unit_na_otgruzku_all_guid_OPN_uid) 		AS unit_na_otgruzku_all_guid_OPN_uid
	,any(cte.unit_na_otgruzku_all_name) 				AS unit_na_otgruzku_all_name
	,any(cte.city_na_otgruzku_all_guid_OPN_uid) 		AS city_na_otgruzku_all_guid_OPN_uid
	,any(cte.city_na_otgruzku_all_name) 				AS city_na_otgruzku_all_name
	,any(cte.data_otgruzki_zayavka_na_otgruzku_date) 	AS data_otgruzki_zayavka_na_otgruzku_date
	,any(cte.ves_itogo_zayavka_na_otgruzku) 			AS ves_itogo_zayavka_na_otgruzku
	,any(cte.obyom_itogo_zayavka_na_otgruzku) 			AS obyom_itogo_zayavka_na_otgruzku
	,cte.date_unit_otgruzki_id							AS date_unit_otgruzki_id
	,any(cte.truck_rejs_rn_sort_1) 						AS truck_rejs_rn_sort_1
	,any(cte.truck_rejs_rn_name_1) 						AS truck_rejs_rn_name_1
	,any(cte.truck_rejs_rn_sort_2) 						AS truck_rejs_rn_sort_2
	,any(cte.truck_rejs_rn_name_2) 						AS truck_rejs_rn_name_2
	,any(cte.truck_rejs_rn_sort_3) 						AS truck_rejs_rn_sort_3
	,any(cte.truck_rejs_rn_name_3) 						AS truck_rejs_rn_name_3
FROM 
	cte_total AS cte
GROUP BY 
	cte.date_unit_otgruzki_id