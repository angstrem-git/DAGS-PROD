-- v02_080_insert_fct_deficit_packet_order.sql
-- {{ params.db1 }}: "db1": DB1
-- {{ params.db3 }}: "db3": DB3
WITH 
cte_dt AS 
(
	SELECT 
	 	batch_id_dttm							AS batch_id_dttm	
		,batch_id_str							AS batch_id_str
		,toDate(datetime_id)					AS date_id
		,datetime_id							AS datetime_id
	--FROM rasp1.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-10-13')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
-- Все Полные заказы с дефицитом только Стандарт-1 и/или Стандарт-2
cte_full AS 				-- 2_064 строк (на 2026-10-13)
(
	SELECT
		fu.date_id									AS date_id
		,fu.full_order_id							AS full_order_id
		,if(min(fu.is_deficit_std1) = 1, 1, 0)		AS is_deficit_std1
		,if(min(fu.is_deficit_std2) = 1, 1, 0)		AS is_deficit_std2
		,if(
			min(fu.is_deficit_std1) = 1
			AND min(fu.is_deficit_std2) = 1,
			1,
			0
		)											AS is_deficit_std1_and_std2
		,if(
			min(fu.is_deficit_std1) = 1
			AND min(fu.is_deficit_std2) = 0,
			1,
			0
		)											AS is_deficit_std1_only
		,if(
			min(fu.is_deficit_std1) = 0
			AND min(fu.is_deficit_std2) = 1,
			1,
			0
		)											AS is_deficit_std2_only
	FROM 
		--dev3_v02.full_order AS fu
		{{ params.db3 }}.full_order AS fu
	WHERE
		fu.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)	-- Текущее распределение
		AND fu.order_stage_id = 50 										-- Нет продукции
		AND 
		(																-- Дефицит только по Стандарт-1 и/или Стандарт-2
			(fu.is_deficit_std1 = 1 OR fu.is_deficit_std2 = 1)
			AND fu.is_deficit_mip = 0
			AND fu.is_deficit_kich = 0
			AND fu.is_deficit_stor = 0
			AND fu.is_deficit_matr = 0
			AND fu.is_deficit_tech = 0
			AND fu.is_deficit_other = 0
		)
	GROUP BY 
		fu.date_id
		,fu.full_order_id
)
,
-- Все Self-Заказы за дату
cte_self AS 				-- 6_511 строк (на 2026-10-13)
(
	SELECT 
		sf.date_id							AS date_id
		,sf.order_id						AS self_order_id
		,sf.full_order_id					AS full_order_id
		,any(sf.order_roznica_guid_uid)		AS order_roznica_guid_uid
		,any(sf.order_roznica_doc_num)		AS order_roznica_doc_num
		,any(sf.order_roznica_doc_datetime)	AS order_roznica_doc_datetime
		,any(sf.unit_name)					AS unit_name
		,any(sf.city_name)					AS city_name
		,min(sf.total_sum_std1) 
			+ min(sf.total_sum_std2) 
			+ min(sf.total_sum_mip) 
			+ min(sf.total_sum_kich) 
			+ min(sf.total_sum_stor) 
			+ min(sf.total_sum_matr) 
			+ min(sf.total_sum_tech) 
			+ min(sf.total_sum_other) 
			+ min(sf.total_sum_serv) 		AS total_sum_culculate
	FROM 	
		--dev3_v02.self_order AS sf  
		{{ params.db3 }}.self_order AS sf  
	WHERE 
		sf.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)	-- Текущее распределение
	GROUP BY 
		sf.date_id
		,sf.order_id
		,sf.full_order_id
)
,
-- Все Self-Заказы с дефицитом только Стандарт-1 и/или Стандарт-2
cte_rd AS			-- 2_361 строк (на 2026-10-13)
(
	SELECT
		sf.date_id							AS date_id
		,sf.self_order_id					AS self_order_id
		,sf.full_order_id					AS full_order_id
		,min(fu.is_deficit_std1)			AS is_deficit_std1
		,min(fu.is_deficit_std2)			AS is_deficit_std2
		,min(fu.is_deficit_std1_and_std2)	AS is_deficit_std1_and_std2
		,min(fu.is_deficit_std1_only)		AS is_deficit_std1_only
		,min(fu.is_deficit_std2_only)		AS is_deficit_std2_only
		,any(sf.order_roznica_guid_uid)		AS order_roznica_guid_uid
		,any(sf.order_roznica_doc_num)		AS order_roznica_doc_num
		,any(sf.order_roznica_doc_datetime)	AS order_roznica_doc_datetime
		,any(sf.unit_name)					AS unit_name
		,any(sf.city_name)					AS city_name
		,min(sf.total_sum_culculate)		AS total_sum_culculate
	FROM
		cte_self 						AS sf 	-- 6 511 строк (на 2026-10-13)
		INNER JOIN cte_full				AS fu 	-- 2 064 строк (на 2026-10-13)
			ON sf.date_id = fu.date_id
			AND sf.full_order_id = fu.full_order_id
	GROUP BY 
		sf.date_id
		,sf.self_order_id
		,sf.full_order_id
)
,
-- Все дефицитные пакеты за дату
cte_packets AS 				-- 38_434 строк (на 2026-10-13)
(
	SELECT 
		pkt.batch_id_dttm							AS batch_id_dttm
		,pkt.batch_id_str							AS batch_id_str
		,pkt.date_id								AS date_id
		,pkt.order_roznica_guid_str 				AS order_roznica_guid_str
		,toUUIDOrZero(pkt.order_roznica_guid_str) 	AS order_roznica_guid_uid
		,pkt.doc_order_rn							AS doc_order_rn
		,pkt.doc_order_rn_guid_1C_uid				AS doc_order_rn_guid_1C_uid
		,pkt.doc_order_rn_guid_1C_str				AS doc_order_rn_guid_1C_str
		,pkt.doc_order_rn_number					AS doc_order_rn_number
		,pkt.doc_order_rn_datetime					AS doc_order_rn_datetime
		,pkt.nomenclature_name						AS nomenclature_name
		,pkt.nomenclature_guid_OPN_uid				AS nomenclature_guid_OPN_uid
		,pkt.nomenclature_guid_OPN_str				AS nomenclature_guid_OPN_str
		,pkt.nomenclature_property_name				AS nomenclature_property_name
		,pkt.nomenclature_property_guid_OPN_uid		AS nomenclature_property_guid_OPN_uid
		,pkt.nomenclature_property_guid_OPN_str		AS nomenclature_property_guid_OPN_str
		,pkt.packet_name							AS packet_name
		,pkt.packet_guid_OPN_uid					AS packet_guid_OPN_uid
		,pkt.packet_guid_OPN_str					AS packet_guid_OPN_str
		,pkt.packet_property_name					AS packet_property_name
		,pkt.packet_property_guid_OPN_uid			AS packet_property_guid_OPN_uid
		,pkt.packet_property_guid_OPN_str			AS packet_property_guid_OPN_str
		,pkt.unit_name								AS unit_name
		,pkt.unit_guid_OPN_uid						AS unit_guid_OPN_uid
		,pkt.kolichestvo_dolg						AS kolichestvo_dolg
		,pkt.tip_zakaza_rasp_str					AS tip_zakaza_rasp_str
		,pkt.is_ispolzuemaya_today					AS is_ispolzuemaya_today
		,pkt.status_modul_today_short_name			AS status_modul_today_short_name
		,pkt.status_packet_today_short_name			AS status_packet_today_short_name
		,pkt.is_ispolzuemaya_create_date			AS is_ispolzuemaya_create_date
		,pkt.status_modul_create_date_short_name	AS status_modul_create_date_short_name
		,pkt.status_packet_create_date_short_name	AS status_packet_create_date_short_name
		,pkt.doc_razmeshhenie_rasp_str				AS doc_razmeshhenie_rasp_str
		,pkt.doc_razmeshhenie_rasp_number			AS doc_razmeshhenie_rasp_number
		,pkt.doc_razmeshhenie_rasp_data				AS doc_razmeshhenie_rasp_data
		,pkt.unit_proizvodstva_name					AS unit_proizvodstva_name
		,pkt.smena_name								AS smena_name
		,pkt.is_iz_gorizonta						AS is_iz_gorizonta
		,pkt.data_vyhoda_iz_proizvodstva			AS data_vyhoda_iz_proizvodstva
		,pkt.data_zadaniya							AS data_zadaniya
	FROM
		--rasp1.packet AS pkt
		{{ params.db1 }}.packet AS pkt
	WHERE
		pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)	-- Текущее распределение
		AND (
			pkt.is_iz_proizvodstva_rasp = 1 
			OR pkt.is_s_uchetom_vypuska_rasp = 1		
		)											-- Дефицит
)
,
-- Артикулы Стандартной продукции
cte_art AS				-- 39_721 строк (на 2026-10-13)
(
	SELECT 
		ar.nomenclature_guid_uuid								AS nomenclature_guid_uuid
		,ar.nomenclature_property_guid_uuid						AS nomenclature_property_guid_uuid
		,argMax(ar.art_id, ar.create_dttm)						AS art_id
		,argMax(ar.nomenclature_id, ar.create_dttm)				AS nomenclature_id
		,argMax(ar.nomenclature_property_id, ar.create_dttm)	AS nomenclature_property_id
	FROM 
		rasp2.art AS ar
	WHERE
		ar.type_id IN (1, 17)									-- 1 - СТАНДАРТ, 17 - Стандарт-2
	GROUP BY
		ar.nomenclature_guid_uuid
		,ar.nomenclature_property_guid_uuid
)
,
-- Дефицит пакетов в Полных заказах
cte_def AS 				-- 14_066 строк (на 2026-10-13)
(
	SELECT
		rd.date_id 										AS date_id
		,rd.full_order_id								AS full_order_id	
		,pk.packet_guid_OPN_uid							AS packet_guid_OPN_uid
		,pk.packet_property_guid_OPN_uid				AS packet_property_guid_OPN_uid	
		,any(pk.batch_id_dttm)							AS batch_id_dttm
		,any(pk.batch_id_str)							AS batch_id_str
		,any(pk.nomenclature_name)						AS nomenclature_name
		,any(pk.nomenclature_guid_OPN_uid)				AS nomenclature_guid_OPN_uid
		,any(pk.nomenclature_guid_OPN_str)				AS nomenclature_guid_OPN_str
		,any(pk.nomenclature_property_name)				AS nomenclature_property_name
		,any(pk.nomenclature_property_guid_OPN_uid)		AS nomenclature_property_guid_OPN_uid
		,any(pk.nomenclature_property_guid_OPN_str)		AS nomenclature_property_guid_OPN_str
		,any(pk.packet_name)							AS packet_name
		,any(pk.packet_guid_OPN_str)					AS packet_guid_OPN_str
		,any(pk.packet_property_name)					AS packet_property_name
		,any(pk.packet_property_guid_OPN_str)			AS packet_property_guid_OPN_str
		,any(pk.unit_name)								AS unit_name
		,any(pk.unit_guid_OPN_uid)						AS unit_guid_OPN_uid
		,sum(pk.kolichestvo_dolg)						AS kolichestvo_dolg
		,any(pk.tip_zakaza_rasp_str)					AS tip_zakaza_rasp_str
		,any(pk.is_ispolzuemaya_today)					AS is_ispolzuemaya_today
		,any(pk.status_modul_today_short_name)			AS status_modul_today_short_name
		,any(pk.status_packet_today_short_name)			AS status_packet_today_short_name
		,any(pk.is_ispolzuemaya_create_date)			AS is_ispolzuemaya_create_date
		,any(pk.status_modul_create_date_short_name)	AS status_modul_create_date_short_name
		,any(pk.status_packet_create_date_short_name)	AS status_packet_create_date_short_name
		,any(pk.doc_razmeshhenie_rasp_str)				AS doc_razmeshhenie_rasp_str
		,any(pk.doc_razmeshhenie_rasp_number)			AS doc_razmeshhenie_rasp_number
		,any(pk.doc_razmeshhenie_rasp_data)				AS doc_razmeshhenie_rasp_data
		,any(pk.unit_proizvodstva_name)					AS unit_proizvodstva_name
		,any(pk.smena_name)								AS smena_name
		,any(pk.is_iz_gorizonta)						AS is_iz_gorizonta
		,any(pk.data_vyhoda_iz_proizvodstva)			AS data_vyhoda_iz_proizvodstva
		,any(pk.data_zadaniya)							AS data_zadaniya	
		,any(art_nom.art_id)							AS product_art_id
		,any(art_pack.art_id)							AS packet_art_id
	FROM
		cte_packets 							AS pk			-- 38 434 строк (на 2026-10-13)
		INNER JOIN cte_rd 						AS rd			-- 2 361 строк (на 2026-10-13)
			ON pk.date_id = rd.date_id
			AND pk.order_roznica_guid_uid = rd.order_roznica_guid_uid
		INNER JOIN cte_art 						AS art_nom		-- 39 721 строк (на 2026-10-13) 
			ON pk.nomenclature_guid_OPN_uid = art_nom.nomenclature_guid_uuid
			AND pk.nomenclature_property_guid_OPN_uid = art_nom.nomenclature_property_guid_uuid
		INNER JOIN rasp2.art 					AS art_pack 	-- 39 721 строк (на 2026-10-13)
			ON pk.packet_guid_OPN_uid = art_pack.nomenclature_guid_uuid
			AND pk.packet_property_guid_OPN_uid = art_pack.nomenclature_property_guid_uuid
	GROUP BY
		rd.date_id 
		,rd.full_order_id		
		,pk.packet_guid_OPN_uid
		,pk.packet_property_guid_OPN_uid
)
--INSERT INTO rasp3_v02.fct_deficit_packet_order
INSERT INTO {{ params.db3 }}.fct_deficit_packet_order
(
	batch_id_dttm,
	batch_id_str, 
	date_id,
	order_roznica_guid_uid,
	doc_order_rn_number,
	doc_order_rn_datetime,
	total_order_id,
	date_total_order_id,
	unit_name,								
	unit_guid_OPN_uid,
	product_name,
	product_guid_OPN_uid,
	product_property_name,
	product_property_guid_OPN_uid,
	total_product_art_id,
	date_total_product_art_id,
	packet_name,
	packet_guid_OPN_uid,
	packet_property_name,
	packet_property_guid_OPN_uid,
	total_packet_art_id,
	date_total_packet_art_id,
	kolichestvo_dolg, 
	order_total_sum,
	is_ispolzuemaya_today,
	status_modul_today_short_name,
	status_packet_today_short_name,
	is_ispolzuemaya_create_date,
	status_modul_create_date_short_name,
	status_packet_create_date_short_name,
	doc_razmeshhenie_rasp_str,
	doc_razmeshhenie_rasp_number,
	doc_razmeshhenie_rasp_data,
	unit_proizvodstva_name,
	smena_name,
	is_iz_gorizonta,
	data_vyhoda_iz_proizvodstva,
	data_zadaniya
)
SELECT					-- 16_642 строк (на 2026-10-13)
	df.batch_id_dttm														AS batch_id_dttm
	,df.batch_id_str														AS batch_id_str
	,df.date_id																AS date_id
	,rd.order_roznica_guid_uid												AS order_roznica_guid_uid
	,rd.order_roznica_doc_num												AS doc_order_rn_number
	,rd.order_roznica_doc_datetime											AS doc_order_rn_datetime
	/*
	,if(
			rd.self_order_id != 0, 
			toInt64(rd.self_order_id), 
			-reinterpretAsInt64( cityHash64(rd.order_roznica_guid_uid) ) 		-- cityHash64() возвращает UInt64, надо преобразовать в Int64     	
		) 																	AS total_order_id	
	,cityHash64(
		df.date_id, 
		rd.self_order_id, 
		rd.order_roznica_guid_uid
	) 																		AS date_total_order_id -- (UInt64)
	*/
	,rd.self_order_id														AS total_order_id			
	,toInt64(toYYYYMMDD(df.date_id)) * 100_000_000_000 
		+ toInt64(rd.self_order_id)											AS date_total_order_id	-- (Int64)
	,df.unit_name 															AS unit_name							
	,df.unit_guid_OPN_uid													AS unit_guid_OPN_uid					
	,df.nomenclature_name													AS product_name
	,df.nomenclature_guid_OPN_uid											AS product_guid_OPN_uid 
	,df.nomenclature_property_name											AS product_property_name
	,df.nomenclature_property_guid_OPN_uid									AS product_property_guid_OPN_uid
	/*
	,if(
			df.product_art_id != 0, 
			toInt64(df.product_art_id), 
			-reinterpretAsInt64( cityHash64(
									df.nomenclature_guid_OPN_uid, 
									df.nomenclature_property_guid_OPN_uid) 
								)												-- cityHash64() возвращает UInt64, надо преобразовать в Int64	
        ) 										 							AS total_product_art_id
	,cityHash64(
		df.date_id, 
		df.product_art_id, 
		df.nomenclature_guid_OPN_uid, 
		df.nomenclature_property_guid_OPN_uid
	)																		AS date_total_product_art_id -- (UInt64)
    */
	,df.product_art_id														AS total_product_art_id 
	,toInt64(toYYYYMMDD(df.date_id)) * 100_000_000_000 
		+ toInt64(df.product_art_id)										AS date_total_product_art_id -- (Int64)
    ,df.packet_name															AS packet_name
	,df.packet_guid_OPN_uid													AS packet_guid_OPN_uid
	,df.packet_property_name												AS packet_property_name
	,df.packet_property_guid_OPN_uid										AS packet_property_guid_OPN_uid
	/*
	,if(
			df.packet_art_id != 0, 
			toInt64(df.packet_art_id), 
			-reinterpretAsInt64( cityHash64(
									df.packet_guid_OPN_uid, 
									df.packet_property_guid_OPN_uid) 
								) 	 											-- cityHash64() возвращает UInt64, надо преобразовать в Int64
		) 										 							AS total_packet_art_id
	,cityHash64(
		df.date_id, 
		df.packet_art_id, 
		df.packet_guid_OPN_uid, 
		df.packet_property_guid_OPN_uid
	) 																		AS date_total_packet_art_id
	*/
	,df.packet_art_id														AS total_packet_art_id
	,toInt64(toYYYYMMDD(df.date_id)) * 100_000_000_000 
		+ toInt64(df.packet_art_id)											AS date_total_packet_art_id	-- (Int64)
	,df.kolichestvo_dolg													AS kolichestvo_dolg
	,rd.total_sum_culculate								 					AS order_total_sum
	,df.is_ispolzuemaya_today												AS is_ispolzuemaya_today
	,df.status_modul_today_short_name										AS status_modul_today_short_name
	,df.status_packet_today_short_name										AS status_packet_today_short_name
	,df.is_ispolzuemaya_create_date											AS is_ispolzuemaya_create_date
	,df.status_modul_create_date_short_name									AS status_modul_create_date_short_name
	,df.status_packet_create_date_short_name								AS status_packet_create_date_short_name
	,df.doc_razmeshhenie_rasp_str											AS doc_razmeshhenie_rasp_str
	,df.doc_razmeshhenie_rasp_number										AS doc_razmeshhenie_rasp_number
	,df.doc_razmeshhenie_rasp_data											AS doc_razmeshhenie_rasp_data
	,df.unit_proizvodstva_name												AS unit_proizvodstva_name
	,df.smena_name															AS smena_name
	,df.is_iz_gorizonta														AS is_iz_gorizonta
	,df.data_vyhoda_iz_proizvodstva											AS data_vyhoda_iz_proizvodstva
	,df.data_zadaniya														AS data_zadaniya
FROM
	cte_def AS df					-- 14 066 строк (на 2026-10-13)	
	INNER JOIN cte_rd AS rd  		-- 2 361 строк (на 2026-10-13)
		ON df.date_id = rd.date_id 
		AND df.full_order_id = rd.full_order_id


--SELECT 
--	sum(total_sum)
--FROM
--	(SELECT total_order_id, min(order_total_sum) AS total_sum FROM cte GROUP BY total_order_id)

--SELECT * FROM cte_art				-- 39 721 строк (на 2026-10-13)
--SELECT * FROM cte_def				-- 14 066 строк (на 2026-10-13)
--SELECT * FROM cte_packets			-- 38 434 строк (на 2026-10-13)
--SELECT * FROM cte_rd				-- 2 361 строк (на 2026-10-13)
--SELECT * FROM cte_self			-- 6 511 строк (на 2026-10-13)
--SELECT * FROM cte_full			-- 2 064 строк (на 2026-10-13)
--SELECT * FROM cte_packets WHERE order_roznica_guid_str = ''
--SELECT * FROM cte_def --WHERE order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
--SELECT * FROM cte_def WHERE product_art_id = 0
--SELECT * FROM cte_def WHERE packet_art_id = 0
--SELECT * FROM cte_rd WHERE full_order_id = 2876203
