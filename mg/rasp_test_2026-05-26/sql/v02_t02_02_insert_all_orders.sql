-- "db1_source": DB1_source
-- "db2": DB2 
WITH 
cte_dt AS 
	(
	SELECT 
	 	batch_id_dttm						AS batch_id_dttm	
		,batch_id_str						AS batch_id_str
		,create_dttm						AS create_dttm
		,toDate(datetime_id)				AS date_id
		,addDays(toDate(datetime_id), -1) 	AS prev_date_id		-- Добавлено 2026-06-01
		,datetime_id						AS datetime_id
	FROM {{ params.db1_source }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-05-15')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	ORDER BY create_dttm DESC
	LIMIT 1
	)
,
rn AS
(
	SELECT 
		DISTINCT
		toUUIDOrZero(pkt.order_roznica_guid_str)	AS order_roznica_guid_uid
		,pkt.doc_order_rn_guid_1C_uid				AS doc_order_rn_guid_1C_uid
	 FROM 
		{{ params.db1_source }}.packet AS pkt
	 WHERE
		pkt.unit_guid_OPN_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
		--pkt.unit_guid_OPN_str IN (SELECT unit_guid_str FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) -- Подразделеня-Розница -- unit_guid_OPN_str = unit_guid_CONS_str - для всех строк
		-- Не на страховой запас
		AND pkt.is_strahovoj_zapas_roznica = 0
		-- Не на выставку
		AND pkt.is_vystavka_roznica = 0	
		-- Не рекламации
		AND pkt.is_reklamacii = 0
		-- Не полиграфия
		AND pkt.is_poligrafiya = 0
		--AND pkt.date_id = toDate('2026-05-15')
		AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
)
,
cte AS 
(
	SELECT 
		DISTINCT
		bch.batch_id_dttm									AS batch_id_dttm
		,bch.batch_id_str									AS batch_id_str
		,bch.create_dttm									AS create_dttm
		,bch.date_id										AS date_id
		,prd.order_guid_uid									AS order_roznica_guid_uid
		,prd.order_id										AS order_id
		,prd.client_id										AS client_id
		,prd.client_name									AS client_name
		,prd.client_guid_uid								AS client_guid_uid
		,prd.order_phone									AS order_phone
		,prd.employee_id									AS employee_id
		,prd.employee_name									AS employee_name
		,prd.employee_guid_uid								AS employee_guid_uid
		,prd.order_is_individual							AS is_mip
		,prd.order_is_kitchen								AS is_kitchen
		,prd.order_mipik_collection_id						AS mipik_collection_id
		,prd.order_mipik_collection_name					AS mipik_collection_name
		,prd.order_mipik_collection_guid_uid				AS mipik_collection_guid_uid
		,prd.order_mipik_assortiment_group_id				AS mipik_assortiment_group_id
		,prd.order_mipik_assortiment_group_name				AS mipik_assortiment_group_name
		,prd.order_mipik_assortiment_group_guid_uid			AS mipik_assortiment_group_guid_uid
		,prd.order_mipik_customization_type_id				AS mipik_customization_type_id
		,prd.order_mipik_customization_type_name			AS mipik_customization_type_name
		,prd.order_mipik_customization_type_guid_uid		AS mipik_customization_type_guid_uid
	FROM 
		rasp2.open_orders_goods_history_rn AS prd	
		LEFT JOIN cte_dt AS bch
			ON prd.date_id = bch.date_id
	WHERE 
		--prd.date_id = toDate('2026-05-15')				-- Заменить prd.date_id = toDate('2026-05-15') на текущую дату !!!
		prd.date_id = (SELECT date_id FROM cte_dt LIMIT 1)
)
INSERT INTO {{ params.db2 }}.all_orders(
	batch_id_dttm							
	,batch_id_str							
	,create_dttm							
	,date_id 
	,order_id
	,order_roznica_guid_uid	
	,doc_order_opn_guid_1C_uid
	,order_roznica_doc_num
	,order_roznica_doc_datetime
	,unit_guid_uid
	,unit_name
	,city_guid_uid
	,city_name
	,total_sum
	,client_id
	,client_name
	,client_guid_uid
	,order_phone
	,employee_id
	,employee_name
	,employee_guid_uid
	,is_mip
	,is_kitchen
	,mipik_status_id	-- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
	,mipik_status_name						
	,mipik_status_order						
	,mipik_order_sum						
	,mipik_collection_id					
	,mipik_collection_name					
	,mipik_collection_guid_uid				
	,mipik_assortiment_group_id				
	,mipik_assortiment_group_name			
	,mipik_assortiment_group_guid_uid		
	,mipik_customization_type_id		
	,mipik_customization_type_name
	,mipik_customization_type_guid_uid
	,mipik_trek_mik_name			
	,mipik_trek_mik_guid_uid		
	,max_data_perenosa
	,is_oplachen_roznica
	,is_po_prosbe_clienta_roznica 
	,data_dostavki_dogovor_roznica
	,data_dostavki_roznica
	,data_otsechki  
	,data_komplekta							
	,data_celevaya							
	,kommentarii_roznica					
	,count_deficit
	,count_not_ispolzuemaya_create_date_modul_without_vypusk
	,count_status_un_dev_set_modul_without_vypusk
)
SELECT 
	cte.batch_id_dttm
	,cte.batch_id_str
	,now('Europe/Moscow')
	,cte.date_id	
	--,toDate('2026-04-17')
	,cte.order_id
	,cte.order_roznica_guid_uid 	
	,rn.doc_order_rn_guid_1C_uid
	,sm.order_doc_num
	,sm.order_doc_date
	,sm.unit_guid_uid
	,sm.unit_name
	,sm.city_guid_uid
	,sm.city_name
	,sm.total_sum
	,cte.client_id
	,cte.client_name
	,cte.client_guid_uid
	,cte.order_phone
	,cte.employee_id
	,cte.employee_name
	,cte.employee_guid_uid
	,cte.is_mip
	,cte.is_kitchen
	,mip.mipik_status_id	-- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
	,mip.mipik_status_name						-- V
	,mip.mipik_status_order						-- V
	,mip.mipik_order_sum						-- V
	,cte.mipik_collection_id					-- V
	,cte.mipik_collection_name					-- V
	,cte.mipik_collection_guid_uid				-- V
	,cte.mipik_assortiment_group_id				-- V
	,cte.mipik_assortiment_group_name			-- V
	,cte.mipik_assortiment_group_guid_uid		-- V
	,cte.mipik_customization_type_id			-- V
	,cte.mipik_customization_type_name			-- V
	,cte.mipik_customization_type_guid_uid		-- V
	,pl.mipik_trek_mik_name						-- V
	,pl.mipik_trek_mik_guid_uid					-- V
	,cel.max_data_perenosa
	,pl.is_oplachen_roznica
	,pl.is_po_prosbe_clienta_roznica 
	,pl.data_dostavki_dogovor_roznica
	,pl.data_dostavki_roznica
	,pl.data_otsechki  
	,pl.data_komplekta							-- V
	,pl.data_celevaya							-- V
	,pl.kommentarii_roznica						-- V
	,dfct.count_deficit
	,dfct.count_not_ispolzuemaya_create_date_modul_without_vypusk
	,dfct.count_status_un_dev_set_modul_without_vypusk
FROM 
	cte 
	LEFT JOIN rn ON cte.order_roznica_guid_uid = rn.order_roznica_guid_uid
	LEFT JOIN 
		(
		 SELECT 			
			prd.order_guid_uid					AS order_guid_uid	
			,prd.order_doc_num					AS order_doc_num
			,prd.order_doc_date					AS order_doc_date
			,prd.unit_guid_uid 					AS unit_guid_uid
			,prd.unit_name						AS unit_name
			,prd.city_guid_uid 					AS city_guid_uid
			,prd.city_name						AS city_name
			,sum(prd.total_sum)					AS total_sum			
	 	 FROM 
			rasp2.open_orders_goods_history_rn 	AS prd	
	 	 WHERE 
			--prd.date_id = toDate('2026-05-15')				-- Заменить prd.date_id = toDate('2026-05-15') на текущую дату !!!
	 	 	prd.date_id = (SELECT date_id FROM cte_dt LIMIT 1)
		 GROUP BY  
		 	prd.order_guid_uid		
			,prd.order_doc_num					
			,prd.order_doc_date					
			,prd.unit_guid_uid 	
			,prd.unit_name						
			,prd.city_guid_uid	
			,prd.city_name								
		) AS sm
		ON cte.order_roznica_guid_uid = sm.order_guid_uid
	-- Статусы МИП
	LEFT JOIN 
		(SELECT
			toUUIDOrZero(order_guid_1C) 		AS order_guid_uid	
			,order_status_id					AS mipik_status_id
			,status_name						AS mipik_status_name
			,status_order						AS mipik_status_order
			,order_sum							AS mipik_order_sum
		 FROM
			from_mssql.mg_VPointStatusIndividualOrders 
		 WHERE
		 	--point_date = toDate('2026-05-15')
		 	--point_date = (SELECT date_id FROM cte_dt LIMIT 1)
		 	point_date = (SELECT prev_date_id FROM cte_dt LIMIT 1) 	-- Изменено 2026-06-01 (point_date - это вчера, данные по итога дня)
		)AS mip 
		ON cte.order_roznica_guid_uid = mip.order_guid_uid
	-- Переносы РГС - до какой даты последний перенос	
	LEFT JOIN 
		(SELECT	
			doc_order_rn_guid_1C_uid			AS doc_order_rn_guid_1C_uid
			,argMax(data_celevaya, datetime_id)	AS max_data_perenosa     	-- Целевая дата для последнего распределения (MAX datetime_id), у которого schetchik_perenosov = 1
		 FROM 	
			rasp1.parametry_zakaza
		 WHERE	
			schetchik_perenosov = 1
			AND datetime_id <= (SELECT datetime_id FROM cte_dt ORDER BY create_dttm DESC LIMIT 1)	-- Добавлено 30.05.2026
		 GROUP BY	
			doc_order_rn_guid_1C_uid
		) AS cel
		ON rn.doc_order_rn_guid_1C_uid = cel.doc_order_rn_guid_1C_uid
	-- Оплачен и по просьбе клиента
	LEFT JOIN 
		(SELECT
			toUUIDOrZero(pkt.order_roznica_guid_str)	AS order_roznica_guid_uid
			,pkt.doc_order_rn_number					AS doc_order_rn_number
			,pkt.doc_order_rn_datetime					AS doc_order_rn_datetime
			,pkt.unit_guid_OPN_uid						AS unit_guid_OPN_uid
			,pkt.unit_name								AS unit_name
			,pkt.city_guid_OPN_uid						AS city_guid_OPN_uid
			,pkt.city_name								AS city_name
			,pkt.iz_trek_mik_name						AS mipik_trek_mik_name
			,pkt.iz_trek_mik_guid_OPN_uid				AS mipik_trek_mik_guid_uid
			,min(is_oplachen_roznica)					AS is_oplachen_roznica
			,max(is_po_prosbe_clienta_roznica)			AS is_po_prosbe_clienta_roznica
			,max(data_dostavki_dogovor_roznica)			AS data_dostavki_dogovor_roznica
			,max(data_dostavki_roznica)					AS data_dostavki_roznica
			,max(data_otsechki)							AS data_otsechki
			,max(data_komplekta_rasp)					AS data_komplekta
			,max(data_celevaya_rasp)					AS data_celevaya
			,any(str_kommentarii_roznica)				AS kommentarii_roznica
		 FROM 
			{{ params.db1_source }}.packet AS pkt
	 	 WHERE
			pkt.unit_guid_OPN_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
			--pkt.unit_guid_OPN_str IN (SELECT unit_guid_str FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) -- Подразделеня-Розница -- unit_guid_OPN_str = unit_guid_CONS_str - для всех строк
			-- Не на страховой запас
			AND pkt.is_strahovoj_zapas_roznica = 0
			-- Не на выставку
			AND pkt.is_vystavka_roznica = 0	
			-- Не рекламации
			AND pkt.is_reklamacii = 0
			-- Не полиграфия
			AND pkt.is_poligrafiya = 0
			--AND pkt.date_id = toDate('2026-05-15')
			AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		 GROUP BY 
		 	toUUIDOrZero(pkt.order_roznica_guid_str)
		 	,pkt.doc_order_rn_number					
			,pkt.doc_order_rn_datetime					
			,pkt.unit_guid_OPN_uid						
			,pkt.unit_name								
			,pkt.city_guid_OPN_uid						
			,pkt.city_name	
			,pkt.iz_trek_mik_name
			,pkt.iz_trek_mik_guid_OPN_uid
		) AS pl
		ON cte.order_roznica_guid_uid = pl.order_roznica_guid_uid
	-- Дефицит
	LEFT JOIN  
		(SELECT
			order_guid_uid									AS order_guid_uid
			,countIf(
					is_iz_proizvodstva_rasp = 1
					or is_s_uchetom_vypuska_rasp = 1
				)											AS count_deficit
			,countIf(
					is_iz_gorizonta = 1	
--						(
--							is_iz_gorizonta = 1
--							or (
--									is_iz_proizvodstva_rasp = 0
--									and is_s_uchetom_vypuska_rasp = 0
--								)
--						)
					and is_ispolzuemaya_create_date = 0
				)											AS count_not_ispolzuemaya_create_date_modul_without_vypusk
			-- 1 = Un, 3 = Dev, 5 = Set И нет плана выпуска (is_iz_gorizonta = True)
			,countIf(
					is_iz_gorizonta = 1
--						(
--							is_iz_gorizonta = 1
--							or (
--									is_iz_proizvodstva_rasp = 0
--									and is_s_uchetom_vypuska_rasp = 0
--								)
--						)
					and status_modul_create_date_number IN (1, 3, 5)
				)											AS count_status_un_dev_set_modul_without_vypusk
		 FROM
		 	{{ params.db2 }}.all_goods
		 WHERE
		 	--date_id = toDate('2026-05-15')
		 	batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		 GROUP BY
		 	order_guid_uid
		) AS dfct
		ON cte.order_roznica_guid_uid = dfct.order_guid_uid
WHERE 
	cte.order_roznica_guid_uid != toUUID('00000000-0000-0000-0000-000000000000')
