-- "db1": DB1
-- "db2": DB2 
WITH 
cte_dt AS 
(
	SELECT 
	 	batch_id_dttm							AS batch_id_dttm	
		,batch_id_str							AS batch_id_str
		,toDate(datetime_id)					AS date_id
		,subtractDays(toDate(datetime_id), 1)	AS prev_date_id					-- Добавлено 2026-06-01: Прерыдущий день для mg_VPointStatusIndividualOrders
		,subtractYears(datetime_id, 2)			AS two_years_ago_datetime_id	-- Добавлено 2026-08-05: Для ограничения выборки из ПараметрыЗаказов - 2 последних года
		,datetime_id							AS datetime_id
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-08-03')
	WHERE batch_id_str = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_str") }}'
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
pl AS
(
	SELECT
		toUUIDOrZero(pkt.order_roznica_guid_str)								AS order_roznica_guid_uid
		,argMin(pkt.doc_order_rn_guid_1C_uid, pkt.doc_order_rn_datetime)		AS doc_order_rn_guid_1C_uid
		,argMin(pkt.doc_order_rn_number, pkt.doc_order_rn_datetime)				AS doc_order_rn_number
		,min(pkt.doc_order_rn_datetime)											AS doc_order_rn_datetime
		,argMin(pkt.unit_guid_OPN_uid, pkt.doc_order_rn_datetime)				AS unit_guid_OPN_uid
		,argMin(pkt.unit_name, pkt.doc_order_rn_datetime)						AS unit_name
		,argMin(pkt.city_guid_OPN_uid, pkt.doc_order_rn_datetime)				AS city_guid_OPN_uid
		,argMin(pkt.city_name, pkt.doc_order_rn_datetime)						AS city_name
		,argMin(pkt.iz_trek_mik_name, pkt.doc_order_rn_datetime)				AS mipik_trek_mik_name
		,argMin(pkt.iz_trek_mik_guid_OPN_uid, pkt.doc_order_rn_datetime)		AS mipik_trek_mik_guid_uid
		,min(is_oplachen_roznica)												AS is_oplachen_roznica
		,max(is_po_prosbe_clienta_roznica)										AS is_po_prosbe_clienta_roznica
		,max(data_dostavki_dogovor_roznica)										AS data_dostavki_dogovor_roznica
		,max(data_dostavki_roznica)												AS data_dostavki_roznica
		,max(data_otsechki)														AS data_otsechki
		,max(data_komplekta_rasp)												AS data_komplekta
		,max(data_celevaya_rasp)												AS data_celevaya
		,any(str_kommentarii_roznica)											AS kommentarii_roznica
		,sum(kolichestvo_dolg)													AS kolichestvo_dolg_total
	 FROM 
	 	--rasp1_v02.packet AS pkt
		{{ params.db1 }}.packet AS pkt
 	 WHERE
		pkt.unit_guid_OPN_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) -- Подразделеня-Розница
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
)
,
cte AS 
(
	SELECT 
		prd.order_guid_uid															AS order_roznica_guid_uid
		,min(bch.batch_id_dttm)														AS batch_id_dttm
		,argMin(bch.batch_id_str, bch.batch_id_dttm)								AS batch_id_str
		,argMin(bch.date_id, bch.batch_id_dttm)										AS date_id	
		,argMin(prd.order_id, prd.order_doc_date)									AS order_id
		,argMin(prd.order_doc_num, prd.order_doc_date)								AS order_doc_num
		,min(prd.order_doc_date)													AS order_doc_date
		,argMin(prd.unit_guid_uid, prd.order_doc_date)								AS unit_guid_uid
		,argMin(prd.unit_name, prd.order_doc_date)									AS unit_name
		,argMin(prd.city_guid_uid, prd.order_doc_date)								AS city_guid_uid
		,argMin(prd.city_name, prd.order_doc_date)									AS city_name	
		,argMin(prd.client_id, prd.order_doc_date)									AS client_id
		,argMin(prd.client_name, prd.order_doc_date)								AS client_name
		,argMin(prd.client_guid_uid, prd.order_doc_date)							AS client_guid_uid
		,argMin(prd.order_phone, prd.order_doc_date)								AS order_phone
		,argMin(prd.employee_id, prd.order_doc_date)								AS employee_id
		,argMin(prd.employee_name, prd.order_doc_date)								AS employee_name
		,argMin(prd.employee_guid_uid, prd.order_doc_date)							AS employee_guid_uid
		,argMin(prd.order_is_individual, prd.order_doc_date)						AS is_mip
		,argMin(prd.order_is_kitchen, prd.order_doc_date)							AS is_kitchen
		,argMin(prd.order_mipik_collection_id, prd.order_doc_date)					AS mipik_collection_id
		,argMin(prd.order_mipik_collection_name, prd.order_doc_date)				AS mipik_collection_name
		,argMin(prd.order_mipik_collection_guid_uid, prd.order_doc_date)			AS mipik_collection_guid_uid
		,argMin(prd.order_mipik_assortiment_group_id, prd.order_doc_date)			AS mipik_assortiment_group_id
		,argMin(prd.order_mipik_assortiment_group_name, prd.order_doc_date)			AS mipik_assortiment_group_name
		,argMin(prd.order_mipik_assortiment_group_guid_uid, prd.order_doc_date)		AS mipik_assortiment_group_guid_uid
		,argMin(prd.order_mipik_customization_type_id, prd.order_doc_date)			AS mipik_customization_type_id
		,argMin(prd.order_mipik_customization_type_name, prd.order_doc_date)		AS mipik_customization_type_name
		,argMin(prd.order_mipik_customization_type_guid_uid, prd.order_doc_date)	AS mipik_customization_type_guid_uid
		,sum(prd.total_sum)															AS total_sum	
	FROM 
		rasp2.open_orders_goods_history_rn AS prd	
		LEFT JOIN cte_dt AS bch
			ON prd.date_id = bch.date_id
	WHERE 
		--prd.date_id = toDate('2026-05-15')				-- Заменить prd.date_id = toDate('2026-05-15') на текущую дату !!!
		prd.date_id = (SELECT date_id FROM cte_dt LIMIT 1)
	GROUP BY
		prd.order_guid_uid
)
INSERT INTO {{ params.db2 }}.all_orders(
--INSERT INTO rasp2_v02.all_orders(
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
	,count_perenosov				-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,row_number_parametry_zakaza	-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,is_oplachen_roznica
	,is_po_prosbe_clienta_roznica 
	,data_dostavki_dogovor_roznica
	,data_dostavki_roznica
	,data_otsechki  
	,data_komplekta							
	,data_celevaya							
	,kommentarii_roznica	
	,total_sum
	,kolichestvo_dolg_total		-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить				
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
	,pl.doc_order_rn_guid_1C_uid
	,cte.order_doc_num
	,cte.order_doc_date
	,cte.unit_guid_uid
	,cte.unit_name
	,cte.city_guid_uid
	,cte.city_name	
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
	,cel.count_perenosov					-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,row_n.row_number_parametry_zakaza		-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,pl.is_oplachen_roznica
	,pl.is_po_prosbe_clienta_roznica 
	,pl.data_dostavki_dogovor_roznica
	,pl.data_dostavki_roznica
	,pl.data_otsechki  
	,pl.data_komplekta							-- V
	,pl.data_celevaya							-- V
	,pl.kommentarii_roznica						-- V
	,cte.total_sum
	,pl.kolichestvo_dolg_total				-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,dfct.count_deficit
	,dfct.count_not_ispolzuemaya_create_date_modul_without_vypusk
	,dfct.count_status_un_dev_set_modul_without_vypusk
FROM 
	cte 
	-- Оплачен и по просьбе клиента
	LEFT JOIN pl ON cte.order_roznica_guid_uid = pl.order_roznica_guid_uid
	-- Статусы МИП
	LEFT JOIN 
		(
			SELECT
				toUUIDOrZero(order_guid_1C) 		AS order_guid_uid	
				,any(order_status_id)				AS mipik_status_id
				,any(status_name)					AS mipik_status_name
				,any(status_order)					AS mipik_status_order
				,any(order_sum)						AS mipik_order_sum
			FROM
				rasp2.mg_VPointStatusIndividualOrders
			WHERE
			 	point_date = (SELECT prev_date_id FROM cte_dt LIMIT 1) 	-- Изменено 2026-06-01 (point_date - это вчера, данные по итогам дня)
			GROUP BY
				toUUIDOrZero(order_guid_1C)
		) AS mip 
		ON cte.order_roznica_guid_uid = mip.order_guid_uid
	-- Переносы РГС - до какой даты последний перенос	
	LEFT JOIN 
		(
			SELECT	
				doc_order_rn_guid_1C_uid			AS doc_order_rn_guid_1C_uid
				,argMax(data_celevaya, datetime_id)	AS max_data_perenosa     	-- Целевая дата для последнего распределения (MAX datetime_id), у которого schetchik_perenosov = 1
				-- mg-22.07.2026 -------------------------------------------------
				,count()							AS count_perenosov				
				-- mg-22.07.2026 -------------------------------------------------
			FROM 	
				rasp1.parametry_zakaza
			WHERE	
				schetchik_perenosov = 1
				AND datetime_id <= (SELECT datetime_id FROM cte_dt ORDER BY batch_id_dttm DESC LIMIT 1)	-- Добавлено 30.05.2026
				AND datetime_id >= (SELECT two_years_ago_datetime_id FROM cte_dt ORDER BY batch_id_dttm DESC LIMIT 1)	-- Добавлено 2026-08-05: Для ограничения выборки из ПараметрыЗаказов - 2 последних года
			GROUP BY	
				doc_order_rn_guid_1C_uid
		) AS cel
		ON pl.doc_order_rn_guid_1C_uid = cel.doc_order_rn_guid_1C_uid
	-- Добавлено 2026-08-05: Заказы в текущем распределении
	LEFT JOIN 
		(
			SELECT
				doc_order_rn_guid_1C_uid			AS doc_order_rn_guid_1C_uid
				,max(`row_number`)					AS row_number_parametry_zakaza
			FROM 
				rasp1.parametry_zakaza
			WHERE
				datetime_id = (SELECT datetime_id FROM cte_dt ORDER BY batch_id_dttm DESC LIMIT 1)
			GROUP BY	
				doc_order_rn_guid_1C_uid
		) AS row_n
		ON pl.doc_order_rn_guid_1C_uid = row_n.doc_order_rn_guid_1C_uid 
	-- Дефицит
	LEFT JOIN  
		(
			SELECT
				order_guid_uid									AS order_guid_uid
				,countIf(
						is_iz_proizvodstva_rasp = 1
						or is_s_uchetom_vypuska_rasp = 1
					)											AS count_deficit
				,countIf(
						is_iz_gorizonta = 1	
						and is_ispolzuemaya_create_date = 0
					)											AS count_not_ispolzuemaya_create_date_modul_without_vypusk
				-- 1 = Un, 3 = Dev, 5 = Set И нет плана выпуска (is_iz_gorizonta = True)
				,countIf(
						is_iz_gorizonta = 1
						and status_modul_create_date_number IN (1, 3, 5)
					)											AS count_status_un_dev_set_modul_without_vypusk
			FROM
			 	--rasp2_v02.all_goods
				{{ params.db2 }}.all_goods
			WHERE
			 	batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
			GROUP BY
			 	order_guid_uid
		) AS dfct
		ON cte.order_roznica_guid_uid = dfct.order_guid_uid
WHERE 
	cte.order_roznica_guid_uid != toUUID('00000000-0000-0000-0000-000000000000')
