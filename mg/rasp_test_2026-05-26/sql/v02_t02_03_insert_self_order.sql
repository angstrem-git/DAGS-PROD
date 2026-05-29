-- "db1_source": DB1_source
-- "db2": DB2
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
,
cte_prnt AS
(
	SELECT 
		DISTINCT
		order_id								AS order_id
		,order_doc_date							AS order_doc_date
		,order_doc_num							AS order_doc_num
		,order_guid_uid							AS order_guid_uid
		,parent_order_id						AS parent_order_id
		,parent_order_doc_date					AS parent_order_doc_date
		,parent_order_doc_num					AS parent_order_doc_num
		,parent_order_guid_uid					AS parent_order_guid_uid
	FROM
		rasp2.open_orders_goods_history_rn
	WHERE
		date_id = (SELECT date_id FROM cte_dt LIMIT 1)
)
,
cte AS 
(
SELECT
	ard.batch_id_dttm												AS batch_id_dttm							
	,ard.batch_id_str												AS batch_id_str							
	,now('Europe/Moscow')											AS create_dttm
	,ard.date_id													AS date_id
	,ard.order_id													AS order_id
	,ard.order_roznica_guid_uid										AS order_roznica_guid_uid
	,ard.order_roznica_doc_num										AS order_roznica_doc_num
	,ard.order_roznica_doc_datetime									AS order_roznica_doc_datetime
	,ard.unit_guid_uid												AS unit_guid_uid
	,ard.unit_name													AS unit_name
	,ard.city_guid_uid												AS city_guid_uid
	,ard.city_name													AS city_name
	,if(
		sn.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
		,ard.order_id	
		,sn.full_order_id
	)																AS full_order_id
	,if(
		sn.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
		,ard.order_roznica_guid_uid	
		,sn.full_order_roznica_guid_uid
	)																AS full_order_roznica_guid_uid
	,if(
		sn.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
		,ard.order_roznica_doc_num	
		,sn.full_order_roznica_number
	)																AS full_order_roznica_number
	,if(
		sn.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
		,ard.order_roznica_doc_datetime	
		,sn.full_order_roznica_date
	)																AS full_order_roznica_datetime		
	,sn.is_link_order												AS is_link_order
	,ard.total_sum													AS total_sum_total
	,multiIf( 
		ard.is_11_not_oplachen_mip,				1	
		,ard.is_21_design_mip,					1
		,ard.is_70_dostavka,					3	
		,ard.is_12_not_oplachen_not_mip,		2
		,ard.is_41_perenes_po_zayavleniyu,		2
		,ard.is_42_perenes_po_prosbe_clienta,	2
		,ard.is_43_otkaz_ot_dostavki,			2
		,ard.is_51_net_snyatogo_tovara,			2
		,ard.is_52_net_ne_snyatogo_tovara,		2
		,2
	)																AS source_stage_id
--	,multiIf( 
--		ard.is_11_not_oplachen_mip,				'Согласование МИПиК (1С УТ)'	
--		,ard.is_21_design_mip,					'Согласование МИПиК (1С УТ)'
--		,ard.is_70_dostavka,					'Доставка (1С УТ)'	
--		,ard.is_12_not_oplachen_not_mip,		'Распределение (1С ОПН)'
--		,ard.is_41_perenes_po_zayavleniyu,		'Распределение (1С ОПН)'
--		,ard.is_42_perenes_po_prosbe_clienta,	'Распределение (1С ОПН)'
--		,ard.is_43_otkaz_ot_dostavki,			'Распределение (1С ОПН)'
--		,ard.is_51_net_snyatogo_tovara,			'Распределение (1С ОПН)'
--		,ard.is_52_net_ne_snyatogo_tovara,		'Распределение (1С ОПН)'
--		,'Распределение (1С ОПН)'
--	)																AS source_stage_name
	,multiIf( 
		ard.is_11_not_oplachen_mip,				10	
		,ard.is_21_design_mip,					20
		,ard.is_70_dostavka,					70	
		,ard.is_12_not_oplachen_not_mip,		10
		,ard.is_41_perenes_po_zayavleniyu,		40
		,ard.is_42_perenes_po_prosbe_clienta,	40
		,ard.is_43_otkaz_ot_dostavki,			40
		,ard.is_51_net_snyatogo_tovara,			50
		,ard.is_52_net_ne_snyatogo_tovara,		50
		,60
	)																AS order_stage_id
--	,multiIf( 
--		ard.is_11_not_oplachen_mip,				'Нет оплаты'	
--		,ard.is_21_design_mip,					'МИПиК у дизайнера'
--		,ard.is_70_dostavka,					'Доставка'	
--		,ard.is_12_not_oplachen_not_mip,		'Нет оплаты'
--		,ard.is_41_perenes_po_zayavleniyu,		'Клиент не принимает'
--		,ard.is_42_perenes_po_prosbe_clienta,	'Клиент не принимает'
--		,ard.is_43_otkaz_ot_dostavki,			'Клиент не принимает'
--		,ard.is_51_net_snyatogo_tovara,			'Нет продукции'
--		,ard.is_52_net_ne_snyatogo_tovara,		'Нет продукции'
--		,'Ждет отгрузку из Воронежа'
--	)																AS order_stage_name
	,multiIf( 
		ard.is_11_not_oplachen_mip,				110	
		,ard.is_21_design_mip,					210
		,ard.is_70_dostavka,					710	
		,ard.is_12_not_oplachen_not_mip,		120
		,ard.is_41_perenes_po_zayavleniyu,		410
		,ard.is_42_perenes_po_prosbe_clienta,	420
		,ard.is_43_otkaz_ot_dostavki,			430
		,ard.is_51_net_snyatogo_tovara,			510
		,ard.is_52_net_ne_snyatogo_tovara,		520
		,610
	)																AS order_sub_stage_id
--	,multiIf( 
--		ard.is_11_not_oplachen_mip,				'Нет оплаты МИПиК'	
--		,ard.is_21_design_mip,					'МИПиК у дизайнера'
--		,ard.is_70_dostavka,					'Доставка'	
--		,ard.is_12_not_oplachen_not_mip,		'Нет оплаты (НЕ МИПиК)'
--		,ard.is_41_perenes_po_zayavleniyu,		'Перенос по заявлению'
--		,ard.is_42_perenes_po_prosbe_clienta,	'Перенос по просьбе клиента'
--		,ard.is_43_otkaz_ot_dostavki,			'Отказ от доставки'
--		,ard.is_51_net_snyatogo_tovara,			'Нет снятой продукции'
--		,ard.is_52_net_ne_snyatogo_tovara,		'Нет продукции'
--		,										'Ждет отгрузку из Воронежа'
--	)																AS order_sub_stage_name
	,tp.has_std1													AS has_std1
	,tp.is_deficit_std1												AS is_deficit_std1	
	,tp.total_sum_std1												AS total_sum_std1
	,tp.has_std2													AS has_std2
	,tp.is_deficit_std2												AS is_deficit_std2
	,tp.total_sum_std2												AS total_sum_std2
	,tp.has_mip														AS has_mip
	,tp.is_deficit_mip												AS is_deficit_mip	
	,tp.total_sum_mip												AS total_sum_mip
	,tp.has_kich													AS has_kich
	,tp.is_deficit_kich												AS is_deficit_kich
	,tp.total_sum_kich												AS total_sum_kich
	,tp.has_stor													AS has_stor
	,tp.is_deficit_stor												AS is_deficit_stor
	,tp.total_sum_stor												AS total_sum_stor
	,tp.has_matr													AS has_matr
	,tp.is_deficit_matr												AS is_deficit_matr
	,tp.total_sum_matr												AS total_sum_matr
	,tp.has_tech													AS has_tech
	,tp.is_deficit_tech												AS is_deficit_tech
	,tp.total_sum_tech												AS total_sum_tech
	,tp.has_other													AS has_other
	,tp.is_deficit_other											AS is_deficit_other
	,tp.total_sum_other												AS total_sum_other
	,tp.has_serv													AS has_serv
	,tp.total_sum_serv												AS total_sum_serv
	,ard.client_id													AS client_id							-- V
	,ard.client_name												AS client_name							-- V
	,ard.client_guid_uid											AS client_guid_uid						-- V
	,ard.order_phone												AS order_phone							-- V
	,ard.employee_id												AS employee_id							-- V
	,ard.employee_name												AS employee_name						-- V
	,ard.employee_guid_uid											AS employee_guid_uid					-- V
	,ard.is_mip														AS is_mip								-- V
	,ard.is_kitchen													AS is_kitchen							-- V
	,ard.mipik_status_id											AS mipik_status_id						-- V 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
	,ard.mipik_status_name											AS mipik_status_name					-- V
	,ard.mipik_status_order											AS mipik_status_order					-- V
	,ard.mipik_collection_id										AS mipik_collection_id					-- V
	,ard.mipik_collection_name										AS mipik_collection_name 				-- V
	,ard.mipik_collection_guid_uid									AS mipik_collection_guid_uid 			-- V
	,ard.mipik_assortiment_group_id									AS mipik_assortiment_group_id 			-- V
	,ard.mipik_assortiment_group_name								AS mipik_assortiment_group_name 		-- V
	,ard.mipik_assortiment_group_guid_uid							AS mipik_assortiment_group_guid_uid 	-- V
	,ard.mipik_customization_type_id								AS mipik_customization_type_id 			-- V
	,ard.mipik_customization_type_name								AS mipik_customization_type_name 		-- V
	,ard.mipik_customization_type_guid_uid							AS mipik_customization_type_guid_uid 	-- V
	,ard.mipik_trek_mik_name										AS mipik_trek_mik_name					-- V
	,ard.mipik_trek_mik_guid_uid									AS mipik_trek_mik_guid_uid				-- V
	,ard.max_data_perenosa											AS max_data_perenosa 					-- V
	,ard.is_oplachen_roznica										AS is_oplachen_roznica 					-- V
	,ard.is_po_prosbe_clienta_roznica								AS is_po_prosbe_clienta_roznica 		-- V 
	,ard.data_dostavki_dogovor_roznica								AS data_dostavki_dogovor_roznica 		-- V
	,ard.data_dostavki_roznica										AS data_dostavki_roznica 				-- V
	,ard.data_otsechki  											AS data_otsechki 						-- V
	,ard.data_komplekta												AS data_komplekta 						-- V
	,ard.data_celevaya												AS data_celevaya 						-- V
	,ard.kommentarii_roznica										AS kommentarii_roznica					-- V
	,ard.norma_days_dostavki_iz_voronezha							AS norma_days_dostavki_iz_voronezha		-- V
FROM
	(
	SELECT 
		rs.batch_id_dttm																	AS batch_id_dttm							
		,rs.batch_id_str																	AS batch_id_str	
		,rs.date_id																			AS date_id
		,rs.order_id																		AS order_id
		,rs.order_roznica_guid_uid															AS order_roznica_guid_uid
		,rs.total_sum																		AS total_sum
		,(rs.mipik_status_id = 1) 															AS is_11_not_oplachen_mip		-- Не оплчачен МИПиК
		,(rs.mipik_status_id = 2) 															AS is_21_design_mip				-- МИПиК у дизайнеров
		,(rs.doc_order_opn_guid_1C_uid = toUUID('00000000-0000-0000-0000-000000000000') )	AS is_70_dostavka				-- Доставка в Рознице
		,(rs.is_oplachen_roznica = 0)														AS is_12_not_oplachen_not_mip	-- Не оплчачен НЕ МИПиК
		,(
				rs.is_po_prosbe_clienta_roznica = 1
				-- 27.05.2026 - вернул условие на ДатуДостаки (два вариант - ДатаДоставки позже ДатыДоговора и раньше): 
				AND rs.data_dostavki_roznica > rs.data_dostavki_dogovor_roznica
				-- Из ДатыДоставки вычитаем кол-во дней на доставку из Воронежа до клиента (параметры из словаря from_mssql.dict_city_delivery_days)
				AND (
						rs.data_dostavki_roznica 
						- INTERVAL dictGetOrDefault('from_mssql.dict_city_delivery_days', 'total_count_days', toString(rs.city_guid_uid), 7) DAY
					) >= (SELECT date_id FROM cte_dt LIMIT 1)			-- toDate('2026-04-17') 
			)																				AS is_41_perenes_po_zayavleniyu	-- Перенос доставки по заявлению
		,(
				rs.is_po_prosbe_clienta_roznica = 1
				-- 27.05.2026 - вернул условие на ДатуДостаки (два вариант - ДатаДоставки позже ДатыДоговора и раньше): 
				AND rs.data_dostavki_roznica <= rs.data_dostavki_dogovor_roznica
				-- Из ДатыДоставки вычитаем кол-во дней на доставку из Воронежа до клиента (параметры из словаря from_mssql.dict_city_delivery_days)
				AND (
						rs.data_dostavki_roznica 
						- INTERVAL dictGetOrDefault('from_mssql.dict_city_delivery_days', 'total_count_days', toString(rs.city_guid_uid), 7) DAY
					) >= (SELECT date_id FROM cte_dt LIMIT 1)			-- toDate('2026-04-17') 
			)																				AS is_42_perenes_po_prosbe_clienta	-- Перенос доставки по просьбе клиента
		,(rs.max_data_perenosa >= (SELECT date_id FROM cte_dt LIMIT 1) ) 	-- toDate('2026-04-17') ) 									
																							AS is_43_otkaz_ot_dostavki		-- Отказ клиента от доставки
		,(
				rs.count_deficit > 0
				AND rs.count_not_ispolzuemaya_create_date_modul_without_vypusk > 0
			)																				AS is_51_net_snyatogo_tovara	-- Нет снятого товара
		,(
				rs.count_deficit > 0
				AND rs.count_not_ispolzuemaya_create_date_modul_without_vypusk = 0
			)																				AS is_52_net_ne_snyatogo_tovara -- Нет НЕ снятого товара
		,rs.order_roznica_doc_num															AS order_roznica_doc_num
		,rs.order_roznica_doc_datetime														AS order_roznica_doc_datetime
		,rs.unit_guid_uid																	AS unit_guid_uid
		,rs.unit_name																		AS unit_name
		,rs.city_guid_uid																	AS city_guid_uid
		,rs.city_name																		AS city_name
		,rs.client_id																		AS client_id							-- V
		,rs.client_name																		AS client_name							-- V
		,rs.client_guid_uid																	AS client_guid_uid						-- V
		,rs.order_phone																		AS order_phone							-- V
		,rs.employee_id																		AS employee_id							-- V
		,rs.employee_name																	AS employee_name						-- V
		,rs.employee_guid_uid																AS employee_guid_uid					-- V
		,rs.is_mip																			AS is_mip								-- V
		,rs.is_kitchen																		AS is_kitchen							-- V
		,rs.mipik_status_id																	AS mipik_status_id -- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
		,rs.mipik_status_name																AS mipik_status_name 					-- V
		,rs.mipik_status_order																AS mipik_status_order 					-- V
		,rs.mipik_collection_id																AS mipik_collection_id 					-- V
		,rs.mipik_collection_name															AS mipik_collection_name 				-- V
		,rs.mipik_collection_guid_uid														AS mipik_collection_guid_uid 			-- V
		,rs.mipik_assortiment_group_id														AS mipik_assortiment_group_id 			-- V
		,rs.mipik_assortiment_group_name													AS mipik_assortiment_group_name 		-- V
		,rs.mipik_assortiment_group_guid_uid												AS mipik_assortiment_group_guid_uid 	-- V
		,rs.mipik_customization_type_id														AS mipik_customization_type_id 			-- V
		,rs.mipik_customization_type_name													AS mipik_customization_type_name 		-- V
		,rs.mipik_customization_type_guid_uid												AS mipik_customization_type_guid_uid 	-- V
		,rs.mipik_trek_mik_name																AS mipik_trek_mik_name					-- V
		,rs.mipik_trek_mik_guid_uid															AS mipik_trek_mik_guid_uid				-- V
		,rs.max_data_perenosa																AS max_data_perenosa 					-- V
		,rs.is_oplachen_roznica																AS is_oplachen_roznica 					-- V
		,rs.is_po_prosbe_clienta_roznica													AS is_po_prosbe_clienta_roznica 		-- V 
		,rs.data_dostavki_dogovor_roznica													AS data_dostavki_dogovor_roznica 		-- V
		,rs.data_dostavki_roznica															AS data_dostavki_roznica 				-- V
		,rs.data_otsechki  																	AS data_otsechki 						-- V
		,rs.data_komplekta																	AS data_komplekta 						-- V
		,rs.data_celevaya																	AS data_celevaya 						-- V
		,rs.kommentarii_roznica																AS kommentarii_roznica 					-- V
		,dictGetOrDefault('from_mssql.dict_city_delivery_days', 'total_count_days', toString(rs.city_guid_uid), 7) AS norma_days_dostavki_iz_voronezha
	FROM 
		{{ params.db2 }}.all_orders	AS rs
	WHERE 
		--rs.date_id = toDate('2026-04-17')
		rs.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
	)  																AS ard
	-- has и is_deficit всех типов
	LEFT JOIN (
			SELECT
				agg.order_guid_uid											AS order_guid_uid				
				,if(agg.has_std1_sum > 0, 1, 0) 							AS has_std1
				,if(agg.is_deficit_std1_sum > 0, 1, 0) 						AS is_deficit_std1
				,agg.total_sum_std1_sum										AS total_sum_std1				
				,if(agg.has_std2_sum > 0, 1, 0) 							AS has_std2
				,if(agg.is_deficit_std2_sum > 0, 1, 0) 						AS is_deficit_std2
				,agg.total_sum_std2_sum										AS total_sum_std2				
				,if(agg.has_mip_sum > 0, 1, 0) 								AS has_mip
				,if(agg.is_deficit_mip_sum > 0, 1, 0) 						AS is_deficit_mip
				,agg.total_sum_mip_sum										AS total_sum_mip				
				,if(agg.has_kich_sum > 0, 1, 0) 							AS has_kich
				,if(agg.is_deficit_kich_sum > 0, 1, 0) 						AS is_deficit_kich
				,agg.total_sum_kich_sum										AS total_sum_kich				
				,if(agg.has_stor_sum > 0, 1, 0) 							AS has_stor
				,if(agg.is_deficit_stor_sum > 0, 1, 0) 						AS is_deficit_stor
				,agg.total_sum_stor_sum										AS total_sum_stor				
				,if(agg.has_matr_sum > 0, 1, 0) 							AS has_matr
				,if(agg.is_deficit_matr_sum > 0, 1, 0) 						AS is_deficit_matr
				,agg.total_sum_matr_sum										AS total_sum_matr				
				,if(agg.has_tech_sum > 0, 1, 0) 							AS has_tech
				,if(agg.is_deficit_tech_sum > 0, 1, 0) 						AS is_deficit_tech
				,agg.total_sum_tech_sum										AS total_sum_tech				
				,if(agg.has_other_sum > 0, 1, 0) 							AS has_other
				,if(agg.is_deficit_other_sum > 0, 1, 0)		 				AS is_deficit_other
				,agg.total_sum_other_sum									AS total_sum_other				
				,if(agg.has_serv_sum > 0, 1, 0) 							AS has_serv
				,agg.total_sum_serv_sum										AS total_sum_serv				
			FROM
			(
				SELECT
					order_guid_uid										AS order_guid_uid			
					,sum(has_std1) 				 						AS has_std1_sum
					,sum(is_deficit_std1) 				 				AS is_deficit_std1_sum
					,sum(has_std1 * kolichestvo_total * avg_price)		AS total_sum_std1_sum			
					,sum(has_std2) 				 						AS has_std2_sum
					,sum(is_deficit_std2) 				 				AS is_deficit_std2_sum
					,sum(has_std2 * kolichestvo_total * avg_price)		AS total_sum_std2_sum			
					,sum(has_mip)				 						AS has_mip_sum
					,sum(is_deficit_mip)			 					AS is_deficit_mip_sum
					,sum(has_mip * kolichestvo_total * avg_price)		AS total_sum_mip_sum				
					,sum(has_kich) 										AS has_kich_sum
					,sum(is_deficit_kich) 				 				AS is_deficit_kich_sum
					,sum(has_kich * kolichestvo_total * avg_price)		AS total_sum_kich_sum				
					,sum(has_stor) 				 						AS has_stor_sum
					,sum(is_deficit_stor) 								AS is_deficit_stor_sum
					,sum(has_stor * kolichestvo_total * avg_price)		AS total_sum_stor_sum				
					,sum(has_matr) 										AS has_matr_sum
					,sum(is_deficit_matr)				 				AS is_deficit_matr_sum
					,sum(has_matr * kolichestvo_total * avg_price)		AS total_sum_matr_sum				
					,sum(has_tech) 				 						AS has_tech_sum
					,sum(is_deficit_tech) 				 				AS is_deficit_tech_sum
					,sum(has_tech * kolichestvo_total * avg_price)		AS total_sum_tech_sum			
					,sum(has_other)  									AS has_other_sum
					,sum(is_deficit_other) 				 				AS is_deficit_other_sum
					,sum(has_other * kolichestvo_total * avg_price)		AS total_sum_other_sum			
					,sum(has_serv) 				 						AS has_serv_sum
					,sum(has_serv * kolichestvo_total * avg_price)		AS total_sum_serv_sum				
				FROM {{ params.db2 }}.all_goods
				WHERE
					batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
				GROUP BY order_guid_uid
			) AS agg
		) 															AS tp
		ON ard.order_roznica_guid_uid = tp.order_guid_uid
	-- Обогащение
	LEFT JOIN (
			SELECT 
				DISTINCT
				pnt.order_guid_uid							AS order_guid_uid					-- GUID Заказ покупателя в 1С УТ
				,pnt.parent_order_guid_uid					AS parent_order_guid_uid			-- GUID Основной Заказ покуп. розница в 1С ОПН
				,if(
					pnt.parent_order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
					,pnt.order_id	
					,pnt.parent_order_id
				)											AS full_order_id
				,if(
					pnt.parent_order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
					,pnt.order_guid_uid		
					,pnt.parent_order_guid_uid
				)											AS full_order_roznica_guid_uid			-- GUID Заказ покупателя в 1С УТ
				,if(
					pnt.parent_order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
					,pnt.order_doc_num		
					,pnt.parent_order_doc_num
				)											AS full_order_roznica_number
				,if(
					pnt.parent_order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
					,pnt.order_doc_date	
					,pnt.parent_order_doc_date
				)											AS full_order_roznica_date
				,if(
					pnt.parent_order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
						and slv.parent_order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
					,0
					,1
				)											AS is_link_order
	 		FROM 
				cte_prnt 								AS pnt	
				ANY LEFT JOIN 	-- Может быть несколько подчиненныз заказов. Нужен только один факт подчиненного заказа -> ANY JOIN
					(SELECT DISTINCT parent_order_guid_uid									-- GUID Заказ покупателя розница в 1С ОПН
			 		 FROM cte_prnt
				 	 ) 		AS slv
				 	ON pnt.order_guid_uid = slv.parent_order_guid_uid
		) 																AS sn
		ON ard.order_roznica_guid_uid = sn.order_guid_uid	
)
INSERT INTO {{ params.db3 }}.self_order(
	batch_id_dttm							
	,batch_id_str							
	,create_dttm
	,date_id 
	,order_id
	,order_roznica_guid_uid 
	,order_roznica_doc_num
	,order_roznica_doc_datetime
	,unit_guid_uid
	,unit_name
	,city_guid_uid
	,city_name
	,full_order_id
	,full_order_roznica_guid_uid 
	,full_order_roznica_number
	,full_order_roznica_datetime
	,is_link_order 
	,total_sum_total 
	,source_stage_id 
	,source_stage_name 
	,order_stage_id 
	,order_stage_name 
	,order_sub_stage_id 
	,order_sub_stage_name 
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
	,client_id								-- V
	,client_name							-- V
	,client_guid_uid						-- V
	,order_phone							-- V
	,employee_id							-- V
	,employee_name							-- V
	,employee_guid_uid						-- V
	,is_mip									-- V
	,is_kitchen								-- V
	,mipik_status_id						-- V -- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
	,mipik_status_name						-- V
	,mipik_status_order						-- V
	,mipik_collection_id					-- V
	,mipik_collection_name 					-- V
	,mipik_collection_guid_uid 				-- V
	,mipik_assortiment_group_id 			-- V
	,mipik_assortiment_group_name 			-- V
	,mipik_assortiment_group_guid_uid 		-- V
	,mipik_customization_type_id 			-- V
	,mipik_customization_type_name 			-- V
	,mipik_customization_type_guid_uid 		-- V
	,mipik_trek_mik_name					-- V
	,mipik_trek_mik_guid_uid				-- V
	,max_data_perenosa 						-- V
	,is_oplachen_roznica 					-- V
	,is_po_prosbe_clienta_roznica 			-- V 
	,data_dostavki_dogovor_roznica 			-- V
	,data_dostavki_roznica 					-- V
	,data_otsechki 							-- V
	,data_komplekta 						-- V
	,data_celevaya 							-- V
	,kommentarii_roznica					-- V
	,norma_days_dostavki_iz_voronezha		-- V
)
SELECT
	cte.batch_id_dttm												AS batch_id_dttm							
	,cte.batch_id_str												AS batch_id_str							
	,cte.create_dttm												AS create_dttm
	,cte.date_id													AS date_id
	,cte.order_id													AS order_id
	,cte.order_roznica_guid_uid										AS order_roznica_guid_uid
	,cte.order_roznica_doc_num										AS order_roznica_doc_num
	,cte.order_roznica_doc_datetime									AS order_roznica_doc_datetime
	,cte.unit_guid_uid												AS unit_guid_uid
	,cte.unit_name													AS unit_name
	,cte.city_guid_uid												AS city_guid_uid
	,cte.city_name													AS city_name
	,cte.full_order_id												AS full_order_id
	,cte.full_order_roznica_guid_uid								AS full_order_roznica_guid_uid
	,cte.full_order_roznica_number									AS full_order_roznica_number
	,cte.full_order_roznica_datetime								AS full_order_roznica_datetime		
	,cte.is_link_order												AS is_link_order
	,cte.total_sum_total											AS total_sum_total
	,cte.source_stage_id											AS source_stage_id
	--,cte.source_stage_name											AS source_stage_name
	--,dictGet('rasp2.dict_source_stage', 'source_stage_name', cte.source_stage_id)			AS source_stage_name
	,''														AS source_stage_name
	,cte.order_stage_id												AS order_stage_id
	--,cte.order_stage_name											AS order_stage_name
	--,dictGet('rasp2.dict_order_stage', 'order_stage_name', cte.order_stage_id)				AS order_stage_name
	,''													AS order_stage_name
	,cte.order_sub_stage_id											AS order_sub_stage_id
	--,cte.order_sub_stage_name										AS order_sub_stage_name
	--,dictGet('rasp2.dict_order_sub_stage', 'order_sub_stage_name', cte.order_sub_stage_id)	AS order_sub_stage_name
	,'' 													AS order_sub_stage_name
	,cte.has_std1													AS has_std1
	,cte.is_deficit_std1											AS is_deficit_std1
	,cte.total_sum_std1												AS total_sum_std1
	,cte.has_std2													AS has_std2
	,cte.is_deficit_std2											AS is_deficit_std2
	,cte.total_sum_std2												AS total_sum_std2
	,cte.has_mip													AS has_mip
	,cte.is_deficit_mip												AS is_deficit_mip
	,cte.total_sum_mip												AS total_sum_mip
	,cte.has_kich													AS has_kich
	,cte.is_deficit_kich											AS is_deficit_kich
	,cte.total_sum_kich												AS total_sum_kich
	,cte.has_stor													AS has_stor
	,cte.is_deficit_stor											AS is_deficit_stor
	,cte.total_sum_stor												AS total_sum_stor
	,cte.has_matr													AS has_matr
	,cte.is_deficit_matr											AS is_deficit_matr
	,cte.total_sum_matr												AS total_sum_matr
	,cte.has_tech													AS has_tech
	,cte.is_deficit_tech											AS is_deficit_tech
	,cte.total_sum_tech												AS total_sum_tech
	,cte.has_other													AS has_other
	,cte.is_deficit_other											AS is_deficit_other
	,cte.total_sum_other											AS total_sum_other
	,cte.has_serv													AS has_serv
	,cte.total_sum_serv												AS total_sum_serv
	,cte.client_id													AS client_id							-- V
	,cte.client_name												AS client_name							-- V
	,cte.client_guid_uid											AS client_guid_uid						-- V
	,cte.order_phone												AS order_phone							-- V
	,cte.employee_id												AS employee_id							-- V
	,cte.employee_name												AS employee_name						-- V
	,cte.employee_guid_uid											AS employee_guid_uid					-- V
	,cte.is_mip														AS is_mip								-- V
	,cte.is_kitchen													AS is_kitchen							-- V
	,cte.mipik_status_id											AS mipik_status_id						-- V 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
	,cte.mipik_status_name											AS mipik_status_name					-- V
	,cte.mipik_status_order											AS mipik_status_order					-- V
	,cte.mipik_collection_id										AS mipik_collection_id					-- V
	,cte.mipik_collection_name										AS mipik_collection_name 				-- V
	,cte.mipik_collection_guid_uid									AS mipik_collection_guid_uid 			-- V
	,cte.mipik_assortiment_group_id									AS mipik_assortiment_group_id 			-- V
	,cte.mipik_assortiment_group_name								AS mipik_assortiment_group_name 		-- V
	,cte.mipik_assortiment_group_guid_uid							AS mipik_assortiment_group_guid_uid 	-- V
	,cte.mipik_customization_type_id								AS mipik_customization_type_id 			-- V
	,cte.mipik_customization_type_name								AS mipik_customization_type_name 		-- V
	,cte.mipik_customization_type_guid_uid							AS mipik_customization_type_guid_uid 	-- V
	,cte.mipik_trek_mik_name										AS mipik_trek_mik_name					-- V
	,cte.mipik_trek_mik_guid_uid									AS mipik_trek_mik_guid_uid				-- V
	,cte.max_data_perenosa											AS max_data_perenosa 					-- V
	,cte.is_oplachen_roznica										AS is_oplachen_roznica 					-- V
	,cte.is_po_prosbe_clienta_roznica								AS is_po_prosbe_clienta_roznica 		-- V 
	,cte.data_dostavki_dogovor_roznica								AS data_dostavki_dogovor_roznica 		-- V
	,cte.data_dostavki_roznica										AS data_dostavki_roznica 				-- V
	,cte.data_otsechki  											AS data_otsechki 						-- V
	,cte.data_komplekta												AS data_komplekta 						-- V
	,cte.data_celevaya												AS data_celevaya 						-- V
	,cte.kommentarii_roznica										AS kommentarii_roznica					-- V
	,cte.norma_days_dostavki_iz_voronezha							AS norma_days_dostavki_iz_voronezha		-- V
FROM
	cte
		
