-- "db1": DB1
-- "db2": DB2
-- "days_back": 0, 1 или 2
-- Передавать из Airflow два параметра: (1) cte_current_snapshot_date (дата открытых заказов), (2) cte_dt_rasp (ближайшее распределение)
WITH 
--	toDate('2026-08-01') AS cte_current_snapshot_date		-- Текущая дата (в том числе суббота и воскресенье, когда нет распределения)
--	,
--	1 AS days_back
--,
-- Ближайшее распределение (для субботы и воскресенья - это распределение понедельника)
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
		) 													AS current_date 	-- Текущий день (для которого обрабатываем данные)
		--,subtractDays(toDate(datetime_id), days_back) 		AS current_date 	-- Текущий день (для которого обрабатываем данные)
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-08-03')
	WHERE batch_id_str = '{{ ti.xcom_pull(task_ids="tg_work_day_01_31_id.r_001_set_xcom_id", key="batch_id_str") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
-- Избавляемся от пакетов
cte_pkt_series AS
(
	SELECT 
		toUUIDOrZero(pkt.order_roznica_guid_str)		AS order_roznica_guid_uid
		,pkt.nomenclature_guid_OPN_uid					AS nomenclature_guid_uid
		,pkt.nomenclature_name							AS nomenclature_name
		,pkt.nomenclature_property_guid_OPN_uid			AS nomenclature_property_guid_uid
		,pkt.nomenclature_property_name					AS nomenclature_property_name
		,pkt.nomenclature_serie_guid_OPN_uid			AS nomenclature_serie_guid_uid
		,pkt.nomenclature_serie_name					AS nomenclature_serie_name
		,pkt.doc_zayavka_na_otgruzku_rasp_guid_1C_uid	AS doc_zayavka_na_otgruzku_guid_uid
		,pkt.row_number_zayavka_na_otgruzku				AS row_number_zayavka_na_otgruzku
		,pkt.row_key_ochered							AS row_key_ochered
		,min(pkt.kolichestvo_dolg)						AS kolichestvo_dolg 			-- Количество штук открытых заказов
		,min(pkt.kolichestvo_v_rezerve)					AS kolichestvo_v_rezerve		-- Резерв
		,min(pkt.kolichestvo_podtverzhdeno)				AS kolichestvo_podtverzhdeno	-- Документ.ЗаявкаНаОтгрузку.Товары: Количество + ИзРегиона + ИзТранзита
		,min(pkt.kolichestvo_zayavka_na_otgruzku)		AS kolichestvo_zayavka_na_otgruzku 		--	Документ.ЗаявкаНаОтгрузку.Товары.Количество
		,min(pkt.kolichestvo_iz_regiona_zayavka_na_otgruzku)	AS kolichestvo_iz_regiona_zayavka_na_otgruzku 	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзРегиона
		,min(pkt.kolichestvo_iz_tranzita_zayavka_na_otgruzku)	AS kolichestvo_iz_tranzita_zayavka_na_otgruzku 	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзТранзита
		,min(pkt.kolichestvo_raspredeleno_modul_rasp)	AS kolichestvo_raspredeleno_modul_rasp 	-- РаспределениеПодЗаказТовары.Количество (Только распределенные товары (модули) (для которых нашлась продукция)
		,min(pkt.kolichestvo_iz_vypuska_rasp)			AS kolichestvo_iz_vypuska_rasp 	-- Признак распределения из выпуска
		,min(pkt.kolichestvo_iz_regionov_rasp)			AS kolichestvo_iz_regionov_rasp	-- РаспределениеПодЗаказТовары.СоСкладаРегиона + РаспределениеПодЗаказТовары.СоСкладаТранзит
		,min(pkt.kolichestvo_so_sklada_regiona_rasp)	AS kolichestvo_so_sklada_regiona_rasp 	-- РаспределениеПодЗаказТовары.СоСкладаРегиона
		,min(pkt.kolichestvo_so_sklada_transit_rasp)	AS kolichestvo_so_sklada_transit_rasp 	-- РаспределениеПодЗаказТовары.СоСкладаТранзит
		,min(pkt.kolichestvo_ochered_rasp)				AS kolichestvo_ochered_rasp 	-- РаспределениеПодЗаказОчередь.Количество (Все товары, участвующие в распределении (в том числе те, на которые не хватило продукции))
		,min(pkt.kolichestvo_v_rezerve_rasp)			AS kolichestvo_v_rezerve_rasp 	-- РаспределениеПодЗаказОчередь.ВРезерве
		,min(pkt.kolichestvo_v_zayavkah_rasp)			AS kolichestvo_v_zayavkah_rasp 	-- РаспределениеПодЗаказОчередь.ВЗаявках
		,min(pkt.kolichestvo_kompleltno_rasp)			AS kolichestvo_kompleltno_rasp 	-- РаспределениеПодЗаказОчередь.Комплектно
		,if(max(pkt.is_s_uchetom_vypuska_rasp) > 0, 1, 0)		AS is_s_uchetom_vypuska_rasp	-- РаспределениеПодЗаказТовары.СУчетомВыпуска	
		,if(max(pkt.is_iz_proizvodstva_rasp) > 0, 1, 0)	AS is_iz_proizvodstva_rasp 		-- Дефицит пакета
		,if(max(pkt.is_iz_reserva_rasp) > 0, 1, 0)		AS is_iz_reserva_rasp 			-- Резерв, но нет Заявки на отгрузку (резерв поставлен вручную)	
		,if(max(pkt.is_iz_regiona_rasp) > 0, 1, 0)		AS is_iz_regiona_rasp			-- Из свободного остатка региона
		,if(max(pkt.is_iz_postupleniya_rasp) > 0, 1, 0)	AS is_iz_postupleniya_rasp		-- Из Заказа поставщику
		,if(max(pkt.is_iz_plana_rasp) > 0, 1, 0)		AS is_iz_plana_rasp				-- Из регистра Производственный план (до 25.09.2025 из документа Мастер-план)	
		,if(max(pkt.is_iz_gorizonta_rasp) > 0, 1, 0)	AS is_iz_gorizonta_rasp			-- Из срока поставки (нет ни Заказов поставщику, ни Задания на производства, ни Плана производства)
		,if(max(pkt.is_iz_gorizonta) > 0, 1, 0)			AS is_iz_gorizonta				-- Из срока поставки - другим способом (запрос Кости)		
		,max(pkt.data_reserva_rasp)						AS data_reserva_rasp			-- Дата Выпуска/Поставки дефицитного модуля
		,any(pkt.status_modul_create_date_number)		AS status_modul_create_date_number
		,any(pkt.status_modul_create_date_short_name)	AS status_modul_create_date_short_name
		,any(pkt.is_ispolzuemaya_create_date)			AS is_ispolzuemaya_create_date
	FROM 	
	--	rasp1_v02.packet AS pkt
		{{ params.db1 }}.packet AS pkt
	WHERE
		-- Подразделеня-Розница -- unit_guid_OPN_str = unit_guid_CONS_str - для всех строк	
		pkt.unit_guid_OPN_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
		-- Не на страховой запас
		AND pkt.is_strahovoj_zapas_roznica = 0
		-- Не на выставку
		AND pkt.is_vystavka_roznica = 0	
		-- Не рекламации
		AND pkt.is_reklamacii = 0
		-- Не полиграфия
		AND pkt.is_poligrafiya = 0
		AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
	GROUP BY  
		toUUIDOrZero(pkt.order_roznica_guid_str)		
		,pkt.nomenclature_guid_OPN_uid	
		,pkt.nomenclature_name
		,pkt.nomenclature_property_guid_OPN_uid		
		,pkt.nomenclature_property_name
		,pkt.nomenclature_serie_guid_OPN_uid	
		,pkt.nomenclature_serie_name
		,pkt.doc_zayavka_na_otgruzku_rasp_guid_1C_uid	
		,pkt.row_number_zayavka_na_otgruzku				
		,pkt.row_key_ochered
)
,
-- mg-07.08.2026 - Избавляемся от серий
cte_pkt AS 
(
	SELECT 
		pkt.order_roznica_guid_uid						AS order_roznica_guid_uid
		,pkt.nomenclature_guid_uid						AS nomenclature_guid_uid
		,pkt.nomenclature_name							AS nomenclature_name
		,pkt.nomenclature_property_guid_uid				AS nomenclature_property_guid_uid
		,pkt.nomenclature_property_name					AS nomenclature_property_name
		,sum(pkt.kolichestvo_dolg)						AS kolichestvo_dolg 			-- Количество штук открытых заказов
		,sum(pkt.kolichestvo_v_rezerve)					AS kolichestvo_v_rezerve		-- Резерв
		,sum(pkt.kolichestvo_podtverzhdeno)				AS kolichestvo_podtverzhdeno	-- Документ.ЗаявкаНаОтгрузку.Товары: Количество + ИзРегиона + ИзТранзита
		,sum(pkt.kolichestvo_zayavka_na_otgruzku)		AS kolichestvo_zayavka_na_otgruzku 		--	Документ.ЗаявкаНаОтгрузку.Товары.Количество
		,sum(pkt.kolichestvo_iz_regiona_zayavka_na_otgruzku)	AS kolichestvo_iz_regiona_zayavka_na_otgruzku 	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзРегиона
		,sum(pkt.kolichestvo_iz_tranzita_zayavka_na_otgruzku)	AS kolichestvo_iz_tranzita_zayavka_na_otgruzku 	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзТранзита
		,sum(pkt.kolichestvo_raspredeleno_modul_rasp)	AS kolichestvo_raspredeleno_modul_rasp 	-- РаспределениеПодЗаказТовары.Количество (Только распределенные товары (модули) (для которых нашлась продукция)
		,sum(pkt.kolichestvo_iz_vypuska_rasp)			AS kolichestvo_iz_vypuska_rasp 	-- Признак распределения из выпуска
		,sum(pkt.kolichestvo_iz_regionov_rasp)			AS kolichestvo_iz_regionov_rasp	-- РаспределениеПодЗаказТовары.СоСкладаРегиона + РаспределениеПодЗаказТовары.СоСкладаТранзит
		,sum(pkt.kolichestvo_so_sklada_regiona_rasp)	AS kolichestvo_so_sklada_regiona_rasp 	-- РаспределениеПодЗаказТовары.СоСкладаРегиона
		,sum(pkt.kolichestvo_so_sklada_transit_rasp)	AS kolichestvo_so_sklada_transit_rasp 	-- РаспределениеПодЗаказТовары.СоСкладаТранзит
		,sum(pkt.kolichestvo_ochered_rasp)				AS kolichestvo_ochered_rasp 	-- РаспределениеПодЗаказОчередь.Количество (Все товары, участвующие в распределении (в том числе те, на которые не хватило продукции))
		,sum(pkt.kolichestvo_v_rezerve_rasp)			AS kolichestvo_v_rezerve_rasp 	-- РаспределениеПодЗаказОчередь.ВРезерве
		,sum(pkt.kolichestvo_v_zayavkah_rasp)			AS kolichestvo_v_zayavkah_rasp 	-- РаспределениеПодЗаказОчередь.ВЗаявках
		,sum(pkt.kolichestvo_kompleltno_rasp)			AS kolichestvo_kompleltno_rasp 	-- РаспределениеПодЗаказОчередь.Комплектно
		,max(pkt.is_s_uchetom_vypuska_rasp)				AS is_s_uchetom_vypuska_rasp	-- РаспределениеПодЗаказТовары.СУчетомВыпуска	
		,max(pkt.is_iz_proizvodstva_rasp)				AS is_iz_proizvodstva_rasp 		-- Дефицит пакета
		,max(pkt.is_iz_reserva_rasp)					AS is_iz_reserva_rasp 			-- Резерв, но нет Заявки на отгрузку (резерв поставлен вручную)	
		,max(pkt.is_iz_regiona_rasp)					AS is_iz_regiona_rasp			-- Из свободного остатка региона
		,max(pkt.is_iz_postupleniya_rasp)				AS is_iz_postupleniya_rasp		-- Из Заказа поставщику
		,max(pkt.is_iz_plana_rasp)						AS is_iz_plana_rasp				-- Из регистра Производственный план (до 25.09.2025 из документа Мастер-план)	
		,max(pkt.is_iz_gorizonta_rasp)					AS is_iz_gorizonta_rasp			-- Из срока поставки (нет ни Заказов поставщику, ни Задания на производства, ни Плана производства)
		,max(pkt.is_iz_gorizonta)						AS is_iz_gorizonta				-- Из срока поставки - другим способом (запрос Кости)		
		,max(pkt.data_reserva_rasp)						AS data_reserva_rasp			-- Дата Выпуска/Поставки дефицитного модуля
		,any(pkt.status_modul_create_date_number)		AS status_modul_create_date_number
		,any(pkt.status_modul_create_date_short_name)	AS status_modul_create_date_short_name
		,any(pkt.is_ispolzuemaya_create_date)			AS is_ispolzuemaya_create_date
	FROM 
		cte_pkt_series AS pkt
	GROUP BY
		pkt.order_roznica_guid_uid						
		,pkt.nomenclature_guid_uid						
		,pkt.nomenclature_name							
		,pkt.nomenclature_property_guid_uid				
		,pkt.nomenclature_property_name					
)
,
cte_prd AS 
(
	SELECT 
		bch.batch_id_dttm									AS batch_id_dttm
		,bch.batch_id_str									AS batch_id_str
		,rd.date_id											AS date_id
		,rd.order_guid_uid									AS order_guid_uid
		,rd.order_id										AS order_id
		,rd.nomenclature_guid_uid 							AS nomenclature_guid_uid
		,rd.nomenclature_name								AS nomenclature_name
		,rd.nomenclature_property_guid_uid					AS nomenclature_property_guid_uid
		,rd.nomenclature_property_name						AS nomenclature_property_name
		,rd.type_id											AS type_id
		,rd.order_count										AS order_count
		,rd.total_sum										AS total_sum
		,rd.avg_price										AS avg_price
	FROM 
		rasp2.open_orders_goods_history_rn AS rd	
		LEFT JOIN cte_dt_rasp AS bch
			ON rd.date_id = bch.date_id
	WHERE 
		rd.unit_guid_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
		AND rd.date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--AND rd.date_id = cte_current_snapshot_date
)
,
cte_total AS
(
	SELECT 
		rd.batch_id_dttm								AS batch_id_dttm
		,rd.batch_id_str								AS batch_id_str
		,rd.date_id										AS date_id
		,if(
			pk.order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,rd.order_guid_uid
			,pk.order_roznica_guid_uid
		)																				AS order_guid_uid 
		,rd.order_id																	AS order_id
		,if(
			pk.nomenclature_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,rd.nomenclature_guid_uid
			,pk.nomenclature_guid_uid
		)																				AS nomenclature_guid_uid
		,if(
			pk.nomenclature_name = ''
			,rd.nomenclature_name
			,pk.nomenclature_name
		)																				AS nomenclature_name
		,if(
			pk.nomenclature_property_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,rd.nomenclature_property_guid_uid
			,pk.nomenclature_property_guid_uid
		)																				AS nomenclature_property_guid_uid
		,if(
			pk.nomenclature_property_name = ''
			,rd.nomenclature_property_name
			,pk.nomenclature_property_name
		)																				AS nomenclature_property_name
--		,pk.nomenclature_serie_guid_uid													AS nomenclature_serie_guid_uid
--		,pk.nomenclature_serie_name														AS nomenclature_serie_name
--		,pk.doc_zayavka_na_otgruzku_guid_uid											AS doc_zayavka_na_otgruzku_guid_uid
--		,pk.row_number_zayavka_na_otgruzku												AS row_number_zayavka_na_otgruzku
--		,pk.row_key_ochered																AS row_key_ochered
		,pk.kolichestvo_dolg 							AS kolichestvo_dolg								-- Количество штук открытых заказов
		,pk.kolichestvo_v_rezerve						AS kolichestvo_v_rezerve						-- Резерв
		,pk.kolichestvo_podtverzhdeno					AS kolichestvo_podtverzhdeno					-- Документ.ЗаявкаНаОтгрузку.Товары: Количество + ИзРегиона + ИзТранзита
		,pk.kolichestvo_zayavka_na_otgruzku 			AS kolichestvo_zayavka_na_otgruzku				-- Документ.ЗаявкаНаОтгрузку.Товары.Количество
		,pk.kolichestvo_iz_regiona_zayavka_na_otgruzku 	AS kolichestvo_iz_regiona_zayavka_na_otgruzku	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзРегиона
		,pk.kolichestvo_iz_tranzita_zayavka_na_otgruzku	AS kolichestvo_iz_tranzita_zayavka_na_otgruzku 	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзТранзита
		,pk.kolichestvo_raspredeleno_modul_rasp 		AS kolichestvo_raspredeleno_modul_rasp			-- РаспределениеПодЗаказТовары.Количество (Только распределенные товары (модули) (для которых нашлась продукция)
		,pk.kolichestvo_iz_vypuska_rasp 				AS kolichestvo_iz_vypuska_rasp					-- Признак распределения из выпуска
		,pk.kolichestvo_iz_regionov_rasp				AS kolichestvo_iz_regionov_rasp					-- РаспределениеПодЗаказТовары.СоСкладаРегиона + РаспределениеПодЗаказТовары.СоСкладаТранзит
		,pk.kolichestvo_so_sklada_regiona_rasp			AS kolichestvo_so_sklada_regiona_rasp			-- РаспределениеПодЗаказТовары.СоСкладаРегиона
		,pk.kolichestvo_so_sklada_transit_rasp 			AS kolichestvo_so_sklada_transit_rasp			-- РаспределениеПодЗаказТовары.СоСкладаТранзит
		,pk.kolichestvo_ochered_rasp 					AS kolichestvo_ochered_rasp						-- РаспределениеПодЗаказОчередь.Количество (Все товары, участвующие в распределении (в том числе те, на которые не хватило продукции))
		,pk.kolichestvo_v_rezerve_rasp 					AS kolichestvo_v_rezerve_rasp					-- РаспределениеПодЗаказОчередь.ВРезерве
		,pk.kolichestvo_v_zayavkah_rasp 				AS kolichestvo_v_zayavkah_rasp					-- РаспределениеПодЗаказОчередь.ВЗаявках
		,pk.kolichestvo_kompleltno_rasp 				AS kolichestvo_kompleltno_rasp					-- РаспределениеПодЗаказОчередь.Комплектно
		,pk.is_s_uchetom_vypuska_rasp					AS is_s_uchetom_vypuska_rasp	-- РаспределениеПодЗаказТовары.СУчетомВыпуска
		,pk.is_iz_proizvodstva_rasp						AS is_iz_proizvodstva_rasp 		-- Дефицит пакета
		,pk.is_iz_reserva_rasp							AS is_iz_reserva_rasp 			-- Резерв, но нет Заявки на отгрузку (резерв поставлен вручную)	
		,pk.is_iz_regiona_rasp							AS is_iz_regiona_rasp			-- Из свободного остатка региона
		,pk.is_iz_postupleniya_rasp						AS is_iz_postupleniya_rasp		-- Из Заказа поставщику
		,pk.is_iz_plana_rasp							AS is_iz_plana_rasp				-- Из регистра Производственный план (до 25.09.2025 из документа Мастер-план)	
		,pk.is_iz_gorizonta_rasp						AS is_iz_gorizonta_rasp			-- Из срока поставки (нет ни Заказов поставщику, ни Задания на производства, ни Плана производства)
		,pk.is_iz_gorizonta								AS is_iz_gorizonta				-- Из срока поставки - другим способом (запрос Кости)
		,pk.data_reserva_rasp							AS data_reserva_rasp			-- Дата Выпуска/Поставки дефицитного модуля
		,pk.status_modul_create_date_number				AS status_modul_create_date_number
		,pk.status_modul_create_date_short_name			AS status_modul_create_date_short_name
		,pk.is_ispolzuemaya_create_date					AS is_ispolzuemaya_create_date
		,rd.type_id										AS type_id
		,rd.order_count									AS order_count
		,rd.total_sum									AS total_sum
		,rd.avg_price									AS avg_price
	FROM  
		cte_prd AS rd 
		LEFT JOIN cte_pkt AS pk
			ON pk.order_roznica_guid_uid = rd.order_guid_uid
			AND pk.nomenclature_guid_uid = rd.nomenclature_guid_uid
			--AND pk.nomenclature_property_guid_uid = rd.nomenclature_property_guid_uid
			-- Новое условие на случай, когда в MS SQL Server не выгружаются характеристики
			AND (
					pk.nomenclature_property_guid_uid = rd.nomenclature_property_guid_uid 
					OR rd.nomenclature_property_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				)
)
INSERT INTO {{ params.db2 }}.all_goods(
--INSERT INTO rasp2_v02.all_goods(
		batch_id_dttm
		,batch_id_str
		,create_dttm
		,date_id 
		,order_id
		,order_guid_uid 
		,nomenclature_guid_uid 
		,nomenclature_name 
		,nomenclature_property_guid_uid 
		,nomenclature_property_name 
		,nomenclature_serie_guid_uid 				
		,nomenclature_serie_name 					
		,doc_zayavka_na_otgruzku_guid_uid 		
		,row_number_zayavka_na_otgruzku 			
		,row_key_ochered 							
		,kolichestvo_dolg 
		,kolichestvo_v_rezerve 
		,kolichestvo_podtverzhdeno 
		,kolichestvo_zayavka_na_otgruzku 
		,kolichestvo_iz_regiona_zayavka_na_otgruzku 
		,kolichestvo_iz_tranzita_zayavka_na_otgruzku 
		,kolichestvo_raspredeleno_modul_rasp 
		,kolichestvo_iz_vypuska_rasp 
		,kolichestvo_iz_regionov_rasp 
		,kolichestvo_so_sklada_regiona_rasp 
		,kolichestvo_so_sklada_transit_rasp	
		,kolichestvo_ochered_rasp 
		,kolichestvo_v_rezerve_rasp	
		,kolichestvo_v_zayavkah_rasp 
		,kolichestvo_kompleltno_rasp 
		,is_s_uchetom_vypuska_rasp
		,is_iz_proizvodstva_rasp 
		,is_iz_reserva_rasp								
		,is_iz_regiona_rasp							
		,is_iz_postupleniya_rasp						
		,is_iz_plana_rasp								
		,is_iz_gorizonta_rasp						
		,is_iz_gorizonta	
		,data_reserva_rasp
		,status_modul_create_date_number
		,status_modul_create_date_short_name
		,is_ispolzuemaya_create_date
		,open_order_count 
		,open_order_total_sum 
		,avg_price 
		,type_id 
		,type_name 
		,kolichestvo_total 
		,has_std1
		,is_deficit_std1		
		,has_std2
		,is_deficit_std2
		,has_mip
		,is_deficit_mip	
		,has_kich
		,is_deficit_kich
		,has_stor
		,is_deficit_stor
		,has_matr
		,is_deficit_matr	
		,has_tech
		,is_deficit_tech		
		,has_other
		,is_deficit_other
		,has_serv
)
SELECT
		tt.batch_id_dttm								AS batch_id_dttm
		,tt.batch_id_str								AS batch_id_str
		,now('Europe/Moscow')							AS create_dttm
		,tt.date_id										AS date_id
		,tt.order_id									AS order_id
		,tt.order_guid_uid								AS order_guid_uid 
		,tt.nomenclature_guid_uid						AS nomenclature_guid_uid
		,tt.nomenclature_name							AS nomenclature_name
		,tt.nomenclature_property_guid_uid				AS nomenclature_property_guid_uid
		,tt.nomenclature_property_name					AS nomenclature_property_name
--		,tt.nomenclature_serie_guid_uid					AS nomenclature_serie_guid_uid			-- mg-07.08.2026 - Убрать
--		,tt.nomenclature_serie_name						AS nomenclature_serie_name				-- mg-07.08.2026 - Убрать
--		,tt.doc_zayavka_na_otgruzku_guid_uid			AS doc_zayavka_na_otgruzku_guid_uid		-- mg-07.08.2026 - Убрать
--		,tt.row_number_zayavka_na_otgruzku				AS row_number_zayavka_na_otgruzku		-- mg-07.08.2026 - Убрать
--		,tt.row_key_ochered								AS row_key_ochered						-- mg-07.08.2026 - Убрать
		,toUUID('00000000-0000-0000-0000-000000000000') AS nomenclature_serie_guid_uid
		,''												AS nomenclature_serie_name
		,toUUID('00000000-0000-0000-0000-000000000000')	AS doc_zayavka_na_otgruzku_guid_uid
		,0												AS row_number_zayavka_na_otgruzku
		,0												AS row_key_ochered
		,tt.kolichestvo_dolg 							AS kolichestvo_dolg								-- Количество штук открытых заказов
		,tt.kolichestvo_v_rezerve						AS kolichestvo_v_rezerve						-- Резерв
		,tt.kolichestvo_podtverzhdeno					AS kolichestvo_podtverzhdeno					-- Документ.ЗаявкаНаОтгрузку.Товары: Количество + ИзРегиона + ИзТранзита
		,tt.kolichestvo_zayavka_na_otgruzku 			AS kolichestvo_zayavka_na_otgruzku				-- Документ.ЗаявкаНаОтгрузку.Товары.Количество
		,tt.kolichestvo_iz_regiona_zayavka_na_otgruzku 	AS kolichestvo_iz_regiona_zayavka_na_otgruzku	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзРегиона
		,tt.kolichestvo_iz_tranzita_zayavka_na_otgruzku	AS kolichestvo_iz_tranzita_zayavka_na_otgruzku 	-- Документ.ЗаявкаНаОтгрузку.Товары.ИзТранзита
		,tt.kolichestvo_raspredeleno_modul_rasp 		AS kolichestvo_raspredeleno_modul_rasp			-- РаспределениеПодЗаказТовары.Количество (Только распределенные товары (модули) (для которых нашлась продукция)
		,tt.kolichestvo_iz_vypuska_rasp 				AS kolichestvo_iz_vypuska_rasp					-- Признак распределения из выпуска
		,tt.kolichestvo_iz_regionov_rasp				AS kolichestvo_iz_regionov_rasp					-- РаспределениеПодЗаказТовары.СоСкладаРегиона + РаспределениеПодЗаказТовары.СоСкладаТранзит
		,tt.kolichestvo_so_sklada_regiona_rasp			AS kolichestvo_so_sklada_regiona_rasp			-- РаспределениеПодЗаказТовары.СоСкладаРегиона
		,tt.kolichestvo_so_sklada_transit_rasp 			AS kolichestvo_so_sklada_transit_rasp			-- РаспределениеПодЗаказТовары.СоСкладаТранзит
		,tt.kolichestvo_ochered_rasp 					AS kolichestvo_ochered_rasp						-- РаспределениеПодЗаказОчередь.Количество (Все товары, участвующие в распределении (в том числе те, на которые не хватило продукции))
		,tt.kolichestvo_v_rezerve_rasp 					AS kolichestvo_v_rezerve_rasp					-- РаспределениеПодЗаказОчередь.ВРезерве
		,tt.kolichestvo_v_zayavkah_rasp 				AS kolichestvo_v_zayavkah_rasp					-- РаспределениеПодЗаказОчередь.ВЗаявках
		,tt.kolichestvo_kompleltno_rasp 				AS kolichestvo_kompleltno_rasp					-- РаспределениеПодЗаказОчередь.Комплектно
		,tt.is_s_uchetom_vypuska_rasp					AS is_s_uchetom_vypuska_rasp	-- РаспределениеПодЗаказТовары.СУчетомВыпуска
		,tt.is_iz_proizvodstva_rasp						AS is_iz_proizvodstva_rasp 		-- Дефицит пакета
		,tt.is_iz_reserva_rasp							AS is_iz_reserva_rasp 			-- Резерв, но нет Заявки на отгрузку (резерв поставлен вручную)	
		,tt.is_iz_regiona_rasp							AS is_iz_regiona_rasp			-- Из свободного остатка региона
		,tt.is_iz_postupleniya_rasp						AS is_iz_postupleniya_rasp		-- Из Заказа поставщику
		,tt.is_iz_plana_rasp							AS is_iz_plana_rasp				-- Из регистра Производственный план (до 25.09.2025 из документа Мастер-план)	
		,tt.is_iz_gorizonta_rasp						AS is_iz_gorizonta_rasp			-- Из срока поставки (нет ни Заказов поставщику, ни Задания на производства, ни Плана производства)
		,tt.is_iz_gorizonta								AS is_iz_gorizonta				-- Из срока поставки - другим способом (запрос Кости)
		,tt.data_reserva_rasp							AS data_reserva_rasp			-- Дата Выпуска/Поставки дефицитного модуля
		,tt.status_modul_create_date_number				AS status_modul_create_date_number
		,tt.status_modul_create_date_short_name			AS status_modul_create_date_short_name
		,tt.is_ispolzuemaya_create_date					AS is_ispolzuemaya_create_date
		,tt.order_count									AS open_order_count
		,tt.total_sum									AS open_order_total_sum
		,tt.avg_price									AS avg_price
		,tt.type_id										AS type_id
		,dictGetOrDefault('from_mssql.dict_type', 'type_name', tt.type_id, '')	AS type_name
		,if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg)			AS kolichestvo_total	
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 1																	-- СТАНДАРТ
				and tt.status_modul_create_date_short_name != 'Opt'									-- <> Стандарт-2
					, 1
					, 0
			) 											AS has_std1
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 1																	-- СТАНДАРТ
				and tt.status_modul_create_date_short_name != 'Opt'									-- <> Стандарт-2			
					, 1
					, 0
			)											AS is_deficit_std1
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 1																	-- СТАНДАРТ
				and status_modul_create_date_short_name = 'Opt'										-- == Стандарт-2
					, 1
					, 0
			) 											AS has_std2
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 1																	-- СТАНДАРТ
				and status_modul_create_date_short_name = 'Opt'										-- == Стандарт-2				
					, 1
					, 0
			)											AS is_deficit_std2	
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 3																	-- МИП
					, 1
					, 0
			) 											AS has_mip
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 3																	-- МИП
					, 1
					, 0
			)											AS is_deficit_mip		
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 4																	-- Кухни
					, 1
					, 0
			) 											AS has_kich
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 4																	-- Кухни				
					, 1
					, 0
			)											AS is_deficit_kich	
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 6																	-- Сторонняя
					, 1
					, 0
			) 											AS has_stor
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 6																	-- Сторонняя			
					, 1
					, 0
			)											AS is_deficit_stor
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 13																-- Матрасы
					, 1
					, 0
			) 											AS has_matr
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 13																-- Матрасы			
					, 1
					, 0
			)											AS is_deficit_matr
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 15																-- Бытовая техника
					, 1
					, 0
			) 											AS has_tech
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id = 15																-- Бытовая техника		
					, 1
					, 0
			)											AS is_deficit_tech
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id NOT IN (1, 3, 4, 6, 13, 14, 15)										-- Прочие товары
					, 1
					, 0
			) 											AS has_oth
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and tt.type_id NOT IN (1, 3, 4, 6, 13, 14, 15)										-- Прочие товары		
					, 1
					, 0
			)											AS is_deficit_oth		
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and tt.type_id = 14																-- Услуги
					, 1
					, 0
			) 											AS has_serv	
FROM  
	cte_total AS tt
