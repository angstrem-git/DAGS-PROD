-- "db1_source": DB1_source
-- "db2": DB2
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
cte_pkt AS
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
		{{ params.db1_source }}.packet AS pkt
	WHERE
		-- Подразделеня-Розница -- unit_guid_OPN_str = unit_guid_CONS_str - для всех строк	
		pkt.unit_guid_OPN_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
		--pkt.unit_guid_OPN_str IN (SELECT unit_guid_str FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
		-- Не на страховой запас
		AND pkt.is_strahovoj_zapas_roznica = 0
		-- Не на выставку
		AND pkt.is_vystavka_roznica = 0	
		-- Не рекламации
		AND pkt.is_reklamacii = 0
		-- Не полиграфия
		AND pkt.is_poligrafiya = 0
		--AND pkt.date_id = (SELECT date_id FROM cte_dt LIMIT 1)		-- Заменить на pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND pkt.batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
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
cte_prd AS 
	(
	SELECT 
		bch.batch_id_dttm									AS batch_id_dttm
		,bch.batch_id_str									AS batch_id_str
		,bch.create_dttm									AS create_dttm
		,bch.date_id										AS date_id
		,rd.order_guid_uid									AS order_guid_uid
		,rd.order_id										AS order_id
		,rd.nomenclature_guid_uid 							AS nomenclature_guid_uid
		,rd.nomenclature_name								AS nomenclature_name
		,rd.nomenclature_property_guid_uid					AS nomenclature_property_guid_uid
		,rd.nomenclature_property_name						AS nomenclature_property_name
		,rd.order_count										AS order_count
		,rd.total_sum										AS total_sum
		,rd.avg_price										AS avg_price
	FROM 
		rasp2.open_orders_goods_history_rn AS rd		-- Заменить from_mssql.open_orders_goods_history_rn на from_mssql.vw_open_orders_goods_rn_today
		LEFT JOIN cte_dt AS bch
			ON rd.date_id = bch.date_id
	WHERE 
		rd.unit_guid_uid IN (SELECT toUUIDOrZero(unit_guid_str) FROM from_mssql.vw_unit_sale WHERE sales_direction_id = 2) 
		--AND rd.date_id = toDate('2026-05-15')				-- Заменить d.date_id = toDate('2026-05-15') на текущую дату !!!
		AND rd.date_id = (SELECT date_id FROM cte_dt LIMIT 1)
	)
,
cte_total AS
(
	SELECT 
		rd.batch_id_dttm								AS batch_id_dttm
		,rd.batch_id_str								AS batch_id_str
		,rd.create_dttm									AS create_dttm
		,rd.date_id										AS date_id
		,if(
			pk.order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,rd.order_guid_uid
			,pk.order_roznica_guid_uid
		)																				AS order_guid_uid 
		--COALESCE(pk.order_roznica_guid_uid, rd.order_guid_uid)							AS order_guid_uid 
		,rd.order_id																	AS order_id
		,if(
			pk.nomenclature_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,rd.nomenclature_guid_uid
			,pk.nomenclature_guid_uid
		)																				AS nomenclature_guid_uid
		--,COALESCE(pk.nomenclature_guid_uid, rd.nomenclature_guid_uid)					AS nomenclature_guid_uid
		,if(
			pk.nomenclature_name = ''
			,rd.nomenclature_name
			,pk.nomenclature_name
		)																				AS nomenclature_name
		--,COALESCE(pk.nomenclature_name, rd.nomenclature_name)							AS nomenclature_name
		,if(
			pk.nomenclature_property_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,rd.nomenclature_property_guid_uid
			,pk.nomenclature_property_guid_uid
		)																				AS nomenclature_property_guid_uid
		--,COALESCE(pk.nomenclature_property_guid_uid, rd.nomenclature_property_guid_uid)	AS nomenclature_property_guid_uid
		,if(
			pk.nomenclature_property_name = ''
			,rd.nomenclature_property_name
			,pk.nomenclature_property_name
		)																				AS nomenclature_property_name
		--,COALESCE(pk.nomenclature_property_name, rd.nomenclature_property_name)			AS nomenclature_property_name
		,pk.nomenclature_serie_guid_uid													AS nomenclature_serie_guid_uid
		,pk.nomenclature_serie_name														AS nomenclature_serie_name
		,pk.doc_zayavka_na_otgruzku_guid_uid											AS doc_zayavka_na_otgruzku_guid_uid
		,pk.row_number_zayavka_na_otgruzku												AS row_number_zayavka_na_otgruzku
		,pk.row_key_ochered																AS row_key_ochered
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
--,
--ct AS
--(
INSERT INTO {{ params.db2 }}.all_goods(
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
		,tt.nomenclature_serie_guid_uid					AS nomenclature_serie_guid_uid
		,tt.nomenclature_serie_name						AS nomenclature_serie_name
		,tt.doc_zayavka_na_otgruzku_guid_uid			AS doc_zayavka_na_otgruzku_guid_uid
		,tt.row_number_zayavka_na_otgruzku				AS row_number_zayavka_na_otgruzku
		,tt.row_key_ochered								AS row_key_ochered
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
		,art.type_id									AS type_id
		,art.type_name									AS type_name
		--,if(tt.kolichestvo_dolg >= 1, tt.kolichestvo_dolg, tt.order_count)	AS kolichestvo_total
		,if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg)	AS kolichestvo_total	
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 1																	-- СТАНДАРТ
				and tt.status_modul_create_date_short_name != 'Opt'									-- <> Стандарт-2
					, 1
					, 0
			) 											AS has_std1
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 1																	-- СТАНДАРТ
				and tt.status_modul_create_date_short_name != 'Opt'									-- <> Стандарт-2			
					, 1
					, 0
			)											AS is_deficit_std1
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 1																	-- СТАНДАРТ
				and status_modul_create_date_short_name = 'Opt'										-- == Стандарт-2
					, 1
					, 0
			) 											AS has_std2
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 1																	-- СТАНДАРТ
				and status_modul_create_date_short_name = 'Opt'										-- == Стандарт-2				
					, 1
					, 0
			)											AS is_deficit_std2	
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 3																	-- МИП
					, 1
					, 0
			) 											AS has_mip
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 3																	-- МИП
					, 1
					, 0
			)											AS is_deficit_mip		
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 4																	-- Кухни
					, 1
					, 0
			) 											AS has_kich
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 4																	-- Кухни				
					, 1
					, 0
			)											AS is_deficit_kich	
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 6																	-- Сторонняя
					, 1
					, 0
			) 											AS has_stor
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 6																	-- Сторонняя			
					, 1
					, 0
			)											AS is_deficit_stor
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 13																-- Матрасы
					, 1
					, 0
			) 											AS has_matr
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 13																-- Матрасы			
					, 1
					, 0
			)											AS is_deficit_matr
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 15																-- Бытовая техника
					, 1
					, 0
			) 											AS has_tech
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id = 15																-- Бытовая техника		
					, 1
					, 0
			)											AS is_deficit_tech
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id NOT IN (1, 3, 4, 6, 13, 14, 15)										-- Прочие товары
					, 1
					, 0
			) 											AS has_oth
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and (tt.is_iz_proizvodstva_rasp > 0 or tt.is_s_uchetom_vypuska_rasp > 0)
				and art.type_id NOT IN (1, 3, 4, 6, 13, 14, 15)										-- Прочие товары		
					, 1
					, 0
			)											AS is_deficit_oth		
		,if(
				if(tt.order_count > 0, tt.order_count, tt.kolichestvo_dolg) * tt.avg_price > 0
				and art.type_id = 14																-- Услуги
					, 1
					, 0
			) 											AS has_serv	
FROM  
	rasp2.art AS art
	RIGHT JOIN cte_total AS tt
		ON art.nomenclature_guid_uuid = tt.nomenclature_guid_uid
		AND art.nomenclature_property_guid_uuid = tt.nomenclature_property_guid_uid
