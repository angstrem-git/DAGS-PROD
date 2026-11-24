-- v02_070_insert_goods_in_order_stage.sql
-- {{ params.db1 }}: "db1": DB1
-- {{ params.db2 }}: "db2": DB2
-- {{ params.db3 }}: "db3": DB3
-- {{ params.days_back }}: "days_back": 0, 1 или 2
-- Передавать из Airflow два параметра: (1) batch_id_str (id распределения), (2) days_back (сдвиг дней)
WITH 
--	toDate('2026-09-28') AS temp_current_snapshot_date		-- Текущая дата 
--	,
--	0 AS temp_days_back
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
		--,subtractDays(toDate(datetime_id), temp_days_back) 	AS current_date 	-- Текущий день (для которого обрабатываем данные)
		,subtractDays(
			toDate(datetime_id),
			{{ params.days_back }} + 1
		) 													AS previous_date		-- Вчерашний день
		--,subtractDays(toDate(datetime_id), temp_days_back + 1) 	AS previous_date		-- Вчерашний день
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = temp_current_snapshot_date
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
-- ГЛАВНАЯ ТАБЛИЦА: Номенклатуры открытых заказов (в том числе Услуги) на Сегодня и завершенных товаров заказов на Вчера
cte_goods_cur AS				-- 26 156 строк ('2026-09-28')
(
	-- Открытые заказы на сегодня
	SELECT 
		gd.order_id																	AS order_id
		,gd.art_id																	AS art_id
		,argMax(gd.batch_id_dttm,                    			gd.create_dttm)		AS batch_id_dttm
		,argMax(gd.batch_id_str,                    			gd.create_dttm)		AS batch_id_str
		,argMax(gd.date_id,                    					gd.create_dttm)		AS date_id
		,argMax(gd.order_guid_uid,                    			gd.create_dttm)		AS order_guid_uid		
		,argMax(gd.nomenclature_id,                    			gd.create_dttm)		AS nomenclature_id 
		,argMax(gd.nomenclature_guid_uid,              			gd.create_dttm)		AS nomenclature_guid_uid
		,argMax(gd.nomenclature_name,                  			gd.create_dttm)		AS nomenclature_name
		,argMax(gd.nomenclature_property_id,            		gd.create_dttm)		AS nomenclature_property_id
		,argMax(gd.nomenclature_property_guid_uid,      		gd.create_dttm)		AS nomenclature_property_guid_uid
		,argMax(gd.nomenclature_property_name,          		gd.create_dttm)		AS nomenclature_property_name
		,argMax(gd.type_id,                    					gd.create_dttm)		AS type_id
		,argMax(gd.type_name,                    				gd.create_dttm)		AS type_name
		,argMax(gd.is_ispolzuemaya_create_date,         		gd.create_dttm)		AS module_is_ispolzuemaya_create_date
		,argMax(gd.status_modul_create_date_number,     		gd.create_dttm)		AS module_status_create_date_number
		,argMax(gd.status_modul_create_date_short_name, 		gd.create_dttm)		AS module_status_create_date_short_name
		,multiIf(
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'Act', 'Действующий', 
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'Un',  'Снят с производства',
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'New', 'Новый продукт',
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'Dev', 'Разрабатывается',
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'Reg', 'Снимаемый продукт',
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'Set', 'Готов к вводу',
			argMax(gd.status_modul_create_date_short_name, gd.create_dttm) = 'Opt', 'Стандарт-2',
			''
		) 																			AS module_status_create_date_name
		,argMax(gd.has_std1,                    				gd.create_dttm)		AS module_has_std1
		,argMax(gd.is_deficit_std1,                    			gd.create_dttm)		AS module_is_deficit_std1
		,argMax(gd.has_std2,                    				gd.create_dttm)		AS module_has_std2
		,argMax(gd.is_deficit_std2,                    			gd.create_dttm)		AS module_is_deficit_std2
		,argMax(gd.has_mip,                    					gd.create_dttm)		AS module_has_mip
		,argMax(gd.is_deficit_mip,                    			gd.create_dttm)		AS module_is_deficit_mip
		,argMax(gd.has_kich,                    				gd.create_dttm)		AS module_has_kich
		,argMax(gd.is_deficit_kich,                    			gd.create_dttm)		AS module_is_deficit_kich
		,argMax(gd.has_stor,                    				gd.create_dttm)		AS module_has_stor
		,argMax(gd.is_deficit_stor,                    			gd.create_dttm)		AS module_is_deficit_stor
		,argMax(gd.has_matr,                    				gd.create_dttm)		AS module_has_matr
		,argMax(gd.is_deficit_matr,                    			gd.create_dttm)		AS module_is_deficit_matr
		,argMax(gd.has_tech,                    				gd.create_dttm)		AS module_has_tech
		,argMax(gd.is_deficit_tech,                    			gd.create_dttm)		AS module_is_deficit_tech
		,argMax(gd.has_other,                    				gd.create_dttm)		AS module_has_other
		,argMax(gd.is_deficit_other,                    		gd.create_dttm)		AS module_is_deficit_other
		,argMax(gd.has_serv,                    				gd.create_dttm)		AS module_has_serv
		,argMax(gd.module_deficit_info,                 		gd.create_dttm)		AS module_deficit_info
		,argMax(gd.kolichestvo_dolg,                    		gd.create_dttm)		AS kolichestvo_dolg
		,argMax(gd.kolichestvo_v_rezerve,                 		gd.create_dttm)		AS kolichestvo_v_rezerve
		,argMax(gd.kolichestvo_podtverzhdeno,                   gd.create_dttm)		AS kolichestvo_podtverzhdeno
		,argMax(gd.kolichestvo_zayavka_na_otgruzku,             gd.create_dttm)		AS kolichestvo_zayavka_na_otgruzku
		,argMax(gd.kolichestvo_iz_regiona_zayavka_na_otgruzku,  gd.create_dttm)		AS kolichestvo_iz_regiona_zayavka_na_otgruzku
		,argMax(gd.kolichestvo_iz_tranzita_zayavka_na_otgruzku,	gd.create_dttm)		AS kolichestvo_iz_tranzita_zayavka_na_otgruzku
		,argMax(gd.kolichestvo_raspredeleno_modul_rasp,         gd.create_dttm)		AS kolichestvo_raspredeleno_modul_rasp
		,argMax(gd.kolichestvo_iz_vypuska_rasp,                 gd.create_dttm)		AS kolichestvo_iz_vypuska_rasp
		,argMax(gd.kolichestvo_iz_regionov_rasp,                gd.create_dttm)		AS kolichestvo_iz_regionov_rasp
		,argMax(gd.kolichestvo_so_sklada_regiona_rasp,          gd.create_dttm)		AS kolichestvo_so_sklada_regiona_rasp
		,argMax(gd.kolichestvo_so_sklada_transit_rasp,          gd.create_dttm)		AS kolichestvo_so_sklada_transit_rasp
		,argMax(gd.kolichestvo_ochered_rasp,                    gd.create_dttm)		AS kolichestvo_ochered_rasp
		,argMax(gd.kolichestvo_v_rezerve_rasp,                  gd.create_dttm)		AS kolichestvo_v_rezerve_rasp
		,argMax(gd.kolichestvo_v_zayavkah_rasp,                 gd.create_dttm)		AS kolichestvo_v_zayavkah_rasp
		,argMax(gd.kolichestvo_kompleltno_rasp,                 gd.create_dttm)		AS kolichestvo_komplektno_rasp
		,argMax(gd.is_s_uchetom_vypuska_rasp,                   gd.create_dttm)		AS is_s_uchetom_vypuska_rasp
		,argMax(gd.is_iz_proizvodstva_rasp,                    	gd.create_dttm)		AS is_iz_proizvodstva_rasp
		,argMax(gd.is_iz_reserva_rasp,                    		gd.create_dttm)		AS is_iz_reserva_rasp
		,argMax(gd.is_iz_regiona_rasp,                    		gd.create_dttm)		AS is_iz_regiona_rasp
		,argMax(gd.is_iz_postupleniya_rasp,                    	gd.create_dttm)		AS is_iz_postupleniya_rasp
		,argMax(gd.is_iz_plana_rasp,                    		gd.create_dttm)		AS is_iz_plana_rasp
		,argMax(gd.is_iz_gorizonta_rasp,                    	gd.create_dttm)		AS is_iz_gorizonta_rasp
		,argMax(gd.is_iz_gorizonta,                    			gd.create_dttm)		AS is_iz_gorizonta
		,argMax(gd.is_s_uchetom_regionov_rasp,                  gd.create_dttm)		AS is_s_uchetom_regionov_rasp
		,argMax(gd.is_s_uchetom_postupleniya_v_region_rasp,     gd.create_dttm)		AS is_s_uchetom_postupleniya_v_region_rasp
		,argMax(gd.is_s_uchetom_postupleniya_v_transit_rasp,    gd.create_dttm)		AS is_s_uchetom_postupleniya_v_transit_rasp
		,argMax(gd.ves_brutto_na_dolg,                    		gd.create_dttm)		AS module_ves_brutto_na_dolg
		,argMax(gd.obyom_brutto_na_dolg,                    	gd.create_dttm)		AS module_obyom_brutto_na_dolg
		,argMax(gd.open_order_count,                    		gd.create_dttm)		AS module_open_order_count
		,argMax(gd.open_order_total_sum,                    	gd.create_dttm)		AS module_open_order_total_sum
		,argMax(gd.avg_price,                    				gd.create_dttm)		AS module_avg_price
		,if(
			argMax(gd.open_order_count, gd.create_dttm) > 0, 
			argMax(gd.open_order_count, gd.create_dttm), 
			argMax(gd.kolichestvo_dolg, gd.create_dttm)
		)																			AS module_kolichestvo_total
		,'OPEN_ORDER_TODAY'															AS row_source
		,toDecimal64(0, 3)															AS correct_order_count
		,toDecimal64(0, 3)															AS correct_total_sum
		,toDecimal64(0, 3)															AS sale_order_count
		,toDecimal64(0, 3)															AS sale_total_sum		
	FROM 	
		--rasp2_v02.all_goods AS gd
		{{ params.db2 }}.all_goods AS gd
	WHERE
		gd.date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY 
		gd.order_id
		,gd.art_id	
	UNION ALL
	-- Завершенные вчера заказы
	SELECT 
		ex.order_id																		AS order_id
		,ex.art_id																		AS art_id
		,toDateTime64('1970-01-01 00:00:00.000', 3, 'Europe/Moscow')					AS batch_id_dttm
		,toString('')																	AS batch_id_str
		,addDays(
			argMax(ex.date_id,                    					ex.create_dttm)
			,1
		)																				AS date_id
		,argMax(ex.order_guid_uid,                    			ex.create_dttm)			AS order_guid_uid		
		,argMax(ex.nomenclature_id,                    			ex.create_dttm)			AS nomenclature_id 
		,argMax(ex.nomenclature_guid_uid,              			ex.create_dttm)			AS nomenclature_guid_uid
		,argMax(ex.nomenclature_name,                  			ex.create_dttm)			AS nomenclature_name
		,argMax(ex.nomenclature_property_id,            		ex.create_dttm)			AS nomenclature_property_id
		,argMax(ex.nomenclature_property_guid_uid,      		ex.create_dttm)			AS nomenclature_property_guid_uid
		,argMax(ex.nomenclature_property_name,          		ex.create_dttm)			AS nomenclature_property_name
		,argMax(ex.type_id,                    					ex.create_dttm)			AS type_id
		,argMax(ex.type_name,                    				ex.create_dttm)			AS type_name
		,toBool(0)																		AS module_is_ispolzuemaya_create_date
		,toInt8(0)																		AS module_status_create_date_number
		,toString('')																	AS module_status_create_date_short_name
		,toString('')																	AS module_status_create_date_name
		,if(
			argMax(ex.type_id, ex.create_dttm) = 1
			AND any(sts.status_modul_create_date_short_name) != 'Opt'	-- Не Стандарт-2
			, 1
			, 0
		)																				AS module_has_std1   		
		,toBool(0)																		AS module_is_deficit_std1
		,if(
			argMax(ex.type_id, ex.create_dttm) = 1
			AND any(sts.status_modul_create_date_short_name) = 'Opt'	-- Стандарт-2
			, 1
			, 0
		)																				AS module_has_std2			
		,toBool(0)																		AS module_is_deficit_std2
		,if(argMax(ex.type_id, ex.create_dttm) = 3, 1, 0)								AS module_has_mip
		,toBool(0)																		AS module_is_deficit_mip
		,if(argMax(ex.type_id, ex.create_dttm) = 4, 1, 0)								AS module_has_kich
		,toBool(0)																		AS module_is_deficit_kich
		,if(argMax(ex.type_id, ex.create_dttm) = 6, 1, 0)								AS module_has_stor
		,toBool(0)																		AS module_is_deficit_stor
		,if(argMax(ex.type_id, ex.create_dttm) = 13, 1, 0)								AS module_has_matr
		,toBool(0)																		AS module_is_deficit_matr
		,if(argMax(ex.type_id, ex.create_dttm) = 15, 1, 0)								AS module_has_tech
		,toBool(0)																		AS module_is_deficit_tech
		,if(argMax(ex.type_id, ex.create_dttm) NOT IN (1, 3, 4, 6, 13, 14, 15), 1, 0)	AS module_has_other
		,toBool(0)																		AS module_is_deficit_other
		,if(argMax(ex.type_id, ex.create_dttm) = 14, 1, 0)								AS module_has_serv
		,toString('')																	AS module_deficit_info
		,toDecimal64(0, 3)																AS kolichestvo_dolg
		,toDecimal64(0, 3)																AS kolichestvo_v_rezerve
		,toDecimal64(0, 3)																AS kolichestvo_podtverzhdeno
		,toDecimal64(0, 3)																AS kolichestvo_zayavka_na_otgruzku
		,toDecimal64(0, 3)																AS kolichestvo_iz_regiona_zayavka_na_otgruzku
		,toDecimal64(0, 3)																AS kolichestvo_iz_tranzita_zayavka_na_otgruzku
		,toDecimal64(0, 3)																AS kolichestvo_raspredeleno_modul_rasp
		,toDecimal64(0, 3)																AS kolichestvo_iz_vypuska_rasp
		,toDecimal64(0, 3)																AS kolichestvo_iz_regionov_rasp
		,toDecimal64(0, 3)																AS kolichestvo_so_sklada_regiona_rasp
		,toDecimal64(0, 3)																AS kolichestvo_so_sklada_transit_rasp
		,toDecimal64(0, 3)																AS kolichestvo_ochered_rasp
		,toDecimal64(0, 3)																AS kolichestvo_v_rezerve_rasp
		,toDecimal64(0, 3)																AS kolichestvo_v_zayavkah_rasp
		,toDecimal64(0, 3)																AS kolichestvo_komplektno_rasp
		,toBool(0)																		AS is_s_uchetom_vypuska_rasp
		,toBool(0)																		AS is_iz_proizvodstva_rasp
		,toBool(0)																		AS is_iz_reserva_rasp
		,toBool(0)																		AS is_iz_regiona_rasp
		,toBool(0)																		AS is_iz_postupleniya_rasp
		,toBool(0)																		AS is_iz_plana_rasp
		,toBool(0)																		AS is_iz_gorizonta_rasp
		,toBool(0)																		AS is_iz_gorizonta
		,toBool(0)																		AS is_s_uchetom_regionov_rasp
		,toBool(0)																		AS is_s_uchetom_postupleniya_v_region_rasp
		,toBool(0)																		AS is_s_uchetom_postupleniya_v_transit_rasp
		,toDecimal64(0, 3)																AS module_ves_brutto_na_dolg
		,toDecimal64(0, 3)																AS module_obyom_brutto_na_dolg
		,argMax(ex.order_count,                    		ex.create_dttm)					AS module_open_order_count
		,argMax(ex.total_sum,                    		ex.create_dttm)					AS module_open_order_total_sum
		,argMax(ex.avg_price,                    		ex.create_dttm)					AS module_avg_price
		,argMax(ex.order_count,                    		ex.create_dttm)					AS module_kolichestvo_total
		,'EXIT_ORDER_YESTERDAY'															AS row_source
		,argMax(ex.correct_order_count,                 ex.create_dttm)					AS correct_order_count
		,argMax(ex.correct_total_sum,                   ex.create_dttm)					AS correct_total_sum
		,argMax(ex.sale_order_count,                    ex.create_dttm)					AS sale_order_count
		,argMax(ex.sale_total_sum,                    	ex.create_dttm)					AS sale_total_sum
	FROM 	
		rasp2.order_exit_goods_rn_history AS ex
		LEFT JOIN 		-- Для определения статуса "Стандарт-2"
			(
				SELECT 
					gg.order_id														AS order_id
					,gg.art_id														AS art_id
					,argMax(gg.status_modul_create_date_number, gg.create_dttm) 	AS status_modul_create_date_number
					,argMax(gg.status_modul_create_date_short_name, gg.create_dttm)	AS status_modul_create_date_short_name
				FROM 
					--rasp2_v02.all_goods AS gg
					{{ params.db2 }}.all_goods AS gg
				WHERE 		-- Отбор только нужных товаров в нужных заказах
					-- mg-28.10.2026 - изменение формата условия
					-- cityHash64( tuple(gg.order_id, gg.art_id) ) IN 
					tuple(gg.order_id, gg.art_id) IN	
						(			
							SELECT 
				            	--cityHash64(tuple(order_id, art_id) )
								tuple(order_id, art_id)									
				            FROM rasp2.order_exit_goods_rn_history
				            WHERE date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
						)
					-- mg-28.10.2026 - изменение формата условия
				GROUP BY
					gg.order_id									
					,gg.art_id
			) AS sts
		ON ex.order_id = sts.order_id
			AND ex.art_id = sts.art_id
	WHERE
		ex.date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY 
		ex.order_id
		,ex.art_id
)
,
-- Заказы покупателей на Сегодня
cte_self_cur AS
(
	SELECT 						-- 6 303 строк ('2026-09-28')
		st.order_id																				AS self_order_id
		,argMax(st.order_roznica_guid_uid,                    				st.create_dttm)		AS self_order_guid_uid
		,argMax(st.order_roznica_doc_num,                    				st.create_dttm)		AS self_order_number
		,argMax(st.order_roznica_doc_datetime,                    			st.create_dttm)		AS self_order_datetime
		,argMax(st.full_order_id,                    						st.create_dttm)		AS full_order_id
		,argMax(st.unit_id,                    								st.create_dttm)		AS self_unit_id
		,argMax(st.unit_guid_uid,                    						st.create_dttm)		AS self_unit_guid_uid
		,argMax(st.unit_name,                    							st.create_dttm)		AS self_unit_name
		,argMax(st.city_id,                    								st.create_dttm)		AS self_city_id
		,argMax(st.city_guid_uid,                    						st.create_dttm)		AS self_city_guid_uid
		,argMax(st.city_name,                    							st.create_dttm)		AS self_city_name
		,substring(argMax(st.city_name,                    					st.create_dttm), 3)	AS self_city_name_clean
		,argMax(st.client_id,                    							st.create_dttm)		AS client_id
		,argMax(st.client_guid_uid,                    						st.create_dttm)		AS client_guid_uid
		,argMax(st.client_name,                    							st.create_dttm)		AS client_name	
		,argMax(st.order_phone,                    							st.create_dttm)		AS order_phone
		,argMax(st.employee_id,                    							st.create_dttm)		AS employee_id
		,argMax(st.employee_guid_uid,                    					st.create_dttm)		AS employee_guid_uid
		,argMax(st.employee_name,                    						st.create_dttm)		AS employee_name
		,argMax(st.is_mip,                    								st.create_dttm)		AS self_is_mip
		,argMax(st.is_kitchen,                    							st.create_dttm)		AS self_is_kitchen
		,argMax(st.mipik_status_id,                    						st.create_dttm)		AS self_mipik_status_id
		,argMax(st.mipik_status_name,                    					st.create_dttm)		AS self_mipik_status_name
		,argMax(st.mipik_status_order,                    					st.create_dttm)		AS self_mipik_status_order
		,argMax(st.mipik_collection_id,                    					st.create_dttm)		AS self_mipik_collection_id
		,argMax(st.mipik_collection_name,                    				st.create_dttm)		AS self_mipik_collection_name
		,argMax(st.mipik_collection_guid_uid,                    			st.create_dttm)		AS self_mipik_collection_guid_uid
		,argMax(st.mipik_assortiment_group_id,                    			st.create_dttm)		AS self_mipik_assortiment_group_id
		,argMax(st.mipik_assortiment_group_name,                    		st.create_dttm)		AS self_mipik_assortiment_group_name
		,argMax(st.mipik_assortiment_group_guid_uid,                    	st.create_dttm)		AS self_mipik_assortiment_group_guid_uid
		,argMax(st.mipik_customization_type_id,                    			st.create_dttm)		AS self_mipik_customization_type_id
		,argMax(st.mipik_customization_type_name,                    		st.create_dttm)		AS self_mipik_customization_type_name
		,argMax(st.mipik_customization_type_guid_uid,                    	st.create_dttm)		AS self_mipik_customization_type_guid_uid
		,argMax(st.mipik_trek_mik_name,                    					st.create_dttm)		AS self_mipik_trek_mik_name
		,argMax(st.mipik_trek_mik_guid_uid,                    				st.create_dttm)		AS self_mipik_trek_mik_guid_uid
		,argMax(st.max_data_perenosa,                    					st.create_dttm)		AS self_max_data_perenosa
		,argMax(st.count_perenosov,                    						st.create_dttm)		AS self_count_perenosov
		,argMax(st.is_oplachen_roznica,                    					st.create_dttm)		AS self_is_oplachen_roznica
		,argMax(st.is_po_prosbe_clienta_roznica,                    		st.create_dttm)		AS self_is_po_prosbe_clienta_roznica
		,argMax(st.data_dostavki_dogovor_roznica,                    		st.create_dttm)		AS self_data_dostavki_dogovor_roznica
		,argMax(st.data_dostavki_roznica,                    				st.create_dttm)		AS self_data_dostavki_roznica
		,argMax(st.data_otsechki,                    						st.create_dttm)		AS self_data_otsechki
		,argMax(st.data_komplekta,                    						st.create_dttm)		AS self_data_komplekta
		,argMax(st.data_celevaya,                    						st.create_dttm)		AS self_data_celevaya
		,argMax(st.data_vyhoda_iz_proizvodstva,                    			st.create_dttm)		AS self_data_vyhoda_iz_proizvodstva
		,argMax(st.data_sborki_rasp,                    					st.create_dttm)		AS self_data_sborki_rasp
		,argMax(st.data_otgruzki_iz_Voronezh_rasp,                    		st.create_dttm)		AS self_data_otgruzki_iz_Voronezh_rasp
		,argMax(st.data_reserva_rasp,                    					st.create_dttm)		AS self_data_reserva_rasp
		,argMax(st.kommentarii_roznica,                    					st.create_dttm)		AS self_kommentarii_roznica
		,argMax(st.norma_days_dostavki_iz_voronezha,                    	st.create_dttm)		AS self_norma_days_dostavki_iz_voronezha
		,if(
		    argMax(st.data_otgruzki_iz_Voronezh_rasp, st.create_dttm) != toDate('1970-01-01'),
		    addDays(
		        argMax(st.data_otgruzki_iz_Voronezh_rasp, st.create_dttm),
		        argMax(st.norma_days_dostavki_iz_voronezha, st.create_dttm)
		    ),
		    toDate('1970-01-01')																
		)																						AS self_calculated_sale_date
		,argMax(st.source_stage_id,                    						st.create_dttm)		AS self_source_stage_id
		,argMax(st.source_stage_name,                    					st.create_dttm)		AS self_source_stage_name
		,argMax(st.order_stage_id,                    						st.create_dttm)		AS self_stage_id
		,argMax(st.order_stage_name,                    					st.create_dttm)		AS self_stage_name
		,argMax(st.order_sub_stage_id,                    					st.create_dttm)		AS self_sub_stage_id
		,argMax(st.order_sub_stage_name,                    				st.create_dttm)		AS self_sub_stage_name
		,argMax(st.has_std1,                    							st.create_dttm)		AS self_has_std1
		,argMax(st.is_deficit_std1,                    						st.create_dttm)		AS self_is_deficit_std1
		,argMax(st.total_sum_std1,                    						st.create_dttm)		AS self_total_sum_std1
		,argMax(st.has_std2,                    							st.create_dttm)		AS self_has_std2
		,argMax(st.is_deficit_std2,                    						st.create_dttm)		AS self_is_deficit_std2
		,argMax(st.total_sum_std2,                    						st.create_dttm)		AS self_total_sum_std2
		,argMax(st.has_mip,                    								st.create_dttm)		AS self_has_mip
		,argMax(st.is_deficit_mip,                    						st.create_dttm)		AS self_is_deficit_mip
		,argMax(st.total_sum_mip,                    						st.create_dttm)		AS self_total_sum_mip
		,argMax(st.has_kich,                    							st.create_dttm)		AS self_has_kich
		,argMax(st.is_deficit_kich,                    						st.create_dttm)		AS self_is_deficit_kich
		,argMax(st.total_sum_kich,                    						st.create_dttm)		AS self_total_sum_kich
		,argMax(st.has_stor,                    							st.create_dttm)		AS self_has_stor
		,argMax(st.is_deficit_stor,                    						st.create_dttm)		AS self_is_deficit_stor
		,argMax(st.total_sum_stor,                    						st.create_dttm)		AS self_total_sum_stor
		,argMax(st.has_matr,                    							st.create_dttm)		AS self_has_matr
		,argMax(st.is_deficit_matr,                    						st.create_dttm)		AS self_is_deficit_matr
		,argMax(st.total_sum_matr,                    						st.create_dttm)		AS self_total_sum_matr
		,argMax(st.has_tech,                    							st.create_dttm)		AS self_has_tech
		,argMax(st.is_deficit_tech,                    						st.create_dttm)		AS self_is_deficit_tech
		,argMax(st.total_sum_tech,                    						st.create_dttm)		AS self_total_sum_tech
		,argMax(st.has_other,                    							st.create_dttm)		AS self_has_other
		,argMax(st.is_deficit_other,                    					st.create_dttm)		AS self_is_deficit_other
		,argMax(st.total_sum_other,                    						st.create_dttm)		AS self_total_sum_other
		,argMax(st.has_serv,                    							st.create_dttm)		AS self_has_serv
		,argMax(st.total_sum_serv,                    						st.create_dttm)		AS self_total_sum_serv
		,argMax(st.has_total_products,                    					st.create_dttm)		AS self_has_total_products
		,argMax(st.has_total_deficit,                    					st.create_dttm)		AS self_has_total_deficit
		,argMax(st.is_partial_full_order_exit,                    			st.create_dttm)		AS self_is_partial_full_order_exit
		,argMax(st.self_order_deficit_info,                    				st.create_dttm)		AS self_deficit_info	
	FROM
		--rasp3_v02.self_order AS st  
		{{ params.db3 }}.self_order AS st
	WHERE
		st.date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		st.order_id
)
,
-- Заказы покупателей на Вчера
cte_self_prev AS
(
	SELECT 						-- ??? строк ('2026-09-28' - INTERVAL 1 DAY)
		sy.self_order_id																		AS self_order_id
		,argMax(sy.self_source_stage_id,                    				sy.create_dttm)		AS self_source_stage_id
		,argMax(sy.self_stage_id,                    						sy.create_dttm)		AS self_stage_id
		,argMax(sy.self_sub_stage_id,                    					sy.create_dttm)		AS self_sub_stage_id
		,argMax(sy.self_stage_duration_calendar_days,                    	sy.create_dttm)		AS self_stage_duration_calendar_days
		,argMax(sy.self_stage_iteration,                    				sy.create_dttm)		AS self_stage_iteration
		,argMax(sy.self_sub_stage_duration_calendar_days,                   sy.create_dttm)		AS self_sub_stage_duration_calendar_days
		,argMax(sy.self_sub_stage_iteration,                    			sy.create_dttm)		AS self_sub_stage_iteration
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,argMax(sy.self_total_duration_work_days,                    		sy.create_dttm)		AS self_total_duration_work_days
		,argMax(sy.self_stage_duration_work_days,                    		sy.create_dttm)		AS self_stage_duration_work_days
		,argMax(sy.self_sub_stage_duration_work_days,                    	sy.create_dttm)		AS self_sub_stage_duration_work_days
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
	FROM
		--rasp3_v02.goods_in_order_stage AS sy  
		{{ params.db3 }}.goods_in_order_stage AS sy  
	WHERE
		sy.date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		sy.self_order_id	
)
,
-- Кол-во итераций stage_id для self_order_id
cte_self_stage_iteration AS
(
    SELECT						-- ??? строк ('2026-09-28')
        ss.self_order_id					AS self_order_id
        ,ss.self_stage_id					AS self_stage_id
        ,max(ss.self_stage_iteration) 		AS max_num_iteration
    FROM 
    	--rasp3_v02.goods_in_order_stage AS ss
    	{{ params.db3 }}.goods_in_order_stage AS ss
    WHERE 
    	ss.date_id < (SELECT current_date FROM cte_dt_rasp LIMIT 1)
    	AND ss.self_order_id IN
	        (
	            SELECT order_id
	            FROM 
	            	--rasp3_v02.self_order
	            	{{ params.db3 }}.self_order
	            WHERE date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	        )
    GROUP BY
        ss.self_order_id,
        ss.self_stage_id
)		
,
-- Кол-во итераций sub_stage_id для self_order_id
cte_self_sub_stage_iteration AS
(
    SELECT						-- ??? строк ('2026-09-28')
        sb.self_order_id					AS self_order_id
        ,sb.self_sub_stage_id				AS self_sub_stage_id
        ,max(sb.self_sub_stage_iteration) 	AS max_num_iteration
    FROM 
    	--rasp3_v02.goods_in_order_stage AS sb
    	{{ params.db3 }}.goods_in_order_stage AS sb
    WHERE 
    	sb.date_id < (SELECT current_date FROM cte_dt_rasp LIMIT 1)
    	AND sb.self_order_id IN
	        (
	            SELECT order_id
	            FROM 
	            	--rasp3_v02.self_order
	            	{{ params.db3 }}.self_order
	            WHERE date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	        )
    GROUP BY
        sb.self_order_id,
        sb.self_sub_stage_id
)
,
-- Полные заказы покупателей на Сегодня
cte_full_cur AS							-- 5 216 строк ('2026-09-28')
(
	SELECT 
		ft.full_order_id																		AS full_order_id
		,argMax(ft.full_order_roznica_guid_uid,                    			ft.create_dttm)		AS full_order_guid_uid
		,argMax(ft.full_order_roznica_number,                    			ft.create_dttm)		AS full_order_number
		,argMax(ft.full_order_roznica_datetime,                    			ft.create_dttm)		AS full_order_datetime
		,argMax(ft.full_unit_id,                    						ft.create_dttm)		AS full_unit_id
		,argMax(ft.full_unit_guid_uid,                    					ft.create_dttm)		AS full_unit_guid_uid
		,argMax(ft.full_unit_name,                    						ft.create_dttm)		AS full_unit_name
		,argMax(ft.full_city_id,                    						ft.create_dttm)		AS full_city_id
		,argMax(ft.full_city_guid_uid,                    					ft.create_dttm)		AS full_city_guid_uid
		,argMax(ft.full_city_name,                    						ft.create_dttm)		AS full_city_name
		,substring(argMax(ft.full_city_name,                    			ft.create_dttm), 3)	AS full_city_name_clean
		,argMax(ft.is_link_order,                    						ft.create_dttm)		AS full_is_link_order
		,argMax(ft.norma_days_dostavki_iz_voronezha,                    	ft.create_dttm)		AS full_norma_days_dostavki_iz_voronezha
		,argMax(ft.source_stage_id,                    						ft.create_dttm)		AS full_source_stage_id
		,argMax(ft.source_stage_name,                    					ft.create_dttm)		AS full_source_stage_name
		,argMax(ft.order_stage_id,                    						ft.create_dttm)		AS full_stage_id
		,argMax(ft.order_stage_name,                    					ft.create_dttm)		AS full_stage_name
		,argMax(ft.order_sub_stage_id,                    					ft.create_dttm)		AS full_sub_stage_id
		,argMax(ft.order_sub_stage_name,                    				ft.create_dttm)		AS full_sub_stage_name
		,argMax(ft.has_std1,                    							ft.create_dttm)		AS full_has_std1
		,argMax(ft.is_deficit_std1,                    						ft.create_dttm)		AS full_is_deficit_std1
		,argMax(ft.total_sum_std1,                    						ft.create_dttm)		AS full_total_sum_std1
		,argMax(ft.has_std2,                    							ft.create_dttm)		AS full_has_std2
		,argMax(ft.is_deficit_std2,                    						ft.create_dttm)		AS full_is_deficit_std2
		,argMax(ft.total_sum_std2,                    						ft.create_dttm)		AS full_total_sum_std2
		,argMax(ft.has_mip,                    								ft.create_dttm)		AS full_has_mip
		,argMax(ft.is_deficit_mip,                    						ft.create_dttm)		AS full_is_deficit_mip 
		,argMax(ft.total_sum_mip,                    						ft.create_dttm)		AS full_total_sum_mip
		,argMax(ft.has_kich,                    							ft.create_dttm)		AS full_has_kich
		,argMax(ft.is_deficit_kich,                    						ft.create_dttm)		AS full_is_deficit_kich
		,argMax(ft.total_sum_kich,                    						ft.create_dttm)		AS full_total_sum_kich
		,argMax(ft.has_stor,                    							ft.create_dttm)		AS full_has_stor
		,argMax(ft.is_deficit_stor,                    						ft.create_dttm)		AS full_is_deficit_stor
		,argMax(ft.total_sum_stor,                    						ft.create_dttm)		AS full_total_sum_stor
		,argMax(ft.has_matr,                    							ft.create_dttm)		AS full_has_matr
		,argMax(ft.is_deficit_matr,                    						ft.create_dttm)		AS full_is_deficit_matr
		,argMax(ft.total_sum_matr,                    						ft.create_dttm)		AS full_total_sum_matr
		,argMax(ft.has_tech,                    							ft.create_dttm)		AS full_has_tech
		,argMax(ft.is_deficit_tech,                    						ft.create_dttm)		AS full_is_deficit_tech
		,argMax(ft.total_sum_tech,                    						ft.create_dttm)		AS full_total_sum_tech
		,argMax(ft.has_other,                    							ft.create_dttm)		AS full_has_other
		,argMax(ft.is_deficit_other,                    					ft.create_dttm)		AS full_is_deficit_other
		,argMax(ft.total_sum_other,                    						ft.create_dttm)		AS full_total_sum_other
		,argMax(ft.has_serv,                    							ft.create_dttm)		AS full_has_serv
		,argMax(ft.total_sum_serv,                    						ft.create_dttm)		AS full_total_sum_serv
		,argMax(ft.has_total_products,                    					ft.create_dttm)		AS full_has_total_products
		,argMax(ft.has_total_deficit,                    					ft.create_dttm)		AS full_has_total_deficit
		,argMax(ft.full_order_deficit_info,                    				ft.create_dttm)		AS full_deficit_info
	FROM 
		--rasp3_v02.full_order AS ft
		{{ params.db3 }}.full_order AS ft
	WHERE
		ft.date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY 
		ft.full_order_id
)
,
-- Полные заказы покупателей на Вчера
cte_full_prev AS
(
	SELECT 						-- ??? строк ('2026-09-28' - INTERVAL 1 DAY)
		fy.full_order_id																		AS full_order_id
		,argMax(fy.full_source_stage_id,                    				fy.create_dttm)		AS full_source_stage_id
		,argMax(fy.full_stage_id,                    						fy.create_dttm)		AS full_stage_id
		,argMax(fy.full_sub_stage_id,                    					fy.create_dttm)		AS full_sub_stage_id
		,argMax(fy.full_stage_duration_calendar_days,                    	fy.create_dttm)		AS full_stage_duration_calendar_days
		,argMax(fy.full_stage_iteration,                    				fy.create_dttm)		AS full_stage_iteration
		,argMax(fy.full_sub_stage_duration_calendar_days,                   fy.create_dttm)		AS full_sub_stage_duration_calendar_days
		,argMax(fy.full_sub_stage_iteration,                    			fy.create_dttm)		AS full_sub_stage_iteration
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,argMax(fy.full_total_duration_work_days,                    		fy.create_dttm)		AS full_total_duration_work_days
		,argMax(fy.full_stage_duration_work_days,                    		fy.create_dttm)		AS full_stage_duration_work_days
		,argMax(fy.full_sub_stage_duration_work_days,                    	fy.create_dttm)		AS full_sub_stage_duration_work_days
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
	FROM
		--rasp3_v02.goods_in_order_stage AS fy  
		{{ params.db3 }}.goods_in_order_stage AS fy  
	WHERE
		fy.date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		fy.full_order_id		
)
,
-- Кол-во итераций stage_id для full_order_id
cte_full_stage_iteration AS
(
    SELECT
        fs.full_order_id					AS full_order_id
        ,fs.full_stage_id					AS full_stage_id
        ,max(fs.full_stage_iteration) 		AS max_num_iteration
    FROM 
    	--rasp3_v02.goods_in_order_stage AS fs
    	{{ params.db3 }}.goods_in_order_stage AS fs
    WHERE 
    	fs.date_id < (SELECT current_date FROM cte_dt_rasp LIMIT 1)
    	AND fs.full_order_id IN
	        (
	            SELECT full_order_id
	            FROM 
	            	--rasp3_v02.full_order
	            	{{ params.db3 }}.full_order
	            WHERE date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	        )
    GROUP BY
        fs.full_order_id,
        fs.full_stage_id
)
,
-- Кол-во итераций sub_stage_id для full_order_id
cte_full_sub_stage_iteration AS
(
    SELECT
        fb.full_order_id					AS full_order_id
        ,fb.full_sub_stage_id				AS full_sub_stage_id
        ,max(fb.full_sub_stage_iteration) 	AS max_num_iteration
    FROM 
    	--rasp3_v02.goods_in_order_stage AS fb
    	{{ params.db3 }}.goods_in_order_stage AS fb
    WHERE 
    	fb.date_id < (SELECT current_date FROM cte_dt_rasp LIMIT 1)
    	AND fb.full_order_id IN
	        (
	            SELECT full_order_id
	            FROM 
	            	--rasp3_v02.full_order
	            	{{ params.db3 }}.full_order
	            WHERE date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	        )
    GROUP BY
        fb.full_order_id,
        fb.full_sub_stage_id
)
,
-- Подразделения для self_order
cte_unit_self AS
(
	SELECT						-- 809 строк ('2026-09-28')
		su.unit_id											AS unit_id
		,argMax(su.salon_type_id,    su.create_dttm)		AS salon_type_id
		,argMax(su.salon_type_name,  su.create_dttm)		AS salon_type_name
		,argMax(su.division_name,    su.create_dttm)		AS division_name
	FROM
		rasp2.unit_sale AS su
	GROUP BY
		su.unit_id
)
,
-- Подразделения для full_order
cte_unit_full AS
(
	SELECT						-- 809 строк ('2026-09-28')
		fu.unit_id											AS unit_id
		,argMax(fu.salon_type_id,    fu.create_dttm)		AS salon_type_id
		,argMax(fu.salon_type_name,  fu.create_dttm)		AS salon_type_name
		,argMax(fu.division_name,    fu.create_dttm)		AS division_name
	FROM
		rasp2.unit_sale AS fu
	GROUP BY
		fu.unit_id
)
,
-- Классификатор Стандартных модулей
cte_class AS					-- 7 670 строк ('2026-09-28')
(
	SELECT
		cs.art_id														AS art_id
		,argMax(cs.pao_brand_id,    				cs.create_dttm)		AS pao_brand_id
		,argMax(cs.pao_brand_name,    				cs.create_dttm)		AS pao_brand_name
		,argMax(cs.pao_group_id,    				cs.create_dttm)		AS pao_group_id
		,argMax(cs.pao_group_name,    				cs.create_dttm)		AS pao_group_name
		,argMax(cs.pao_collection_id,    			cs.create_dttm)		AS pao_collection_id
		,argMax(cs.pao_collection_name,    			cs.create_dttm)		AS pao_collection_name
		,argMax(cs.pao_color_id,    				cs.create_dttm)		AS pao_color_id
		,argMax(cs.pao_color_name,    				cs.create_dttm)		AS pao_color_name
		,argMax(cs.pao_collection_color_name,    	cs.create_dttm)		AS pao_collection_color_name
		,argMax(cs.ang_brand_id,    				cs.create_dttm)		AS ang_brand_id
		,argMax(cs.ang_brand_name,    				cs.create_dttm)		AS ang_brand_name
		,argMax(cs.ang_group_id,    				cs.create_dttm)		AS ang_group_id
		,argMax(cs.ang_group_name,    				cs.create_dttm)		AS ang_group_name
		,argMax(cs.ang_collection_id,    			cs.create_dttm)		AS ang_collection_id
		,argMax(cs.ang_collection_name,    			cs.create_dttm)		AS ang_collection_name
		,argMax(cs.ang_color_id,    				cs.create_dttm)		AS ang_color_id
		,argMax(cs.ang_color_name,    				cs.create_dttm)		AS ang_color_name
		,argMax(cs.ang_collection_color_name,    	cs.create_dttm)		AS ang_collection_color_name
		,argMax(cs.ang_collection_pao_color_name,   cs.create_dttm)		AS ang_collection_pao_color_name
		,argMax(cs.ang_kind_id,    					cs.create_dttm)		AS ang_kind_id
		,argMax(cs.ang_kind_name,    				cs.create_dttm)		AS ang_kind_name
	FROM
		rasp2.pao_module_classification AS cs
	GROUP BY
		cs.art_id
)
,
-- Сборка запроса
cte_total AS 
(
	SELECT 								-- 26 156 строк ('2026-09-28')
		good.batch_id_dttm									AS batch_id_dttm
		,good.batch_id_str									AS batch_id_str
		,good.date_id										AS date_id
		,f_cur.full_order_id								AS full_order_id
		,f_cur.full_order_guid_uid							AS full_order_guid_uid
		,f_cur.full_order_number							AS full_order_number
		,f_cur.full_order_datetime							AS full_order_datetime
		,f_cur.full_unit_id									AS full_unit_id
		,f_cur.full_unit_guid_uid							AS full_unit_guid_uid
		,f_cur.full_unit_name								AS full_unit_name
		,f_cur.full_city_id									AS full_city_id
		,f_cur.full_city_guid_uid							AS full_city_guid_uid
		,f_cur.full_city_name								AS full_city_name
		,f_cur.full_city_name_clean							AS full_city_name_clean
		,f_unit.salon_type_id								AS full_salon_type_id
		,f_unit.salon_type_name								AS full_salon_type_name
		,f_unit.division_name								AS full_division_name
		,f_cur.full_is_link_order							AS full_is_link_order
		,f_cur.full_norma_days_dostavki_iz_voronezha		AS full_norma_days_dostavki_iz_voronezha
		,f_cur.full_source_stage_id							AS full_source_stage_id
		,f_cur.full_source_stage_name						AS full_source_stage_name
		,f_cur.full_stage_id								AS full_stage_id
		,f_cur.full_stage_name								AS full_stage_name
		,f_cur.full_sub_stage_id							AS full_sub_stage_id
		,f_cur.full_sub_stage_name							AS full_sub_stage_name	
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
			cdr.previous_is_work_day = 1,							-- Вчера - рабочий день
			f_prev.full_total_duration_work_days + 1,				-- Если во вчерашнем срезе заказа не было, то f_prev.full_total_duration_work_days = 0
			f_prev.full_total_duration_work_days
		)													AS full_total_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    f_prev.full_order_id = 0								-- Новый заказ
		    	OR f_prev.full_stage_id != f_cur.full_stage_id,		-- Статус изменился	
		    1,
		    0
		) 													AS full_stage_is_start
		,if(
		    f_prev.full_order_id = 0								-- Новый заказ
		    	OR f_prev.full_stage_id != f_cur.full_stage_id,		-- Статус изменился	
		    1,
		    f_prev.full_stage_duration_calendar_days + 1
		) 													AS full_stage_duration_calendar_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    f_prev.full_order_id = 0								-- Новый заказ
		    	OR f_prev.full_stage_id != f_cur.full_stage_id,		-- Статус изменился
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	1,
		    	0
		    ),	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	f_prev.full_stage_duration_work_days + 1,
		    	f_prev.full_stage_duration_work_days
		    )
		)													AS full_stage_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------	
		,if(
		    f_prev.full_order_id = 0								-- Новый заказ
		    	OR f_prev.full_stage_id != f_cur.full_stage_id,		-- Статус изменился		
		    cte_full_stage_iteration.max_num_iteration + 1,			-- Наращиваем iteration на +1		
		    f_prev.full_stage_iteration								-- Оставляем текущий iteration
		) 													AS full_stage_iteration			
		,if(
		    f_prev.full_order_id = 0										-- Новый заказ
		    	OR f_prev.full_sub_stage_id != f_cur.full_sub_stage_id,		-- Под-Статус изменился	
		    1,
		    0
		) 													AS full_sub_stage_is_start
		,if(
		    f_prev.full_order_id = 0										-- Новый заказ
		    	OR f_prev.full_sub_stage_id != f_cur.full_sub_stage_id,		-- Под-Статус изменился	
		    1,
		    f_prev.full_sub_stage_duration_calendar_days + 1
		) 													AS full_sub_stage_duration_calendar_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    f_prev.full_order_id = 0										-- Новый заказ
		    	OR f_prev.full_sub_stage_id != f_cur.full_sub_stage_id,		-- Под-Статус изменился	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	1,
		    	0
		    ),	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	f_prev.full_sub_stage_duration_work_days + 1,
		    	f_prev.full_sub_stage_duration_work_days
		    )
		)													AS full_sub_stage_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    f_prev.full_order_id = 0										-- Новый заказ
		    	OR f_prev.full_sub_stage_id != f_cur.full_sub_stage_id,		-- Под-Статус изменился		
		    cte_full_sub_stage_iteration.max_num_iteration + 1,				-- Наращиваем iteration на +1		
		    f_prev.full_sub_stage_iteration									-- Оставляем текущий iteration
		) 													AS full_sub_stage_iteration	
		,f_cur.full_has_std1								AS full_has_std1
		,f_cur.full_is_deficit_std1							AS full_is_deficit_std1
		,f_cur.full_total_sum_std1							AS full_total_sum_std1
		,f_cur.full_has_std2								AS full_has_std2
		,f_cur.full_is_deficit_std2							AS full_is_deficit_std2
		,f_cur.full_total_sum_std2							AS full_total_sum_std2
		,f_cur.full_has_mip									AS full_has_mip
		,f_cur.full_is_deficit_mip							AS full_is_deficit_mip 
		,f_cur.full_total_sum_mip							AS full_total_sum_mip
		,f_cur.full_has_kich								AS full_has_kich
		,f_cur.full_is_deficit_kich							AS full_is_deficit_kich
		,f_cur.full_total_sum_kich							AS full_total_sum_kich
		,f_cur.full_has_stor								AS full_has_stor
		,f_cur.full_is_deficit_stor							AS full_is_deficit_stor
		,f_cur.full_total_sum_stor							AS full_total_sum_stor
		,f_cur.full_has_matr								AS full_has_matr
		,f_cur.full_is_deficit_matr							AS full_is_deficit_matr
		,f_cur.full_total_sum_matr							AS full_total_sum_matr
		,f_cur.full_has_tech								AS full_has_tech
		,f_cur.full_is_deficit_tech							AS full_is_deficit_tech
		,f_cur.full_total_sum_tech							AS full_total_sum_tech
		,f_cur.full_has_other								AS full_has_other
		,f_cur.full_is_deficit_other						AS full_is_deficit_other
		,f_cur.full_total_sum_other							AS full_total_sum_other
		,f_cur.full_has_serv								AS full_has_serv
		,f_cur.full_total_sum_serv							AS full_total_sum_serv
		,f_cur.full_has_total_products						AS full_has_total_products
		,f_cur.full_has_total_deficit						AS full_has_total_deficit
		,f_cur.full_deficit_info							AS full_deficit_info	
		,s_cur.self_order_id								AS self_order_id
		,s_cur.self_order_guid_uid							AS self_order_guid_uid
		,s_cur.self_order_number							AS self_order_number
		,s_cur.self_order_datetime							AS self_order_datetime
		,s_cur.self_unit_id									AS self_unit_id
		,s_cur.self_unit_guid_uid							AS self_unit_guid_uid
		,s_cur.self_unit_name								AS self_unit_name
		,s_cur.self_city_id									AS self_city_id
		,s_cur.self_city_guid_uid							AS self_city_guid_uid
		,s_cur.self_city_name								AS self_city_name
		,s_cur.self_city_name_clean							AS self_city_name_clean
		,s_unit.salon_type_id								AS self_salon_type_id
		,s_unit.salon_type_name								AS self_salon_type_name
		,s_unit.division_name								AS self_division_name
		,s_cur.client_id									AS client_id
		,s_cur.client_guid_uid								AS client_guid_uid
		,s_cur.client_name									AS client_name	
		,s_cur.order_phone									AS order_phone
		,s_cur.employee_id									AS employee_id
		,s_cur.employee_guid_uid							AS employee_guid_uid
		,s_cur.employee_name								AS employee_name
		,s_cur.self_is_mip									AS self_is_mip
		,s_cur.self_is_kitchen								AS self_is_kitchen
		,s_cur.self_mipik_status_id							AS self_mipik_status_id
		,s_cur.self_mipik_status_name						AS self_mipik_status_name
		,s_cur.self_mipik_status_order						AS self_mipik_status_order
		,s_cur.self_mipik_collection_id						AS self_mipik_collection_id
		,s_cur.self_mipik_collection_name					AS self_mipik_collection_name
		,s_cur.self_mipik_collection_guid_uid				AS self_mipik_collection_guid_uid
		,s_cur.self_mipik_assortiment_group_id				AS self_mipik_assortiment_group_id
		,s_cur.self_mipik_assortiment_group_name			AS self_mipik_assortiment_group_name
		,s_cur.self_mipik_assortiment_group_guid_uid		AS self_mipik_assortiment_group_guid_uid
		,s_cur.self_mipik_customization_type_id				AS self_mipik_customization_type_id
		,s_cur.self_mipik_customization_type_name			AS self_mipik_customization_type_name
		,s_cur.self_mipik_customization_type_guid_uid		AS self_mipik_customization_type_guid_uid
		,s_cur.self_mipik_trek_mik_name						AS self_mipik_trek_mik_name
		,s_cur.self_mipik_trek_mik_guid_uid					AS self_mipik_trek_mik_guid_uid
		,s_cur.self_max_data_perenosa						AS self_max_data_perenosa
		,s_cur.self_count_perenosov							AS self_count_perenosov
		,s_cur.self_is_oplachen_roznica						AS self_is_oplachen_roznica
		,s_cur.self_is_po_prosbe_clienta_roznica			AS self_is_po_prosbe_clienta_roznica
		,s_cur.self_data_dostavki_dogovor_roznica			AS self_data_dostavki_dogovor_roznica
		,s_cur.self_data_dostavki_roznica					AS self_data_dostavki_roznica
		,if(
			s_cur.self_is_po_prosbe_clienta_roznica = 1 
        		AND s_cur.self_data_dostavki_roznica > s_cur.self_data_dostavki_dogovor_roznica,
    		s_cur.self_data_dostavki_roznica,
    		s_cur.self_data_dostavki_dogovor_roznica	
		)													AS self_data_dostavki_dogovor_or_po_prosbe_clienta_roznica
		,s_cur.self_data_otsechki							AS self_data_otsechki
		,s_cur.self_data_komplekta							AS self_data_komplekta
		,s_cur.self_data_celevaya							AS self_data_celevaya
		,s_cur.self_data_vyhoda_iz_proizvodstva				AS self_data_vyhoda_iz_proizvodstva
		,s_cur.self_data_sborki_rasp						AS self_data_sborki_rasp
		,s_cur.self_data_otgruzki_iz_Voronezh_rasp			AS self_data_otgruzki_iz_Voronezh_rasp
		,s_cur.self_data_reserva_rasp						AS self_data_reserva_rasp
		,s_cur.self_kommentarii_roznica						AS self_kommentarii_roznica
		,s_cur.self_norma_days_dostavki_iz_voronezha		AS self_norma_days_dostavki_iz_voronezha
		,s_cur.self_calculated_sale_date					AS self_calculated_sale_date
		,s_cur.self_source_stage_id							AS self_source_stage_id
		,s_cur.self_source_stage_name						AS self_source_stage_name
		,s_cur.self_stage_id								AS self_stage_id
		,s_cur.self_stage_name								AS self_stage_name
		,s_cur.self_sub_stage_id							AS self_sub_stage_id
		,s_cur.self_sub_stage_name							AS self_sub_stage_name
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
			cdr.previous_is_work_day = 1,							-- Вчера - рабочий день
			s_prev.self_total_duration_work_days + 1,				-- Если во вчерашнем срезе заказа не было, то s_prev.self_total_duration_work_days = 0
			s_prev.self_total_duration_work_days
		)													AS self_total_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    s_prev.self_order_id = 0								-- Новый заказ
		    	OR s_prev.self_stage_id != s_cur.self_stage_id,		-- Статус изменился	
		    1,
		    0
		) 													AS self_stage_is_start
		,if(
		    s_prev.self_order_id = 0								-- Новый заказ
		    	OR s_prev.self_stage_id != s_cur.self_stage_id,		-- Статус изменился	
		    1,
		    s_prev.self_stage_duration_calendar_days + 1
		) 													AS self_stage_duration_calendar_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    s_prev.self_order_id = 0								-- Новый заказ
		    	OR s_prev.self_stage_id != s_cur.self_stage_id,		-- Статус изменился	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	1,
		    	0
		    ),	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	s_prev.self_stage_duration_work_days + 1,
		    	s_prev.self_stage_duration_work_days
		    )
		)													AS self_stage_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    s_prev.self_order_id = 0								-- Новый заказ
		    	OR s_prev.self_stage_id != s_cur.self_stage_id,		-- Статус изменился		
		    cte_self_stage_iteration.max_num_iteration + 1,			-- Наращиваем iteration на +1		
		    s_prev.self_stage_iteration								-- Оставляем текущий iteration
		) 													AS self_stage_iteration
		,if(
		    s_prev.self_order_id = 0										-- Новый заказ
		    	OR s_prev.self_sub_stage_id != s_cur.self_sub_stage_id,		-- Под-Статус изменился	
		    1,
		    0
		) 													AS self_sub_stage_is_start
		,if(
		    s_prev.self_order_id = 0										-- Новый заказ
		    	OR s_prev.self_sub_stage_id != s_cur.self_sub_stage_id,		-- Под-Статус изменился	
		    1,
		    s_prev.self_sub_stage_duration_calendar_days + 1
		) 													AS self_sub_stage_duration_calendar_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    s_prev.self_order_id = 0										-- Новый заказ
		    	OR s_prev.self_sub_stage_id != s_cur.self_sub_stage_id,		-- Под-Статус изменился
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	1,
		    	0
		    ),	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	s_prev.self_sub_stage_duration_work_days + 1,
		    	s_prev.self_sub_stage_duration_work_days
		    )
		)													AS self_sub_stage_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    s_prev.self_order_id = 0										-- Новый заказ
		    	OR s_prev.self_sub_stage_id != s_cur.self_sub_stage_id,		-- Под-Статус изменился		
		    cte_self_sub_stage_iteration.max_num_iteration + 1,				-- Наращиваем iteration на +1		
		    s_prev.self_sub_stage_iteration									-- Оставляем текущий iteration
		) 													AS self_sub_stage_iteration
		,s_cur.self_has_std1								AS self_has_std1
		,s_cur.self_is_deficit_std1							AS self_is_deficit_std1
		,s_cur.self_total_sum_std1							AS self_total_sum_std1
		,s_cur.self_has_std2								AS self_has_std2
		,s_cur.self_is_deficit_std2							AS self_is_deficit_std2
		,s_cur.self_total_sum_std2							AS self_total_sum_std2
		,s_cur.self_has_mip									AS self_has_mip
		,s_cur.self_is_deficit_mip							AS self_is_deficit_mip
		,s_cur.self_total_sum_mip							AS self_total_sum_mip
		,s_cur.self_has_kich								AS self_has_kich
		,s_cur.self_is_deficit_kich							AS self_is_deficit_kich
		,s_cur.self_total_sum_kich							AS self_total_sum_kich
		,s_cur.self_has_stor								AS self_has_stor
		,s_cur.self_is_deficit_stor							AS self_is_deficit_stor
		,s_cur.self_total_sum_stor							AS self_total_sum_stor
		,s_cur.self_has_matr								AS self_has_matr
		,s_cur.self_is_deficit_matr							AS self_is_deficit_matr
		,s_cur.self_total_sum_matr							AS self_total_sum_matr
		,s_cur.self_has_tech								AS self_has_tech
		,s_cur.self_is_deficit_tech							AS self_is_deficit_tech
		,s_cur.self_total_sum_tech							AS self_total_sum_tech
		,s_cur.self_has_other								AS self_has_other
		,s_cur.self_is_deficit_other						AS self_is_deficit_other
		,s_cur.self_total_sum_other							AS self_total_sum_other
		,s_cur.self_has_serv								AS self_has_serv
		,s_cur.self_total_sum_serv							AS self_total_sum_serv
		,s_cur.self_has_total_products						AS self_has_total_products
		,s_cur.self_has_total_deficit						AS self_has_total_deficit
		,s_cur.self_is_partial_full_order_exit				AS self_is_partial_full_order_exit
		,s_cur.self_deficit_info							AS self_deficit_info	
		,good.art_id										AS art_id
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,toInt64(s_cur.self_order_id) * 1_000_000_000 
			+ toInt64(good.art_id)							AS key_fk_dec_self_order_id_art_id
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,good.order_guid_uid								AS order_guid_uid		
		,good.nomenclature_id								AS nomenclature_id 
		,good.nomenclature_guid_uid							AS nomenclature_guid_uid
		,good.nomenclature_name								AS nomenclature_name
		,good.nomenclature_property_id						AS nomenclature_property_id
		,good.nomenclature_property_guid_uid				AS nomenclature_property_guid_uid
		,good.nomenclature_property_name					AS nomenclature_property_name
		,good.type_id										AS type_id
		,good.type_name										AS type_name
		,clsf.pao_brand_id									AS pao_brand_id
		,clsf.pao_brand_name								AS pao_brand_name
		,clsf.pao_group_id									AS pao_group_id
		,clsf.pao_group_name								AS pao_group_name
		,clsf.pao_collection_id								AS pao_collection_id
		,clsf.pao_collection_name							AS pao_collection_name
		,clsf.pao_color_id									AS pao_color_id
		,clsf.pao_color_name								AS pao_color_name
		,clsf.pao_collection_color_name						AS pao_collection_color_name
		,clsf.ang_brand_id									AS ang_brand_id
		,clsf.ang_brand_name								AS ang_brand_name
		,clsf.ang_group_id									AS ang_group_id
		,clsf.ang_group_name								AS ang_group_name
		,clsf.ang_collection_id								AS ang_collection_id
		,clsf.ang_collection_name							AS ang_collection_name
		,clsf.ang_color_id									AS ang_color_id
		,clsf.ang_color_name								AS ang_color_name
		,clsf.ang_collection_color_name						AS ang_collection_color_name
		,clsf.ang_collection_pao_color_name					AS ang_collection_pao_color_name
		,clsf.ang_kind_id									AS ang_kind_id
		,clsf.ang_kind_name									AS ang_kind_name		
		,good.module_is_ispolzuemaya_create_date			AS module_is_ispolzuemaya_create_date
		,good.module_status_create_date_number				AS module_status_create_date_number
		,good.module_status_create_date_short_name			AS module_status_create_date_short_name
		,good.module_status_create_date_name				AS module_status_create_date_name
		,multiIf(	
        	-- Статусы товаров в открытых заказах	
			good.row_source =  'OPEN_ORDER_TODAY'			
			,multiIf(
				s_cur.self_stage_id <= 20 							-- (не оплачен или у дизайнера)	
						,s_cur.self_sub_stage_id
				,s_cur.self_stage_id = 80							-- (заказ-услуга)
	 					,s_cur.self_sub_stage_id
	 			,good.type_id = 14									-- (услуга)
	 					,710 												-- Номенклатура в доставке
	 			,s_cur.self_stage_id = 30							-- (ждет рапределения)
	 					,s_cur.self_sub_stage_id
	 			,(good.module_open_order_count > 0 					-- (есть в 1С УТ)
					AND good.kolichestvo_dolg = 0 					-- (уже нет в 1С ОПН)
					AND s_cur.self_stage_id > 30 					-- (уже были в 1С ОПН)
				 )
						,710 												-- Номенклатура в доставке
				,s_cur.self_stage_id = 40							-- (отказ клиента)
						,s_cur.self_sub_stage_id
				,(
					(
					 good.is_iz_proizvodstva_rasp = 1 				-- (из приоизводства)
				  		OR good.is_s_uchetom_vypuska_rasp = 1		-- (с учетом выпуска)
				  	) 
					AND good.is_iz_gorizonta = 1   					-- (из горизонта)
					AND good.module_is_ispolzuemaya_create_date = 0 -- (неиспользумая)
				 )
						,510												-- Нет НЕиспользуемой номенклетуры
				,(
					good.is_iz_proizvodstva_rasp = 1 				-- (из приоизводства)
				  		OR good.is_s_uchetom_vypuska_rasp = 1		-- (с учетом выпуска)
				 ) 
				 		,520												-- Нет используемой номенклетуры
						,610 												-- Ждет отгрузку в Воронеже
			)	
			-- Статусы завершенных товаров
			,good.row_source = 'EXIT_ORDER_YESTERDAY'				
			,multiIf(			
				good.sale_total_sum > 0		
					,910 --(Реализация товаров)	
				,good.correct_total_sum	> 0	
					,920 --(Закрытие товаров)	
					,930 --(Прочее выбытие товаров)	
			)			
		    , 999 --(Неизвестная причина завершения)						
		)													AS module_sub_stage_id
		,good.module_has_std1								AS module_has_std1
		,good.module_is_deficit_std1						AS module_is_deficit_std1
		,good.module_has_std2								AS module_has_std2
		,good.module_is_deficit_std2						AS module_is_deficit_std2
		,good.module_has_mip								AS module_has_mip
		,good.module_is_deficit_mip							AS module_is_deficit_mip
		,good.module_has_kich								AS module_has_kich
		,good.module_is_deficit_kich						AS module_is_deficit_kich
		,good.module_has_stor								AS module_has_stor
		,good.module_is_deficit_stor						AS module_is_deficit_stor
		,good.module_has_matr								AS module_has_matr
		,good.module_is_deficit_matr						AS module_is_deficit_matr
		,good.module_has_tech								AS module_has_tech
		,good.module_is_deficit_tech						AS module_is_deficit_tech
		,good.module_has_other								AS module_has_other
		,good.module_is_deficit_other						AS module_is_deficit_other
		,good.module_has_serv								AS module_has_serv
		,good.module_deficit_info							AS module_deficit_info
		,good.kolichestvo_dolg								AS kolichestvo_dolg
		,good.kolichestvo_v_rezerve							AS kolichestvo_v_rezerve
		,good.kolichestvo_podtverzhdeno						AS kolichestvo_podtverzhdeno
		,good.kolichestvo_zayavka_na_otgruzku				AS kolichestvo_zayavka_na_otgruzku
		,good.kolichestvo_iz_regiona_zayavka_na_otgruzku	AS kolichestvo_iz_regiona_zayavka_na_otgruzku
		,good.kolichestvo_iz_tranzita_zayavka_na_otgruzku	AS kolichestvo_iz_tranzita_zayavka_na_otgruzku
		,good.kolichestvo_raspredeleno_modul_rasp			AS kolichestvo_raspredeleno_modul_rasp
		,good.kolichestvo_iz_vypuska_rasp					AS kolichestvo_iz_vypuska_rasp
		,good.kolichestvo_iz_regionov_rasp					AS kolichestvo_iz_regionov_rasp
		,good.kolichestvo_so_sklada_regiona_rasp			AS kolichestvo_so_sklada_regiona_rasp
		,good.kolichestvo_so_sklada_transit_rasp			AS kolichestvo_so_sklada_transit_rasp
		,good.kolichestvo_ochered_rasp						AS kolichestvo_ochered_rasp
		,good.kolichestvo_v_rezerve_rasp					AS kolichestvo_v_rezerve_rasp
		,good.kolichestvo_v_zayavkah_rasp					AS kolichestvo_v_zayavkah_rasp
		,good.kolichestvo_komplektno_rasp					AS kolichestvo_komplektno_rasp
		,good.is_s_uchetom_vypuska_rasp						AS is_s_uchetom_vypuska_rasp
		,good.is_iz_proizvodstva_rasp						AS is_iz_proizvodstva_rasp
		,good.is_iz_reserva_rasp							AS is_iz_reserva_rasp
		,good.is_iz_regiona_rasp							AS is_iz_regiona_rasp
		,good.is_iz_postupleniya_rasp						AS is_iz_postupleniya_rasp
		,good.is_iz_plana_rasp								AS is_iz_plana_rasp
		,good.is_iz_gorizonta_rasp							AS is_iz_gorizonta_rasp
		,good.is_iz_gorizonta								AS is_iz_gorizonta
		,good.is_s_uchetom_regionov_rasp					AS is_s_uchetom_regionov_rasp
		,good.is_s_uchetom_postupleniya_v_region_rasp		AS is_s_uchetom_postupleniya_v_region_rasp
		,good.is_s_uchetom_postupleniya_v_transit_rasp		AS is_s_uchetom_postupleniya_v_transit_rasp
		,good.module_ves_brutto_na_dolg						AS module_ves_brutto_na_dolg
		,good.module_obyom_brutto_na_dolg					AS module_obyom_brutto_na_dolg
		,good.module_open_order_count						AS module_open_order_count
		,good.module_open_order_total_sum					AS module_open_order_total_sum
		,good.module_avg_price								AS module_avg_price
		,good.module_kolichestvo_total						AS module_kolichestvo_total
	FROM
		cte_goods_cur 							AS good  
		LEFT JOIN cte_self_cur 					AS s_cur 
			ON good.order_id = s_cur.self_order_id
		LEFT JOIN cte_self_prev 				AS s_prev
			ON good.order_id = s_prev.self_order_id
		LEFT JOIN cte_self_stage_iteration 		AS cte_self_stage_iteration
			ON  s_cur.self_order_id = cte_self_stage_iteration.self_order_id
        	AND	s_cur.self_stage_id = cte_self_stage_iteration.self_stage_id
		LEFT JOIN cte_self_sub_stage_iteration 	AS cte_self_sub_stage_iteration
			ON 	s_cur.self_order_id = cte_self_sub_stage_iteration.self_order_id
        	AND	s_cur.self_sub_stage_id = cte_self_sub_stage_iteration.self_sub_stage_id
		LEFT JOIN cte_full_cur 					AS f_cur 
			ON s_cur.full_order_id = f_cur.full_order_id
		LEFT JOIN cte_full_prev 				AS f_prev 
			ON s_cur.full_order_id = f_prev.full_order_id	
		LEFT JOIN cte_full_stage_iteration 		AS cte_full_stage_iteration
			ON  f_cur.full_order_id = cte_full_stage_iteration.full_order_id
        	AND	f_cur.full_stage_id = cte_full_stage_iteration.full_stage_id
		LEFT JOIN cte_full_sub_stage_iteration 	AS cte_full_sub_stage_iteration
			ON 	f_cur.full_order_id = cte_full_sub_stage_iteration.full_order_id
        	AND	f_cur.full_sub_stage_id = cte_full_sub_stage_iteration.full_sub_stage_id		
		LEFT JOIN cte_unit_self 				AS s_unit
			ON s_cur.self_unit_id = s_unit.unit_id
		LEFT JOIN cte_unit_full 				AS f_unit 
			ON f_cur.full_unit_id = f_unit.unit_id
		LEFT JOIN cte_class 					AS clsf 
			ON good.art_id = clsf.art_id
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		LEFT JOIN from_mssql.vw_calendar		AS cdr
			ON good.date_id = cdr.date_id
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
)
,
-- Товары заказов на Вчера
cte_module_prev AS
(
	SELECT 						-- ??? строк ('2026-09-28' - INTERVAL 1 DAY)
		my.key_fk_dec_self_order_id_art_id														AS key_fk_dec_self_order_id_art_id
		,argMax(my.module_source_stage_id,                    				my.create_dttm)		AS module_source_stage_id
		,argMax(my.module_stage_id,                    						my.create_dttm)		AS module_stage_id
		,argMax(my.module_sub_stage_id,                    					my.create_dttm)		AS module_sub_stage_id
		,argMax(my.module_stage_duration_calendar_days,                    	my.create_dttm)		AS module_stage_duration_calendar_days
		,argMax(my.module_stage_iteration,                    				my.create_dttm)		AS module_stage_iteration
		,argMax(my.module_sub_stage_duration_calendar_days,                 my.create_dttm)		AS module_sub_stage_duration_calendar_days
		,argMax(my.module_sub_stage_iteration,                    			my.create_dttm)		AS module_sub_stage_iteration
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,argMax(my.module_total_duration_work_days,                    		my.create_dttm)		AS module_total_duration_work_days
		,argMax(my.module_stage_duration_work_days,                    		my.create_dttm)		AS module_stage_duration_work_days
		,argMax(my.module_sub_stage_duration_work_days,                    	my.create_dttm)		AS module_sub_stage_duration_work_days
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
	FROM
		--rasp3_v02.goods_in_order_stage AS my  
		{{ params.db3 }}.goods_in_order_stage AS my  
	WHERE
		my.date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		my.key_fk_dec_self_order_id_art_id	
)
,
-- Кол-во итераций stage_id для key_fk_dec_self_order_id_art_id
cte_module_stage_iteration AS
(
    SELECT						-- ??? строк ('2026-09-28')
        ms.key_fk_dec_self_order_id_art_id	AS key_fk_dec_self_order_id_art_id
        ,ms.module_stage_id					AS module_stage_id
        ,max(ms.module_stage_iteration) 	AS max_num_iteration
    FROM 
    	--rasp3_v02.goods_in_order_stage AS ms
    	{{ params.db3 }}.goods_in_order_stage AS ms
    WHERE 
    	ms.date_id < (SELECT current_date FROM cte_dt_rasp LIMIT 1)
    	AND ms.key_fk_dec_self_order_id_art_id IN
	        (
	            SELECT 
	            	key_fk_dec_self_order_id_art_id
	            FROM 
	            	--rasp2_v02.all_goods
	            	{{ params.db2 }}.all_goods
	            WHERE date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	        )
    GROUP BY
        ms.key_fk_dec_self_order_id_art_id,
        ms.module_stage_id
)		
,
-- Кол-во итераций sub_stage_id для key_fk_dec_self_order_id_art_id
cte_module_sub_stage_iteration AS
(
    SELECT						-- ??? строк ('2026-09-28')
        mb.key_fk_dec_self_order_id_art_id	AS key_fk_dec_self_order_id_art_id
        ,mb.module_sub_stage_id				AS module_sub_stage_id
        ,max(mb.module_sub_stage_iteration) AS max_num_iteration
    FROM 
    	--rasp3_v02.goods_in_order_stage AS mb
    	{{ params.db3 }}.goods_in_order_stage AS mb
    WHERE 
    	mb.date_id < (SELECT current_date FROM cte_dt_rasp LIMIT 1)
    	AND mb.key_fk_dec_self_order_id_art_id IN
	        (
	            SELECT 
					key_fk_dec_self_order_id_art_id
	            FROM 
	            	--rasp2_v02.all_goods
	            	{{ params.db2 }}.all_goods
	            WHERE date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
	        )
    GROUP BY
        mb.key_fk_dec_self_order_id_art_id,
        mb.module_sub_stage_id
)
INSERT INTO {{ params.db3 }}.goods_in_order_stage
--INSERT INTO rasp3_v02.goods_in_order_stage
(
	batch_id_dttm 
	,batch_id_str 
	--,create_dttm 	
	,key_pk_bit_date_id_offset_20250101_self_order_id_art_id 
	,key_pk_hash_date_id_self_order_id_art_id 
	,date_id 	
	,full_order_id 
	,full_order_guid_uid 
	,full_order_number 
	,full_order_datetime 
	,key_fk_dec_date_id_full_order_id 
	,key_fk_hash_date_id_full_order_id 
	,full_unit_id 
	,full_unit_guid_uid 
	,full_unit_name 
	,full_city_id 
	,full_city_guid_uid 
	,full_city_name 
	,full_city_name_clean 
	,full_salon_type_id 
	,full_salon_type_name 
	,full_division_name 
	,full_is_link_order 
	,full_norma_days_dostavki_iz_voronezha 
	,full_calculated_sale_date 
	,full_source_stage_id 
	,full_source_stage_name 
	,full_stage_id 
	,full_stage_name 
	,full_sub_stage_id 
	,full_sub_stage_name 
	,full_total_duration_work_days 
	,full_stage_is_start 
	,full_stage_duration_calendar_days 
	,full_stage_duration_work_days 
	,full_stage_iteration 
	,full_sub_stage_is_start 
	,full_sub_stage_duration_calendar_days 
	,full_sub_stage_duration_work_days 
	,full_sub_stage_iteration 
	,full_has_std1 
	,full_is_deficit_std1 
	,full_total_sum_std1 
	,full_has_std2 
	,full_is_deficit_std2 
	,full_total_sum_std2 
	,full_has_mip 
	,full_is_deficit_mip 
	,full_total_sum_mip 
	,full_has_kich 
	,full_is_deficit_kich 
	,full_total_sum_kich 
	,full_has_stor 
	,full_is_deficit_stor 
	,full_total_sum_stor 
	,full_has_matr 
	,full_is_deficit_matr 
	,full_total_sum_matr 
	,full_has_tech 
	,full_is_deficit_tech 
	,full_total_sum_tech 
	,full_has_other 
	,full_is_deficit_other 
	,full_total_sum_other 
	,full_has_serv 
	,full_total_sum_serv 
	,full_has_total_products 
	,full_has_total_deficit 
	,full_deficit_info 
	,full_total_sum 
	,full_order_count_total 
	,full_number_arts_total 
	,full_total_sum_modules 
	,full_order_count_total_modules 
	,full_number_arts_modules_total 
	,full_total_sum_modules_same_type 
	,full_total_sum_modules_other_type 
	,full_total_sum_deficit_modules_same_type 
	,full_total_sum_deficit_modules_other_type 
	,full_order_count_modules_same_type 
	,full_order_count_modules_other_type 
	,full_order_count_deficit_modules_same_type 
	,full_order_count_deficit_modules_other_type 
	,full_number_arts_modules_same_type 
	,full_number_arts_modules_other_type 
	,full_number_arts_deficit_modules_same_type 
	,full_number_arts_deficit_modules_other_type 
	,full_is_deficit_only_this_module_in_full_order 
	,full_is_deficit_another_module_same_type_in_full_order 
	,full_is_deficit_other_module_other_type_in_full_order 
	,self_order_id 
	,self_order_guid_uid 
	,self_order_number 
	,self_order_datetime 
	,key_fk_dec_date_id_self_order_id 
	,key_fk_hash_date_id_self_order_id 
	,self_unit_id 
	,self_unit_guid_uid 
	,self_unit_name 
	,self_city_id 
	,self_city_guid_uid 
	,self_city_name 
	,self_city_name_clean 
	,self_salon_type_id 
	,self_salon_type_name 
	,self_division_name 
	,client_id 
	,client_guid_uid 
	,client_name 
	,order_phone 
	,employee_id 
	,employee_guid_uid 
	,employee_name 
	,self_is_mip 
	,self_is_kitchen 
	,self_mipik_status_id 
	,self_mipik_status_name 
	,self_mipik_status_order 
	,self_mipik_collection_id 
	,self_mipik_collection_guid_uid 
	,self_mipik_collection_name 
	,self_mipik_assortiment_group_id 
	,self_mipik_assortiment_group_guid_uid 
	,self_mipik_assortiment_group_name 
	,self_mipik_customization_type_id 
	,self_mipik_customization_type_guid_uid 
	,self_mipik_customization_type_name 
	,self_mipik_trek_mik_guid_uid 
	,self_mipik_trek_mik_name 
	,self_max_data_perenosa 
	,self_count_perenosov 
	,self_is_oplachen_roznica 
	,self_is_po_prosbe_clienta_roznica 
	,self_data_dostavki_dogovor_roznica 
	,self_data_dostavki_roznica 
	,self_data_dostavki_dogovor_or_po_prosbe_clienta_roznica 
	,self_data_otsechki 
	,self_data_komplekta 
	,self_data_celevaya 
	,self_data_otgruzki_iz_Voronezh_rasp 
	,self_data_reserva_rasp 
	,self_data_sborki_rasp 
	,self_data_vyhoda_iz_proizvodstva 
	,self_kommentarii_roznica 
	,self_norma_days_dostavki_iz_voronezha 
	,self_calculated_sale_date 
	,self_source_stage_id 
	,self_source_stage_name 
	,self_stage_id 
	,self_stage_name 
	,self_sub_stage_id 
	,self_sub_stage_name 
	,self_total_duration_work_days 
	,self_stage_is_start 
	,self_stage_duration_calendar_days 
	,self_stage_duration_work_days 
	,self_stage_iteration 
	,self_sub_stage_is_start 
	,self_sub_stage_duration_calendar_days 
	,self_sub_stage_duration_work_days 
	,self_sub_stage_iteration 
	,self_has_std1 
	,self_is_deficit_std1 
	,self_total_sum_std1 
	,self_has_std2 
	,self_is_deficit_std2 
	,self_total_sum_std2 
	,self_has_mip 
	,self_is_deficit_mip 
	,self_total_sum_mip 
	,self_has_kich 
	,self_is_deficit_kich 
	,self_total_sum_kich 
	,self_has_stor 
	,self_is_deficit_stor 
	,self_total_sum_stor 
	,self_has_matr 
	,self_is_deficit_matr 
	,self_total_sum_matr 
	,self_has_tech 
	,self_is_deficit_tech 
	,self_total_sum_tech 
	,self_has_other 
	,self_is_deficit_other 
	,self_total_sum_other 
	,self_has_serv 
	,self_total_sum_serv 
	,self_has_total_products 
	,self_has_total_deficit 
	,self_is_partial_full_order_exit 
	,self_deficit_info 
	,self_total_sum 
	,self_order_count_total 
	,self_number_arts_total 
	,self_total_sum_modules 
	,self_order_count_total_modules 
	,self_number_arts_modules_total 
	,self_total_sum_modules_same_type 
	,self_total_sum_modules_other_type 
	,self_total_sum_deficit_modules_same_type 
	,self_total_sum_deficit_modules_other_type 
	,self_order_count_modules_same_type 
	,self_order_count_modules_other_type 
	,self_order_count_deficit_modules_same_type 
	,self_order_count_deficit_modules_other_type 
	,self_number_arts_modules_same_type 
	,self_number_arts_modules_other_type 
	,self_number_arts_deficit_modules_same_type 
	,self_number_arts_deficit_modules_other_type 
	,self_is_deficit_only_this_module_in_self_order 
	,self_is_deficit_another_module_same_type_in_self_order 
	,self_is_deficit_other_module_other_type_in_self_order 
	,art_id 
	,key_fk_dec_self_order_id_art_id 
	,key_fk_hash_self_order_id_art_id 
	,key_fk_dec_date_id_art_id 
	,key_fk_hash_date_id_art_id 
	,nomenclature_id 
	,nomenclature_guid_uid 
	,nomenclature_name 
	,nomenclature_property_id 
	,nomenclature_property_guid_uid 
	,nomenclature_property_name 
	,type_id 
	,type_name 
	,pao_brand_id 
	,pao_brand_name 
	,pao_assortment_group_id 
	,pao_assortment_group_name 
	,pao_collection_id 
	,pao_collection_name 
	,pao_color_id 
	,pao_color_name 
	,pao_collection_color_name 
	,ang_brand_id 
	,ang_brand_name 
	,ang_assortment_group_id 
	,ang_assortment_group_name 
	,ang_collection_id 
	,ang_collection_name 
	,ang_color_id 
	,ang_color_name 
	,ang_collection_color_name 
	,ang_collection_pao_color_name 
	,ang_kind_id 
	,ang_kind_name 
	,module_is_ispolzuemaya_create_date 
	,module_status_create_date_number 
	,module_status_create_date_short_name 
	,module_status_create_date_name 
	,module_source_stage_id 
	,module_source_stage_name 
	,module_stage_id 
	,module_stage_name 
	,module_sub_stage_id 
	,module_sub_stage_name 
	,module_total_duration_work_days 
	,module_stage_is_start 
	,module_stage_duration_calendar_days 
	,module_stage_duration_work_days 
	,module_stage_iteration 
	,module_sub_stage_is_start 
	,module_sub_stage_duration_calendar_days 
	,module_sub_stage_duration_work_days 
	,module_sub_stage_iteration 
	,module_has_std1 
	,module_is_deficit_std1 
	,module_has_std2 
	,module_is_deficit_std2 
	,module_has_mip 
	,module_is_deficit_mip 
	,module_has_kich 
	,module_is_deficit_kich 
	,module_has_stor 
	,module_is_deficit_stor 
	,module_has_matr 
	,module_is_deficit_matr 
	,module_has_tech 
	,module_is_deficit_tech 
	,module_has_other 
	,module_is_deficit_other 
	,module_has_serv 
	,module_deficit_info 
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
	,kolichestvo_komplektno_rasp 
	,module_ves_brutto_na_dolg 
	,module_obyom_brutto_na_dolg 
	,is_s_uchetom_vypuska_rasp 
	,is_s_uchetom_regionov_rasp 
	,is_s_uchetom_postupleniya_v_region_rasp 
	,is_s_uchetom_postupleniya_v_transit_rasp 
	,is_iz_proizvodstva_rasp 
	,is_iz_reserva_rasp 
	,is_iz_regiona_rasp 
	,is_iz_postupleniya_rasp 
	,is_iz_plana_rasp 
	,is_iz_gorizonta_rasp 
	,is_iz_gorizonta 
	,module_open_order_count 
	,module_open_order_total_sum 
	,module_avg_price 
	,module_kolichestvo_total 
	,module_date_exit 
)
--,
--cte_final AS 
--(
	SELECT 								-- 26 156 строк ('2026-09-28')
		cte.batch_id_dttm														AS batch_id_dttm
		,cte.batch_id_str														AS batch_id_str
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,( 
			toInt64(
				dateDiff('day', toDate('20250101'), cte.date_id)
			) * 33_554_432 + toInt64(cte.self_order_id) 
		 ) * 33_554_432 + toInt64(cte.art_id) 									AS key_pk_bit_date_id_offset_20250101_self_order_id_art_id
		,cityHash64(
 		   tuple(cte.date_id, cte.self_order_id, cte.art_id)
		)																		AS key_pk_hash_date_id_self_order_id_art_id							
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,cte.date_id															AS date_id
		,cte.full_order_id														AS full_order_id
		,cte.full_order_guid_uid												AS full_order_guid_uid
		,cte.full_order_number													AS full_order_number
		,cte.full_order_datetime												AS full_order_datetime
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,toInt64(toYYYYMMDD(cte.date_id)) * 100_000_000_000 
			+ toInt64(cte.full_order_id)										AS key_fk_dec_date_id_full_order_id
		,cityHash64(
		    tuple(cte.date_id, cte.full_order_id)
		)																		AS key_fk_hash_date_id_full_order_id
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,cte.full_unit_id														AS full_unit_id
		,cte.full_unit_guid_uid													AS full_unit_guid_uid
		,cte.full_unit_name														AS full_unit_name
		,cte.full_city_id														AS full_city_id
		,cte.full_city_guid_uid													AS full_city_guid_uid
		,cte.full_city_name														AS full_city_name
		,cte.full_city_name_clean												AS full_city_name_clean
		,cte.full_salon_type_id													AS full_salon_type_id
		,cte.full_salon_type_name												AS full_salon_type_name
		,cte.full_division_name													AS full_division_name
		,cte.full_is_link_order													AS full_is_link_order
		,cte.full_norma_days_dostavki_iz_voronezha								AS full_norma_days_dostavki_iz_voronezha
		,max(
			addDays(
				cte.self_data_otgruzki_iz_Voronezh_rasp
				,cte.self_norma_days_dostavki_iz_voronezha
			)
		) 	
			OVER (
				PARTITION BY cte.full_order_id
			) 																	AS full_calculated_sale_date
		,cte.full_source_stage_id																AS full_source_stage_id
		,dictGet('rasp2.dict_source_stage', 'source_stage_name', cte.full_source_stage_id) 		AS full_source_stage_name
		,cte.full_stage_id																		AS full_stage_id
		,dictGet('rasp2.dict_order_stage', 'order_stage_name', cte.full_stage_id) 				AS full_stage_name
		,cte.full_sub_stage_id																	AS full_sub_stage_id
		,dictGet('rasp2.dict_order_sub_stage', 'order_sub_stage_name', cte.full_sub_stage_id) 	AS full_sub_stage_name			
		,cte.full_total_duration_work_days										AS full_total_duration_work_days
		,cte.full_stage_is_start												AS full_stage_is_start
		,cte.full_stage_duration_calendar_days									AS full_stage_duration_calendar_days
		,cte.full_stage_duration_work_days										AS full_stage_duration_work_days
		,cte.full_stage_iteration												AS full_stage_iteration		
		,cte.full_sub_stage_is_start											AS full_sub_stage_is_start
		,cte.full_sub_stage_duration_calendar_days								AS full_sub_stage_duration_calendar_days
		,cte.full_sub_stage_duration_work_days									AS full_sub_stage_duration_work_days
		,cte.full_sub_stage_iteration											AS full_sub_stage_iteration
		,cte.full_has_std1														AS full_has_std1
		,cte.full_is_deficit_std1												AS full_is_deficit_std1
		,cte.full_total_sum_std1												AS full_total_sum_std1
		,cte.full_has_std2														AS full_has_std2
		,cte.full_is_deficit_std2												AS full_is_deficit_std2
		,cte.full_total_sum_std2												AS full_total_sum_std2
		,cte.full_has_mip														AS full_has_mip
		,cte.full_is_deficit_mip 												AS full_is_deficit_mip
		,cte.full_total_sum_mip													AS full_total_sum_mip
		,cte.full_has_kich														AS full_has_kich
		,cte.full_is_deficit_kich												AS full_is_deficit_kich
		,cte.full_total_sum_kich												AS full_total_sum_kich
		,cte.full_has_stor														AS full_has_stor
		,cte.full_is_deficit_stor												AS full_is_deficit_stor
		,cte.full_total_sum_stor												AS full_total_sum_stor
		,cte.full_has_matr														AS full_has_matr
		,cte.full_is_deficit_matr												AS full_is_deficit_matr
		,cte.full_total_sum_matr												AS full_total_sum_matr
		,cte.full_has_tech														AS full_has_tech
		,cte.full_is_deficit_tech												AS full_is_deficit_tech
		,cte.full_total_sum_tech												AS full_total_sum_tech
		,cte.full_has_other														AS full_has_other
		,cte.full_is_deficit_other												AS full_is_deficit_other
		,cte.full_total_sum_other												AS full_total_sum_other
		,cte.full_has_serv														AS full_has_serv
		,cte.full_total_sum_serv												AS full_total_sum_serv
		,cte.full_has_total_products											AS full_has_total_products
		,cte.full_has_total_deficit												AS full_has_total_deficit
		,cte.full_deficit_info													AS full_deficit_info
		-- Сумма в деньгах по заказу (товары + услуги)
		,sumIf(
			cte.module_open_order_total_sum,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
		)
			OVER (PARTITION BY cte.full_order_id)								AS full_total_sum
		-- Количество штук номенклатур в заказе (товары + услуги)
		,sumIf(
			cte.module_open_order_count,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
		)
			OVER (PARTITION BY cte.full_order_id)								AS full_order_count_total
		-- Количество номенклатур в заказе (товары + услуги)
		,uniqExactIf(
			cte.art_id,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
		)
			OVER (PARTITION BY cte.full_order_id)								AS full_number_arts_total
		-- Сумма в деньгах по товарам заказа (без услуг)
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id)								AS full_total_sum_modules
		-- Количество штук товаров в заказе (без услуг)
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id)								AS full_order_count_total_modules
		-- Количество номенклатур товаров в заказе (без услуг)
		,uniqExactIf(
			cte.art_id,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id)								AS full_number_arts_modules_total
		-- Сумма в деньгах товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_total_sum_modules_same_type
		-- Сумма в деньгах товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id)
		-
		sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_total_sum_modules_other_type
		-- Сумма в деньгах дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_total_sum_deficit_modules_same_type		
		-- Сумма в деньгах дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id)
		-
		sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_total_sum_deficit_modules_other_type
		-- Количество штук товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_order_count_modules_same_type
		-- Количество штук товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id)
		-
		sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_order_count_modules_other_type
		-- Количество штук дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_order_count_deficit_modules_same_type		
		-- Количество штук дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id)
		-
		sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_order_count_deficit_modules_other_type			
		-- Количество номенклатур товаров в заказе (без услуг) того же типа, что и в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_number_arts_modules_same_type
		-- Количество номенклатур товаров в заказе (без услуг) другого типа, чем в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id)
		-
		uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_number_arts_modules_other_type
		-- Количество номенклатур дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_number_arts_deficit_modules_same_type		
		-- Количество номенклатур дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id)
		-
		uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.full_order_id, cte.type_id)					AS full_number_arts_deficit_modules_other_type			
		-- Есть дефицит только по этому модулю в full_order
		,if(
			dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50													-- stage_id = 50 - Нет продукции этой строки
			AND 
			-- Полное Количество номенклатур дефицитных товаров в заказе (без услуг) всех типов
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.full_order_id) = 1					-- Дефицит только по одной номенклатуре в full_order_id (= по этой строке)
			,1
			,0
		)																		AS full_is_deficit_only_this_module_in_full_order	
		-- Есть ли в full_order ещё дефицит по номенклатуре того же типа, что и в этой строке (кроме дефицита текущей строки)
		,if(
			dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50												-- stage_id = 50 - Нет продукции этой строки
			AND 
			-- Количество номенклатур дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.full_order_id, cte.type_id) > 1	-- Дефицит не только по одной номенклатуре в full_order_id (не только по этой строке)
			,1
			,0
		)																		AS full_is_deficit_another_module_same_type_in_full_order
		-- Есть ли дефицит в full_order по модулям других типов
		,if(
			-- Количество номенклатур дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.full_order_id)
			-
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.full_order_id, cte.type_id) > 0	-- Кол-во дефицитных номенклатур другого типа > 0
			,1
			,0
		)																		AS full_is_deficit_other_module_other_type_in_full_order
		,cte.self_order_id														AS self_order_id
		,cte.self_order_guid_uid												AS self_order_guid_uid
		,cte.self_order_number													AS self_order_number
		,cte.self_order_datetime												AS self_order_datetime
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,toInt64(toYYYYMMDD(cte.date_id)) * 100_000_000_000 
			+ toInt64(cte.self_order_id)										AS key_fk_dec_date_id_self_order_id
		,cityHash64(
		    tuple(cte.date_id, cte.self_order_id)
		)																		AS key_fk_hash_date_id_self_order_id
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,cte.self_unit_id														AS self_unit_id
		,cte.self_unit_guid_uid													AS self_unit_guid_uid
		,cte.self_unit_name														AS self_unit_name
		,cte.self_city_id														AS self_city_id
		,cte.self_city_guid_uid													AS self_city_guid_uid
		,cte.self_city_name														AS self_city_name
		,cte.self_city_name_clean												AS self_city_name_clean
		,cte.self_salon_type_id													AS self_salon_type_id
		,cte.self_salon_type_name												AS self_salon_type_name
		,cte.self_division_name													AS self_division_name
		,cte.client_id															AS client_id
		,cte.client_guid_uid													AS client_guid_uid
		,cte.client_name														AS client_name
		,cte.order_phone														AS order_phone
		,cte.employee_id														AS employee_id
		,cte.employee_guid_uid													AS employee_guid_uid
		,cte.employee_name														AS employee_name
		,cte.self_is_mip														AS self_is_mip
		,cte.self_is_kitchen													AS self_is_kitchen
		,cte.self_mipik_status_id												AS self_mipik_status_id
		,cte.self_mipik_status_name												AS self_mipik_status_name
		,cte.self_mipik_status_order											AS self_mipik_status_order
		,cte.self_mipik_collection_id											AS self_mipik_collection_id
		,cte.self_mipik_collection_guid_uid										AS self_mipik_collection_guid_uid
		,cte.self_mipik_collection_name											AS self_mipik_collection_name	
		,cte.self_mipik_assortiment_group_id									AS self_mipik_assortiment_group_id
		,cte.self_mipik_assortiment_group_guid_uid								AS self_mipik_assortiment_group_guid_uid
		,cte.self_mipik_assortiment_group_name									AS self_mipik_assortiment_group_name	
		,cte.self_mipik_customization_type_id									AS self_mipik_customization_type_id
		,cte.self_mipik_customization_type_guid_uid								AS self_mipik_customization_type_guid_uid
		,cte.self_mipik_customization_type_name									AS self_mipik_customization_type_name
		,cte.self_mipik_trek_mik_guid_uid										AS self_mipik_trek_mik_guid_uid
		,cte.self_mipik_trek_mik_name											AS self_mipik_trek_mik_name
		,cte.self_max_data_perenosa												AS self_max_data_perenosa
		,cte.self_count_perenosov												AS self_count_perenosov
		,cte.self_is_oplachen_roznica											AS self_is_oplachen_roznica
		,cte.self_is_po_prosbe_clienta_roznica									AS self_is_po_prosbe_clienta_roznica
		,cte.self_data_dostavki_dogovor_roznica									AS self_data_dostavki_dogovor_roznica
		,cte.self_data_dostavki_roznica											AS self_data_dostavki_roznica
		,cte.self_data_dostavki_dogovor_or_po_prosbe_clienta_roznica			AS self_data_dostavki_dogovor_or_po_prosbe_clienta_roznica
		,cte.self_data_otsechki													AS self_data_otsechki
		,cte.self_data_komplekta												AS self_data_komplekta
		,cte.self_data_celevaya													AS self_data_celevaya
		,cte.self_data_otgruzki_iz_Voronezh_rasp								AS self_data_otgruzki_iz_Voronezh_rasp
		,cte.self_data_reserva_rasp												AS self_data_reserva_rasp
		,cte.self_data_sborki_rasp												AS self_data_sborki_rasp
		,cte.self_data_vyhoda_iz_proizvodstva									AS self_data_vyhoda_iz_proizvodstva	
		,cte.self_kommentarii_roznica											AS self_kommentarii_roznica
		,cte.self_norma_days_dostavki_iz_voronezha								AS self_norma_days_dostavki_iz_voronezha
		,cte.self_calculated_sale_date											AS self_calculated_sale_date
		,cte.self_source_stage_id																AS self_source_stage_id
		,dictGet('rasp2.dict_source_stage', 'source_stage_name', cte.self_source_stage_id)		AS self_source_stage_name
		,cte.self_stage_id																		AS self_stage_id
		,dictGet('rasp2.dict_order_stage', 'order_stage_name', cte.self_stage_id) 				AS self_stage_name
		,cte.self_sub_stage_id																	AS self_sub_stage_id
		,dictGet('rasp2.dict_order_sub_stage', 'order_sub_stage_name', cte.self_sub_stage_id) 	AS self_sub_stage_name
		,cte.self_total_duration_work_days										AS self_total_duration_work_days
		,cte.self_stage_is_start												AS self_stage_is_start
		,cte.self_stage_duration_calendar_days									AS self_stage_duration_calendar_days
		,cte.self_stage_duration_work_days										AS self_stage_duration_work_days
		,cte.self_stage_iteration												AS self_stage_iteration
		,cte.self_sub_stage_is_start											AS self_sub_stage_is_start
		,cte.self_sub_stage_duration_calendar_days								AS self_sub_stage_duration_calendar_days
		,cte.self_sub_stage_duration_work_days									AS self_sub_stage_duration_work_days
		,cte.self_sub_stage_iteration											AS self_sub_stage_iteration
		,cte.self_has_std1														AS self_has_std1
		,cte.self_is_deficit_std1												AS self_is_deficit_std1
		,cte.self_total_sum_std1												AS self_total_sum_std1
		,cte.self_has_std2														AS self_has_std2
		,cte.self_is_deficit_std2												AS self_is_deficit_std2
		,cte.self_total_sum_std2												AS self_total_sum_std2
		,cte.self_has_mip														AS self_has_mip
		,cte.self_is_deficit_mip												AS self_is_deficit_mip
		,cte.self_total_sum_mip													AS self_total_sum_mip
		,cte.self_has_kich														AS self_has_kich
		,cte.self_is_deficit_kich												AS self_is_deficit_kich
		,cte.self_total_sum_kich												AS self_total_sum_kich
		,cte.self_has_stor														AS self_has_stor
		,cte.self_is_deficit_stor												AS self_is_deficit_stor
		,cte.self_total_sum_stor												AS self_total_sum_stor
		,cte.self_has_matr														AS self_has_matr
		,cte.self_is_deficit_matr												AS self_is_deficit_matr
		,cte.self_total_sum_matr												AS self_total_sum_matr
		,cte.self_has_tech														AS self_has_tech
		,cte.self_is_deficit_tech												AS self_is_deficit_tech
		,cte.self_total_sum_tech												AS self_total_sum_tech
		,cte.self_has_other														AS self_has_other
		,cte.self_is_deficit_other												AS self_is_deficit_other								
		,cte.self_total_sum_other												AS self_total_sum_other
		,cte.self_has_serv														AS self_has_serv
		,cte.self_total_sum_serv												AS self_total_sum_serv
		,cte.self_has_total_products											AS self_has_total_products
		,cte.self_has_total_deficit												AS self_has_total_deficit
		,cte.self_is_partial_full_order_exit									AS self_is_partial_full_order_exit
		,cte.self_deficit_info													AS self_deficit_info
		-- Сумма в деньгах по заказу (товары + услуги)
		,sumIf(
			cte.module_open_order_total_sum,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
		)
			OVER (PARTITION BY cte.self_order_id)								AS self_total_sum
		-- Количество штук номенклатур в заказе (товары + услуги)
		,sumIf(
			cte.module_open_order_count,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
		)
			OVER (PARTITION BY cte.self_order_id)								AS self_order_count_total
		-- Количество номенклатур в заказе (товары + услуги)
		,uniqExactIf(
			cte.art_id,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
		)
			OVER (PARTITION BY cte.self_order_id)								AS self_number_arts_total
		-- Сумма в деньгах по товарам заказа (без услуг)
		,sumIf(
			cte.module_open_order_total_sum, 		
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id)								AS self_total_sum_modules
		-- Количество штук товаров в заказе (без услуг)
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id)								AS self_order_count_total_modules
		-- Количество номенклатур товаров в заказе (без услуг)
		,uniqExactIf(
			cte.art_id,
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id)								AS self_number_arts_modules_total
		-- Сумма в деньгах товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_total_sum_modules_same_type
		-- Сумма в деньгах товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id)
		-
		sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_total_sum_modules_other_type
		-- Сумма в деньгах дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_total_sum_deficit_modules_same_type		
		-- Сумма в деньгах дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id)
		-
		sumIf(
			cte.module_open_order_total_sum, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_total_sum_deficit_modules_other_type
		-- Количество штук товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_order_count_modules_same_type
		-- Количество штук товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id)
		-
		sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_order_count_modules_other_type
		-- Количество штук дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_order_count_deficit_modules_same_type		
		-- Количество штук дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
		,sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id)
		-
		sumIf(
			cte.module_open_order_count, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_order_count_deficit_modules_other_type			
		-- Количество номенклатур товаров в заказе (без услуг) того же типа, что и в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_number_arts_modules_same_type
		-- Количество номенклатур товаров в заказе (без услуг) другого типа, чем в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id)
		-
		uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_number_arts_modules_other_type
		-- Количество номенклатур дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_number_arts_deficit_modules_same_type		
		-- Количество номенклатур дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
		,uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id)
		-
		uniqExactIf(
			cte.art_id, 
			dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
				AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
				AND dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50								-- stage_id = 50 - Нет продукции	
		)								
			OVER (PARTITION BY cte.self_order_id, cte.type_id)					AS self_number_arts_deficit_modules_other_type			
		-- Есть дефицит только по этому модулю в self_order
		,if(
			dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50													-- stage_id = 50 - Нет продукции этой строки
			AND 
			-- Полное Количество номенклатур дефицитных товаров в заказе (без услуг) всех типов
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.self_order_id) = 1					-- Дефицит только по одной номенклатуре в self_order_id (= по этой строке)
			,1
			,0
		)																		AS self_is_deficit_only_this_module_in_self_order	
		-- Есть ли в self_order ещё дефицит по номенклатуре того же типа, что и в этой строке (кроме дефицита текущей строки)
		,if(
			dictGet(
						'rasp2.dict_order_sub_stage'
						,'order_stage_id'
						,cte.module_sub_stage_id	
					) = 50												-- stage_id = 50 - Нет продукции этой строки
			AND 
			-- Количество номенклатур дефицитных товаров в заказе (без услуг) того же типа, что и в этой строке
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.self_order_id, cte.type_id) > 1	-- Дефицит не только по одной номенклатуре в self_order_id (не только по этой строке)
			,1
			,0
		)																		AS self_is_deficit_another_module_same_type_in_self_order
		-- Есть ли дефицит в self_order по модулям других типов
		,if(
			-- Количество номенклатур дефицитных товаров в заказе (без услуг) другого типа, чем в этой строке
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.self_order_id)
			-
			uniqExactIf(
				cte.art_id, 
				dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) != 90	-- stage_id = 90 - Завершение
					AND cte.type_id != 14							-- cte.type_id = 14 - Услуги
					AND dictGet(
							'rasp2.dict_order_sub_stage'
							,'order_stage_id'
							,cte.module_sub_stage_id	
						) = 50								-- stage_id = 50 - Нет продукции	
			)								
				OVER (PARTITION BY cte.self_order_id, cte.type_id) > 0	-- Кол-во дефицитных номенклатур другого типа > 0
			,1
			,0
		)																		AS self_is_deficit_other_module_other_type_in_self_order
		,cte.art_id																AS art_id
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		,cte.key_fk_dec_self_order_id_art_id									AS key_fk_dec_self_order_id_art_id
		,cityHash64(
		    tuple(cte.self_order_id, cte.art_id)
		)																		AS key_fk_hash_self_order_id_art_id	
		,toInt64(toYYYYMMDD(cte.date_id)) * 100_000_000_000 
			+ toInt64(cte.art_id)												AS key_fk_dec_date_id_art_id
		,cityHash64(
		    tuple(cte.date_id, cte.art_id)
		)																		AS key_fk_hash_date_id_art_id
		-- mg-07.11.2026 - Новые поля -----------------------------------------------------------------------------------------------------------
		--,cte.order_guid_uid		
		,cte.nomenclature_id 													AS nomenclature_id
		,cte.nomenclature_guid_uid												AS nomenclature_guid_uid
		,cte.nomenclature_name													AS nomenclature_name
		,cte.nomenclature_property_id											AS nomenclature_property_id
		,cte.nomenclature_property_guid_uid										AS nomenclature_property_guid_uid
		,cte.nomenclature_property_name											AS nomenclature_property_name
		,cte.type_id															AS type_id
		,cte.type_name															AS type_name
		,cte.pao_brand_id														AS pao_brand_id
		,cte.pao_brand_name														AS pao_brand_name
		,cte.pao_group_id														AS pao_assortment_group_id 
		,cte.pao_group_name														AS pao_assortment_group_name 
		,cte.pao_collection_id													AS pao_collection_id
		,cte.pao_collection_name												AS pao_collection_name
		,cte.pao_color_id														AS pao_color_id
		,cte.pao_color_name														AS pao_color_name
		,cte.pao_collection_color_name											AS pao_collection_color_name
		,cte.ang_brand_id														AS ang_brand_id
		,cte.ang_brand_name														AS ang_brand_name
		,cte.ang_group_id														AS ang_assortment_group_id
		,cte.ang_group_name														AS ang_assortment_group_name 
		,cte.ang_collection_id													AS ang_collection_id
		,cte.ang_collection_name												AS ang_collection_name
		,cte.ang_color_id														AS ang_color_id
		,cte.ang_color_name														AS ang_color_name
		,cte.ang_collection_color_name											AS ang_collection_color_name
		,cte.ang_collection_pao_color_name										AS ang_collection_pao_color_name
		,cte.ang_kind_id														AS ang_kind_id
		,cte.ang_kind_name														AS ang_kind_name	
		,cte.module_is_ispolzuemaya_create_date									AS module_is_ispolzuemaya_create_date
		,cte.module_status_create_date_number									AS module_status_create_date_number
		,cte.module_status_create_date_short_name								AS module_status_create_date_short_name
		,cte.module_status_create_date_name										AS module_status_create_date_name
		,dictGet(
			'rasp2.dict_order_stage'
			,'source_stage_id'
			,dictGet(
				'rasp2.dict_order_sub_stage'
				,'order_stage_id'
				,cte.module_sub_stage_id	
			) 	
		) 																		AS module_source_stage_id
		,dictGet(
			'rasp2.dict_source_stage'
			,'source_stage_name'
			,dictGet(
				'rasp2.dict_order_stage'
				,'source_stage_id'
				,dictGet(
					'rasp2.dict_order_sub_stage'
					,'order_stage_id'
					,cte.module_sub_stage_id	
				) 	
			)
		) 																		AS module_source_stage_name
		,dictGet(
			'rasp2.dict_order_sub_stage'
			,'order_stage_id'
			,cte.module_sub_stage_id	
		) 																		AS module_stage_id
		,dictGet(
			'rasp2.dict_order_stage'
			,'order_stage_name'
			,dictGet(
				'rasp2.dict_order_sub_stage'
				,'order_stage_id'
				,cte.module_sub_stage_id	
			) 	
		) 																		AS module_stage_name
		,cte.module_sub_stage_id												AS module_sub_stage_id	
		,dictGet(
			'rasp2.dict_order_sub_stage'
			,'order_sub_stage_name'
			,cte.module_sub_stage_id	
		) 																		AS module_sub_stage_name		
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
			cdr.previous_is_work_day = 1,							-- Вчера - рабочий день
			cte_module_prev.module_total_duration_work_days + 1,	-- Если во вчерашнем срезе заказа не было, то cte_module_prev.module_total_duration_work_days = 0
			cte_module_prev.module_total_duration_work_days
		)																		AS module_total_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0																			-- Новый заказ
		    	OR cte_module_prev.module_stage_id != dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id),	-- Статус изменился	
		    1,
		    0
		) 																		AS module_stage_is_start
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0																			-- Новый заказ
		    	OR cte_module_prev.module_stage_id != dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id),	-- Статус изменился	
		    1,
		    cte_module_prev.module_stage_duration_calendar_days + 1
		) 																		AS module_stage_duration_calendar_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0																			-- Новый заказ
		    	OR cte_module_prev.module_stage_id != dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id),	-- Статус изменился
		 	if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	1,
		    	0
		    ),	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	cte_module_prev.module_stage_duration_work_days + 1,
		    	cte_module_prev.module_stage_duration_work_days
		    )
		)																		AS module_stage_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0																			-- Новый заказ
		    	OR cte_module_prev.module_stage_id != dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id),	-- Статус изменился		
		    cte_module_stage_iteration.max_num_iteration + 1,									-- Наращиваем iteration на +1		
		    cte_module_prev.module_stage_iteration												-- Оставляем текущий iteration
		) 																		AS module_stage_iteration
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0							-- Новый заказ
		    	OR cte_module_prev.module_sub_stage_id != cte.module_sub_stage_id,		-- Под-Статус изменился	
		    1,
		    0
		) 																		AS module_sub_stage_is_start
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0							-- Новый заказ
		    	OR cte_module_prev.module_sub_stage_id != cte.module_sub_stage_id,		-- Под-Статус изменился	
		    1,
		    cte_module_prev.module_sub_stage_duration_calendar_days + 1
		) 																		AS module_sub_stage_duration_calendar_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0							-- Новый заказ
		    	OR cte_module_prev.module_sub_stage_id != cte.module_sub_stage_id,		-- Под-Статус изменился
		   	if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	1,
		    	0
		    ),	
		    if(
		    	cdr.previous_is_work_day = 1,						-- Вчера - рабочий день
		    	cte_module_prev.module_sub_stage_duration_work_days + 1,
		    	cte_module_prev.module_sub_stage_duration_work_days
		    )
		)																		AS module_sub_stage_duration_work_days
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		,if(
		    cte_module_prev.key_fk_dec_self_order_id_art_id = 0							-- Новый заказ
		    	OR cte_module_prev.module_sub_stage_id != cte.module_sub_stage_id,		-- Под-Статус изменился		
		    cte_module_sub_stage_iteration.max_num_iteration + 1,				-- Наращиваем iteration на +1		
		    cte_module_prev.module_sub_stage_iteration							-- Оставляем текущий iteration
		) 																		AS module_sub_stage_iteration	
		,cte.module_has_std1													AS module_has_std1
		,cte.module_is_deficit_std1												AS module_is_deficit_std1
		,cte.module_has_std2													AS module_has_std2
		,cte.module_is_deficit_std2												AS module_is_deficit_std2
		,cte.module_has_mip														AS module_has_mip
		,cte.module_is_deficit_mip												AS module_is_deficit_mip
		,cte.module_has_kich													AS module_has_kich
		,cte.module_is_deficit_kich												AS module_is_deficit_kich
		,cte.module_has_stor													AS module_has_stor
		,cte.module_is_deficit_stor												AS module_is_deficit_stor
		,cte.module_has_matr													AS module_has_matr
		,cte.module_is_deficit_matr												AS module_is_deficit_matr
		,cte.module_has_tech													AS module_has_tech
		,cte.module_is_deficit_tech												AS module_is_deficit_tech
		,cte.module_has_other													AS module_has_other
		,cte.module_is_deficit_other											AS module_is_deficit_other
		,cte.module_has_serv													AS module_has_serv
		,cte.module_deficit_info												AS module_deficit_info
		,cte.kolichestvo_dolg													AS kolichestvo_dolg
		,cte.kolichestvo_v_rezerve												AS kolichestvo_v_rezerve
		,cte.kolichestvo_podtverzhdeno											AS kolichestvo_podtverzhdeno
		,cte.kolichestvo_zayavka_na_otgruzku									AS kolichestvo_zayavka_na_otgruzku
		,cte.kolichestvo_iz_regiona_zayavka_na_otgruzku							AS kolichestvo_iz_regiona_zayavka_na_otgruzku
		,cte.kolichestvo_iz_tranzita_zayavka_na_otgruzku						AS kolichestvo_iz_tranzita_zayavka_na_otgruzku
		,cte.kolichestvo_raspredeleno_modul_rasp								AS kolichestvo_raspredeleno_modul_rasp
		,cte.kolichestvo_iz_vypuska_rasp										AS kolichestvo_iz_vypuska_rasp
		,cte.kolichestvo_iz_regionov_rasp										AS kolichestvo_iz_regionov_rasp
		,cte.kolichestvo_so_sklada_regiona_rasp									AS kolichestvo_so_sklada_regiona_rasp
		,cte.kolichestvo_so_sklada_transit_rasp									AS kolichestvo_so_sklada_transit_rasp
		,cte.kolichestvo_ochered_rasp											AS kolichestvo_ochered_rasp
		,cte.kolichestvo_v_rezerve_rasp											AS kolichestvo_v_rezerve_rasp
		,cte.kolichestvo_v_zayavkah_rasp										AS kolichestvo_v_zayavkah_rasp
		,cte.kolichestvo_komplektno_rasp										AS kolichestvo_komplektno_rasp
		,cte.module_ves_brutto_na_dolg											AS module_ves_brutto_na_dolg
		,cte.module_obyom_brutto_na_dolg										AS module_obyom_brutto_na_dolg
		,cte.is_s_uchetom_vypuska_rasp											AS is_s_uchetom_vypuska_rasp
		,cte.is_s_uchetom_regionov_rasp											AS is_s_uchetom_regionov_rasp
		,cte.is_s_uchetom_postupleniya_v_region_rasp							AS is_s_uchetom_postupleniya_v_region_rasp
		,cte.is_s_uchetom_postupleniya_v_transit_rasp							AS is_s_uchetom_postupleniya_v_transit_rasp	
		,cte.is_iz_proizvodstva_rasp											AS is_iz_proizvodstva_rasp
		,cte.is_iz_reserva_rasp													AS is_iz_reserva_rasp
		,cte.is_iz_regiona_rasp													AS is_iz_regiona_rasp
		,cte.is_iz_postupleniya_rasp											AS is_iz_postupleniya_rasp
		,cte.is_iz_plana_rasp													AS is_iz_plana_rasp
		,cte.is_iz_gorizonta_rasp												AS is_iz_gorizonta_rasp
		,cte.is_iz_gorizonta													AS is_iz_gorizonta
		,cte.module_open_order_count											AS module_open_order_count
		,cte.module_open_order_total_sum										AS module_open_order_total_sum
		,cte.module_avg_price													AS module_avg_price
		,cte.module_kolichestvo_total											AS module_kolichestvo_total
		,if(
			dictGet(
				'rasp2.dict_order_sub_stage'
				,'order_stage_id'
				,cte.module_sub_stage_id	
			) = 90
			,subtractDays(cte.date_id, 1)
			,toDate('1970-01-01')
		)																		AS module_date_exit
	FROM
		cte_total 									AS cte
		LEFT JOIN cte_module_prev 					AS cte_module_prev
			ON cte.key_fk_dec_self_order_id_art_id = cte_module_prev.key_fk_dec_self_order_id_art_id
		LEFT JOIN cte_module_stage_iteration 		AS cte_module_stage_iteration
			ON cte.key_fk_dec_self_order_id_art_id = cte_module_stage_iteration.key_fk_dec_self_order_id_art_id
			AND dictGet('rasp2.dict_order_sub_stage', 'order_stage_id', cte.module_sub_stage_id) = cte_module_stage_iteration.module_stage_id
		LEFT JOIN cte_module_sub_stage_iteration 	AS cte_module_sub_stage_iteration
			ON cte.key_fk_dec_self_order_id_art_id = cte_module_sub_stage_iteration.key_fk_dec_self_order_id_art_id
			AND cte.module_sub_stage_id = cte_module_sub_stage_iteration.module_sub_stage_id
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
		LEFT JOIN from_mssql.vw_calendar			AS cdr
			ON cte.date_id = cdr.date_id
		-- mg-07.11.2026 -----------------------------------------------------------------------------------------------------------
--)
-- Итоговая сборка
--SELECT * FROM cte_final								-- 26 156 строк ('2026-09-28')
--SELECT * FROM cte_final WHERE full_order_id = 0
--SELECT * FROM cte_final WHERE cte.module_stage_id = 90 ORDER BY module_date_exit
--SELECT * FROM cte_final WHERE cte.self_stage_id = 90
--SELECT * FROM cte_final WHERE cte.full_stage_id = 90
--SELECT * FROM cte_total --WHERE module_sub_stage_id = 0
--SELECT * FROM cte_total						-- 25 828 строк ('2026-09-28')
--SELECT * FROM cte_class						-- 7 670 строк ('2026-09-28')
--SELECT * FROM cte_unit_full					-- 809 строк ('2026-09-28')
--SELECT * FROM cte_unit_self					-- 809 строк ('2026-09-28')
--SELECT * FROM cte_full_prev					-- 5 070 строк ('2026-09-28' - INTERVAL 1 DAY)
--SELECT * FROM cte_self_prev					-- ??? строк ('2026-09-28' - INTERVAL 1 DAY)
--SELECT * FROM cte_self_stage_iteration 		-- ??? строк ('2026-09-28')
--SELECT * FROM cte_self_sub_stage_iteration 	-- ??? строк ('2026-09-28')
--SELECT * FROM cte_full_cur					-- 5 216 строк ('2026-09-28')
--SELECT * FROM cte_self_cur					-- 6 303 строк ('2026-09-28')
--SELECT * FROM cte_goods_cur 					-- 25 828 строк ('2026-09-28')
---------------------------------------------------------------------------------------------------------------------------------
--SELECT
--    count() AS rows_count,
--    uniqExact(
--        date_id,
--        self_order_id,
--        art_id
--    ) AS unique_keys,
--    rows_count - unique_keys AS duplicates
--FROM cte_total;
---------------------------------------------------------------------------------------------------------------------------------
--SELECT
--    date_id,
--    self_order_id,
--    art_id,
--    count() AS cnt
--FROM cte_total
--GROUP BY
--    date_id,
--    self_order_id,
--    art_id
--HAVING cnt > 1
--ORDER BY cnt DESC
--LIMIT 100;
---------------------------------------------------------------------------------------------------------------------------------
--SELECT
--    date_id,
--    self_order_id,
--    uniqExact(self_stage_id) AS stages,
--    uniqExact(self_sub_stage_id) AS sub_stages
--FROM cte_total
--GROUP BY
--    date_id,
--    self_order_id
--HAVING stages > 1
--    OR sub_stages > 1
--ORDER BY date_id, self_order_id;
---------------------------------------------------------------------------------------------------------------------------------
--SELECT
--    date_id,
--    self_order_id,
--    count() AS arts_count
--FROM cte_total
--GROUP BY
--    date_id,
--    self_order_id
--ORDER BY arts_count DESC
--LIMIT 20;


