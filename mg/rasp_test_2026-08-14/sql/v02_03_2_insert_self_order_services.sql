-- "db1": DB1
-- "db3": DB3
-- "days_back": 0, 1 или 2
-- Запрос-3.2. Для открытых заказов, содержащих только услуги (нет ни одной номенклатуры товара)
WITH 
--	0 AS days_back
--	,toDate('2026-08-03') 							AS cte_current_snapshot_date
--	--,cte_current_snapshot_date - INTERVAL 1 DAY 	AS cte_previous_snapshot_date_1
--	,subtractDays(cte_current_snapshot_date, 1) 	AS cte_previous_snapshot_date
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
		--,subtractDays(toDate(datetime_id), days_back) 		AS current_date 		-- Текущий день (для которого обрабатываем данные)
		,subtractDays(
			toDate(datetime_id),
			{{ params.days_back }} + 1
		) 													AS previous_date		-- Вчерашний день
		--,subtractDays(toDate(datetime_id), days_back + 1) 	AS previous_date		-- Вчерашний день
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-08-03')
	WHERE batch_id_str = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_str") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
cte_prnt AS						-- 5000 строк
(
	SELECT DISTINCT
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
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
)
,
-- Связанные заказы
rd_parent_all AS				-- 5000 строк
(
	SELECT DISTINCT
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
		ANY LEFT JOIN 	-- Может быть несколько подчиненных заказов. Нужен только один факт подчиненного заказа -> ANY JOIN
			(
				SELECT DISTINCT 
					parent_order_guid_uid		-- GUID Заказ покупателя розница в 1С ОПН
	 		 	FROM 
	 		 		cte_prnt
		 	 ) 									AS slv
		 	ON pnt.order_guid_uid = slv.parent_order_guid_uid
) 
,
-- Открытые заказы на ТЕКУЩУЮ дату (ТОЛЬКО заказы, содержащие только услуги!)
rd_current_service AS 				-- 700 строк
(
	SELECT 
		order_guid_uid														AS order_guid_uid
		,date_id															AS date_id
		,argMin(order_id, order_doc_date)									AS order_id
		,argMin(order_doc_num, order_doc_date)								AS order_doc_num
		,min(order_doc_date)												AS order_doc_datetime
		,argMin(unit_guid_uid, order_doc_date)								AS unit_guid_uid
		,argMin(unit_name, order_doc_date)									AS unit_name
		,argMin(city_guid_uid, order_doc_date)								AS city_guid_uid
		,argMin(city_name, order_doc_date)									AS city_name		
		,sum(total_sum)														AS total_sum_total		
		,argMin(client_id, order_doc_date)									AS client_id
		,argMin(client_name, order_doc_date)								AS client_name
		,argMin(client_guid_uid, order_doc_date)							AS client_guid_uid
		,argMin(order_phone, order_doc_date)								AS order_phone
		,argMin(employee_id, order_doc_date)								AS employee_id
		,argMin(employee_name, order_doc_date)								AS employee_name
		,argMin(employee_guid_uid, order_doc_date)							AS employee_guid_uid
		,argMin(order_is_individual, order_doc_date)						AS is_mip
		,argMin(order_is_kitchen, order_doc_date)							AS is_kitchen		
		,argMin(order_mipik_collection_id, order_doc_date)					AS mipik_collection_id
		,argMin(order_mipik_collection_name, order_doc_date)				AS mipik_collection_name
		,argMin(order_mipik_collection_guid_uid, order_doc_date)			AS mipik_collection_guid_uid
		,argMin(order_mipik_assortiment_group_id, order_doc_date)			AS mipik_assortiment_group_id
		,argMin(order_mipik_assortiment_group_name, order_doc_date)			AS mipik_assortiment_group_name
		,argMin(order_mipik_assortiment_group_guid_uid, order_doc_date)		AS mipik_assortiment_group_guid_uid
		,argMin(order_mipik_customization_type_id, order_doc_date)			AS mipik_customization_type_id
		,argMin(order_mipik_customization_type_name, order_doc_date)		AS mipik_customization_type_name
		,argMin(order_mipik_customization_type_guid_uid, order_doc_date)	AS mipik_customization_type_guid_uid	
--		,sum(order_count)													AS order_count_total_serv
--		,sumIf(order_count, type_id  = 14)									AS order_count_service		-- type_id = 14  - Услуги
		,sumIf(order_count, type_id != 14)									AS order_count_not_service	-- type_id != 14 - Товары
		,sumIf(total_sum, 	type_id  = 14)									AS total_sum_service		-- type_id = 14  - Услуги
		,sumIf(total_sum,	type_id != 14)									AS total_sum_not_service	-- type_id != 14 - Товары
	FROM 
		rasp2.open_orders_goods_history_rn
	WHERE
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
	GROUP BY 
		order_guid_uid
		,date_id
	HAVING
		order_count_not_service	<= 0		-- Нет товаров (шт.)
		AND total_sum_not_service <= 0		-- Нет товаров (руб.)
)
,
-- Открытые заказы на ПРЕДЫДУЩУЮ дату
rd_previous_all AS				-- 5000 строк
(
	SELECT 
		order_guid_uid							AS order_guid_uid
		,min(order_id)							AS order_id
		,sum(order_count)						AS order_count_total
		,sumIf(order_count, type_id  = 14)		AS order_count_service		-- type_id = 14  - Услуги
		,sumIf(order_count, type_id != 14)		AS order_count_not_service	-- type_id != 14 - Товары
		,sum(total_sum)							AS total_sum_total
		,sumIf(total_sum, 	type_id  = 14)		AS total_sum_service		-- type_id = 14  - Услуги
		,sumIf(total_sum,	type_id != 14)		AS total_sum_not_service	-- type_id != 14 - Товары
	FROM 
		rasp2.open_orders_goods_history_rn
	WHERE
		date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_previous_snapshot_date
	GROUP BY 
		order_guid_uid
)
,
-- Выбытие заказов в ПРЕДЫДУЩУЮ дату
rd_exit_previous_all AS			-- 100-1000 строк
(
	SELECT
		order_guid_uid								AS order_guid_uid
		-- (1) Эти количества и суммы вчера выбыли из открытых заказов ----------------------------------------------
		,sum(order_count)							AS order_count_total
		,sumIf(order_count, type_id = 14)			AS order_count_service				-- type_id = 14  - Услуги
		,sumIf(order_count, type_id != 14)			AS order_count_not_service			-- type_id != 14 - Товары
		,sum(total_sum)								AS total_sum_total
		,sumIf(total_sum, type_id = 14)				AS total_sum_service				-- type_id = 14  - Услуги
		,sumIf(total_sum, type_id != 14)			AS total_sum_not_service			-- type_id != 14 - Товары
		-- (1) Эти количества и суммы вчера выбыли из открытых заказов ----------------------------------------------
		-- (2) Из (1) эти были закрыты (скорректированы) ------------------------------------------------------------
		,sum(correct_order_count)					AS correct_order_count_total
		,sumIf(correct_order_count, type_id = 14)	AS correct_order_count_service		-- type_id = 14  - Услуги
		,sumIf(correct_order_count, type_id != 14)	AS correct_order_count_not_service	-- type_id != 14 - Товары
		,sum(correct_total_sum)						AS correct_total_sum_total
		,sumIf(correct_total_sum, type_id = 14)		AS correct_total_sum_service		-- type_id = 14  - Услуги
		,sumIf(correct_total_sum, type_id != 14)	AS correct_total_sum_not_service	-- type_id != 14 - Товары
		-- (2) Из (1) эти были закрыты (скорректированы) ------------------------------------------------------------
		-- (3) Из (1) эти были реализованы (отгружены) --------------------------------------------------------------
		,sum(sale_order_count)						AS sale_order_count_total
		,sumIf(sale_order_count, type_id = 14)		AS sale_order_count_service			-- type_id = 14  - Услуги
		,sumIf(sale_order_count, type_id != 14)		AS sale_order_count_not_service		-- type_id != 14 - Товары
		,sum(sale_total_sum)						AS sale_total_sum_total
		,sumIf(sale_total_sum, type_id = 14)		AS sale_total_sum_service			-- type_id = 14  - Услуги
		,sumIf(sale_total_sum, type_id != 14)		AS sale_total_sum_not_service		-- type_id != 14 - Товары
		-- (3) Из (1) эти были реализованы (отгружены) --------------------------------------------------------------
		-- (4) Из (1) какая-то часть была не закрыта и не отгружена [ = (1) - (2) - (3) ]----------------------------
	FROM 
		rasp2.order_exit_goods_rn_history
	WHERE 
		date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_previous_snapshot_date
	GROUP BY 
		order_guid_uid
)
,
-- Статусы заказов в ПРЕДЫДУЩУЮ дату
self_order_previous_all AS		-- 5000 строк
(
	SELECT
		order_roznica_guid_uid				AS order_roznica_guid_uid
		,source_stage_id					AS source_stage_id
		,order_stage_id						AS order_stage_id
		,order_sub_stage_id					AS order_sub_stage_id
	FROM 
		--rasp3_v02.self_order
		{{ params.db3 }}.self_order
	WHERE
		date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_previous_snapshot_date
		AND order_stage_id != 90					-- 90 - exit-признак (выбытие заказа = отгрузка или закрытие или прочее)
)
,
-- Статусы МИП
mip AS							-- 2000 строк
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
	 	point_date = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		--point_date = cte_previous_snapshot_date 
	GROUP BY
		toUUIDOrZero(order_guid_1C)
)
,
cte AS (
	SELECT 
		-- DEFAULT	--	batch_id_dttm							
		-- DEFAULT	--	,batch_id_str							
		-- DEFAULT	--	,create_dttm
		rd_cur.date_id 									AS date_id								--	,date_id 
		,rd_cur.order_id								AS order_id								--	,order_id
		,rd_cur.order_guid_uid 							AS order_guid_uid						--	,order_roznica_guid_uid 
		,rd_cur.order_doc_num 							AS order_doc_num						--	,order_roznica_doc_num
		,rd_cur.order_doc_datetime 						AS order_doc_datetime						--	,order_roznica_doc_datetime
		,rd_cur.unit_guid_uid 							AS unit_guid_uid						--	,unit_guid_uid
		,rd_cur.unit_name 								AS unit_name							--	,unit_name
		,rd_cur.city_guid_uid 							AS city_guid_uid						--	,city_guid_uid
		,rd_cur.city_name 								AS city_name							--	,city_name
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_cur.order_id	
				,prnt.full_order_id
			)											AS full_order_id						--	,full_order_id
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_cur.order_guid_uid 	
				,prnt.full_order_roznica_guid_uid
			)											AS full_order_roznica_guid_uid			--	,full_order_roznica_guid_uid 
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_cur.order_doc_num
				,prnt.full_order_roznica_number
			)											AS full_order_roznica_number			--	,full_order_roznica_number
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_cur.order_doc_datetime	
				,prnt.full_order_roznica_date
			)											AS full_order_roznica_datetime			--	,full_order_roznica_datetime
		,prnt.is_link_order								AS is_link_order						--	,is_link_order 
		,rd_cur.total_sum_total 						AS total_sum_total						--	,total_sum_total 
		,3												AS source_stage_id						--	,source_stage_id 
		,'' 											AS source_stage_name					--	,source_stage_name 
		,80												AS order_stage_id						--	,order_stage_id 
		,'' 											AS order_stage_name						--	,order_stage_name 
		,multiIf(
			rd_prev.order_id = 0												-- Вчера не было заказа		
												, 840										-- Заказ-услуга
			,self_prev.order_stage_id = 80
												,self_prev.order_sub_stage_id				-- Как вчера
			,(
				rd_prev.total_sum_not_service > 0								-- Вчера был Заказ с товаром
				AND ex_prev.sale_total_sum_not_service > 0						-- Вчера была Реализация любого товара
			)
												, 810										-- Услуга отгруженного товара
			,(
				rd_prev.total_sum_not_service > 0								-- Вчера был Заказ с товаром
				AND ex_prev.correct_total_sum_not_service > 0					-- Вчера было Закрытие любого товара (или другая корректировка до нуля)
			)
												, 820										-- Услуга закрытого товара
												, 830										-- Услуга исчезнувшего товара							
		) 												AS order_sub_stage_id					--	,order_sub_stage_id 
		,''												AS order_sub_stage_name					--	,order_sub_stage_name 
		,0												AS has_std1								--	,has_std1 
		,0												AS is_deficit_std1						--	,is_deficit_std1 	
		,0												AS total_sum_std1						--	,total_sum_std1 
		,0												AS has_std2								--	,has_std2 
		,0												AS is_deficit_std2						--	,is_deficit_std2 
		,0												AS total_sum_std2						--	,total_sum_std2 
		,0												AS has_mip								--	,has_mip 
		,0												AS is_deficit_mip						--	,is_deficit_mip 
		,0												AS total_sum_mip						--	,total_sum_mip 
		,0												AS has_kich								--	,has_kich 
		,0												AS is_deficit_kich						--	,is_deficit_kich 
		,0												AS total_sum_kich						--	,total_sum_kich 
		,0												AS has_stor								--	,has_stor 
		,0												AS is_deficit_stor						--	,is_deficit_stor 
		,0												AS total_sum_stor						--	,total_sum_stor 
		,0												AS has_matr								--	,has_matr 
		,0												AS is_deficit_matr						--	,is_deficit_matr 
		,0												AS total_sum_matr						--	,total_sum_matr 
		,0												AS has_tech								--	,has_tech 
		,0												AS is_deficit_tech						--	,is_deficit_tech 
		,0												AS total_sum_tech						--	,total_sum_tech 
		,0												AS has_other							--	,has_other 
		,0												AS is_deficit_other						--	,is_deficit_other 
		,0												AS total_sum_other						--	,total_sum_other 
		,true											AS has_serv								--	,has_serv 
		,rd_cur.total_sum_service 						AS total_sum_serv						--	,total_sum_serv 
		,0												AS has_total_products					--  ,has_total_products
		,0												AS has_total_deficit					--  ,has_total_deficit
		,0												AS is_partial_full_order_exit			--  ,is_partial_full_order_exit
		,rd_cur.client_id 								AS client_id							--	,client_id								-- V
		,rd_cur.client_name 							AS client_name							--	,client_name							-- V
		,rd_cur.client_guid_uid 						AS client_guid_uid						--	,client_guid_uid						-- V
		,rd_cur.order_phone 							AS order_phone							--	,order_phone							-- V
		,rd_cur.employee_id 							AS employee_id							--	,employee_id							-- V
		,rd_cur.employee_name 							AS employee_name						--	,employee_name							-- V
		,rd_cur.employee_guid_uid 						AS employee_guid_uid					--	,employee_guid_uid						-- V
		,rd_cur.is_mip 									AS is_mip								--	,is_mip									-- V
		,rd_cur.is_kitchen 								AS is_kitchen							--	,is_kitchen								-- V
		,mip.mipik_status_id 							AS mipik_status_id						--	,mipik_status_id						-- V -- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
		,mip.mipik_status_name 							AS mipik_status_name					--	,mipik_status_name						-- V
		,mip.mipik_status_order 						AS mipik_status_order					--	,mipik_status_order						-- V
		,rd_cur.mipik_collection_id 					AS mipik_collection_id					--	,mipik_collection_id					-- V
		,rd_cur.mipik_collection_name 					AS mipik_collection_name				--	,mipik_collection_name 					-- V
		,rd_cur.mipik_collection_guid_uid 				AS mipik_collection_guid_uid			--	,mipik_collection_guid_uid 				-- V
		,rd_cur.mipik_assortiment_group_id 				AS mipik_assortiment_group_id			--	,mipik_assortiment_group_id 			-- V
		,rd_cur.mipik_assortiment_group_name 			AS mipik_assortiment_group_name			--	,mipik_assortiment_group_name 			-- V
		,rd_cur.mipik_assortiment_group_guid_uid 		AS mipik_assortiment_group_guid_uid		--	,mipik_assortiment_group_guid_uid 		-- V
		,rd_cur.mipik_customization_type_id 			AS mipik_customization_type_id			--	,mipik_customization_type_id 			-- V
		,rd_cur.mipik_customization_type_name 			AS mipik_customization_type_name		--	,mipik_customization_type_name 			-- V
		,rd_cur.mipik_customization_type_guid_uid 		AS mipik_customization_type_guid_uid	--	,mipik_customization_type_guid_uid 		-- V
		-- DEFAULT	--	,mipik_trek_mik_name					-- V
		-- DEFAULT	--	,mipik_trek_mik_guid_uid				-- V
		-- DEFAULT	--	,max_data_perenosa 						-- V
		-- DEFAULT	--	--,count_perenosov				-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
		-- DEFAULT	--	--,row_number_parametry_zakaza	-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
		-- DEFAULT	--	,is_oplachen_roznica 					-- V
		-- DEFAULT	--	,is_po_prosbe_clienta_roznica 			-- V 
		-- DEFAULT	--	,data_dostavki_dogovor_roznica 			-- V
		-- DEFAULT	--	,data_dostavki_roznica 					-- V
		-- DEFAULT	--	,data_otsechki 							-- V
		-- DEFAULT	--	,data_komplekta 						-- V
		-- DEFAULT	--	,data_celevaya 							-- V
		-- DEFAULT	--	,kommentarii_roznica					-- V
		-- DEFAULT	--	--,kolichestvo_dolg_total			-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить	
		-- DEFAULT	--	,norma_days_dostavki_iz_voronezha		-- V
	FROM 
		rd_current_service AS rd_cur									-- 700 строк
		LEFT JOIN rd_previous_all AS rd_prev 							-- 5000 строк
			ON rd_cur.order_guid_uid = rd_prev.order_guid_uid
		LEFT JOIN rd_exit_previous_all AS ex_prev						-- 100-1000 строк
			ON rd_cur.order_guid_uid = ex_prev.order_guid_uid
		LEFT JOIN self_order_previous_all AS self_prev					-- 5000 строк
			ON rd_cur.order_guid_uid = self_prev.order_roznica_guid_uid
		LEFT JOIN rd_parent_all AS prnt									-- 5000 строк 
			ON rd_cur.order_guid_uid = prnt.order_guid_uid
		LEFT JOIN mip AS mip											-- 2000 строк
			ON rd_cur.order_guid_uid = mip.order_guid_uid
)
INSERT INTO {{ params.db3 }}.self_order(
--INSERT INTO rasp3_v02.self_order(
--	DEFAULT	--batch_id_dttm							
--	DEFAULT --,batch_id_str							
--	DEFAULT --,create_dttm
	date_id 
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
	,has_total_products					-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,has_total_deficit					-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	,is_partial_full_order_exit			-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
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
--	DEFAULT	--	,mipik_trek_mik_name					-- V
--	DEFAULT	--	,mipik_trek_mik_guid_uid				-- V
--	DEFAULT	--,max_data_perenosa 						-- V
--	DEFAULT	--,count_perenosov				-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
--	DEFAULT --,row_number_parametry_zakaza	-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
--	DEFAULT --,is_oplachen_roznica 						-- V
--	DEFAULT --,is_po_prosbe_clienta_roznica 			-- V 
--	DEFAULT --,data_dostavki_dogovor_roznica 			-- V
--	DEFAULT --,data_dostavki_roznica 					-- V
--	DEFAULT --,data_otsechki 							-- V
--	DEFAULT --,data_komplekta 							-- V
--	DEFAULT --,data_celevaya 							-- V
--	DEFAULT --,kommentarii_roznica						-- V
--	DEFAULT --,kolichestvo_dolg_total			-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить	
--	DEFAULT --,norma_days_dostavki_iz_voronezha			-- V
)
SELECT 
	-- DEFAULT	--	batch_id_dttm							
	-- DEFAULT	--	,batch_id_str							
	-- DEFAULT	--	,create_dttm
	cte.date_id 										AS date_id								--	,date_id 
	,any(cte.order_id)									AS order_id								--	,order_id
	,cte.order_guid_uid 								AS order_guid_uid						--	,order_roznica_guid_uid 
	,any(cte.order_doc_num) 							AS order_doc_num						--	,order_roznica_doc_num
	,any(cte.order_doc_datetime)						AS order_doc_datetime						--	,order_roznica_doc_datetime
	,any(cte.unit_guid_uid) 							AS unit_guid_uid						--	,unit_guid_uid
	,any(cte.unit_name) 								AS unit_name							--	,unit_name
	,any(cte.city_guid_uid) 							AS city_guid_uid						--	,city_guid_uid
	,any(cte.city_name) 								AS city_name							--	,city_name
	,any(cte.full_order_id)								AS full_order_id						--	,full_order_id
	,any(cte.full_order_roznica_guid_uid)				AS full_order_roznica_guid_uid			--	,full_order_roznica_guid_uid 
	,any(cte.full_order_roznica_number)					AS full_order_roznica_number			--	,full_order_roznica_number
	,any(cte.full_order_roznica_datetime)				AS full_order_roznica_datetime			--	,full_order_roznica_datetime
	,any(cte.is_link_order)								AS is_link_order						--	,is_link_order 
	,any(cte.total_sum_total) 							AS total_sum_total						--	,total_sum_total 
	,any(cte.source_stage_id)							AS source_stage_id						--	,source_stage_id 
	,any(cte.source_stage_name)							AS source_stage_name					--	,source_stage_name 
	,any(cte.order_stage_id)							AS order_stage_id						--	,order_stage_id 
	,any(cte.order_stage_name)							AS order_stage_name						--	,order_stage_name 
	,any(cte.order_sub_stage_id) 						AS order_sub_stage_id					--	,order_sub_stage_id 
	,any(cte.order_sub_stage_name)						AS order_sub_stage_name					--	,order_sub_stage_name 
	,any(cte.has_std1)									AS has_std1								--	,has_std1 
	,any(cte.is_deficit_std1)							AS is_deficit_std1						--	,is_deficit_std1 	
	,any(cte.total_sum_std1)							AS total_sum_std1						--	,total_sum_std1 
	,any(cte.has_std2)									AS has_std2								--	,has_std2 
	,any(cte.is_deficit_std2)							AS is_deficit_std2						--	,is_deficit_std2 
	,any(cte.total_sum_std2)							AS total_sum_std2						--	,total_sum_std2 
	,any(cte.has_mip)									AS has_mip								--	,has_mip 
	,any(cte.is_deficit_mip)							AS is_deficit_mip						--	,is_deficit_mip 
	,any(cte.total_sum_mip)								AS total_sum_mip						--	,total_sum_mip 
	,any(cte.has_kich)									AS has_kich								--	,has_kich 
	,any(cte.is_deficit_kich)							AS is_deficit_kich						--	,is_deficit_kich 
	,any(cte.total_sum_kich)							AS total_sum_kich						--	,total_sum_kich 
	,any(cte.has_stor)									AS has_stor								--	,has_stor 
	,any(cte.is_deficit_stor)							AS is_deficit_stor						--	,is_deficit_stor 
	,any(cte.total_sum_stor)							AS total_sum_stor						--	,total_sum_stor 
	,any(cte.has_matr)									AS has_matr								--	,has_matr 
	,any(cte.is_deficit_matr)							AS is_deficit_matr						--	,is_deficit_matr 
	,any(cte.total_sum_matr)							AS total_sum_matr						--	,total_sum_matr 
	,any(cte.has_tech)									AS has_tech								--	,has_tech 
	,any(cte.is_deficit_tech)							AS is_deficit_tech						--	,is_deficit_tech 
	,any(cte.total_sum_tech)							AS total_sum_tech						--	,total_sum_tech 
	,any(cte.has_other)									AS has_other							--	,has_other 
	,any(cte.is_deficit_other)							AS is_deficit_other						--	,is_deficit_other 
	,any(cte.total_sum_other)							AS total_sum_other						--	,total_sum_other 
	,any(cte.has_serv)									AS has_serv								--	,has_serv 
	,any(cte.total_sum_serv) 							AS total_sum_serv						--	,total_sum_serv 
	,any(cte.has_total_products)						AS has_total_products					--  ,has_total_products
	,any(cte.has_total_deficit)							AS has_total_deficit					--  ,has_total_deficit
	,any(cte.is_partial_full_order_exit)				AS is_partial_full_order_exit			--  ,is_partial_full_order_exit
	,any(cte.client_id)									AS client_id							--	,client_id								-- V
	,any(cte.client_name) 								AS client_name							--	,client_name							-- V
	,any(cte.client_guid_uid) 							AS client_guid_uid						--	,client_guid_uid						-- V
	,any(cte.order_phone) 								AS order_phone							--	,order_phone							-- V
	,any(cte.employee_id) 								AS employee_id							--	,employee_id							-- V
	,any(cte.employee_name) 							AS employee_name						--	,employee_name							-- V
	,any(cte.employee_guid_uid) 						AS employee_guid_uid					--	,employee_guid_uid						-- V
	,any(cte.is_mip) 									AS is_mip								--	,is_mip									-- V
	,any(cte.is_kitchen) 								AS is_kitchen							--	,is_kitchen								-- V
	,any(cte.mipik_status_id) 							AS mipik_status_id						--	,mipik_status_id						-- V -- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
	,any(cte.mipik_status_name) 						AS mipik_status_name					--	,mipik_status_name						-- V
	,any(cte.mipik_status_order) 						AS mipik_status_order					--	,mipik_status_order						-- V
	,any(cte.mipik_collection_id) 						AS mipik_collection_id					--	,mipik_collection_id					-- V
	,any(cte.mipik_collection_name) 					AS mipik_collection_name				--	,mipik_collection_name 					-- V
	,any(cte.mipik_collection_guid_uid) 				AS mipik_collection_guid_uid			--	,mipik_collection_guid_uid 				-- V
	,any(cte.mipik_assortiment_group_id) 				AS mipik_assortiment_group_id			--	,mipik_assortiment_group_id 			-- V
	,any(cte.mipik_assortiment_group_name) 				AS mipik_assortiment_group_name			--	,mipik_assortiment_group_name 			-- V
	,any(cte.mipik_assortiment_group_guid_uid) 			AS mipik_assortiment_group_guid_uid		--	,mipik_assortiment_group_guid_uid 		-- V
	,any(cte.mipik_customization_type_id) 				AS mipik_customization_type_id			--	,mipik_customization_type_id 			-- V
	,any(cte.mipik_customization_type_name) 			AS mipik_customization_type_name		--	,mipik_customization_type_name 			-- V
	,any(cte.mipik_customization_type_guid_uid) 		AS mipik_customization_type_guid_uid	--	,mipik_customization_type_guid_uid 		-- V
	-- DEFAULT	--	,mipik_trek_mik_name					-- V
	-- DEFAULT	--	,mipik_trek_mik_guid_uid				-- V
	-- DEFAULT	--	,max_data_perenosa 						-- V
	-- DEFAULT	--	--,count_perenosov				-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	-- DEFAULT	--	--,row_number_parametry_zakaza	-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
	-- DEFAULT	--	,is_oplachen_roznica 					-- V
	-- DEFAULT	--	,is_po_prosbe_clienta_roznica 			-- V 
	-- DEFAULT	--	,data_dostavki_dogovor_roznica 			-- V
	-- DEFAULT	--	,data_dostavki_roznica 					-- V
	-- DEFAULT	--	,data_otsechki 							-- V
	-- DEFAULT	--	,data_komplekta 						-- V
	-- DEFAULT	--	,data_celevaya 							-- V
	-- DEFAULT	--	,kommentarii_roznica					-- V
	-- DEFAULT	--	--,kolichestvo_dolg_total			-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить	
	-- DEFAULT	--	,norma_days_dostavki_iz_voronezha		-- V
FROM 
	cte
GROUP BY 
	cte.date_id								
	,cte.order_guid_uid
;