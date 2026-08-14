-- "db1": DB1
-- "db3": DB3
-- "days_back": 0, 1 или 2
-- Запрос-3.3. Для выбывших заказов вчера
WITH 
--	0 AS days_back
--	,toDate('2026-08-03') 						AS cte_current_snapshot_date
--	--,cte_current_snapshot_date - INTERVAL 1 DAY AS cte_previous_snapshot_date_1
--	,subtractDays(cte_current_snapshot_date, 1) AS cte_previous_snapshot_date
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
-- Связка Заказ - Главный заказ
cte_prnt AS							-- 5000 строк
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
		date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_previous_snapshot_date
)
,
-- Связанные заказы
rd_parent_previous AS				-- 5000 строк
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
-- Открытые заказы на ПРЕДЫДУЩУЮ дату
rd_previous_all AS				-- 5000 строк
(
	SELECT 
		order_guid_uid														AS order_guid_uid		
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
--		,sum(order_count)													AS order_count_total
--		,sumIf(order_count, type_id  = 14)									AS order_count_service		-- type_id = 14  - Услуги
--		,sumIf(order_count, type_id != 14)									AS order_count_not_service	-- type_id != 14 - Товары
--		,sum(total_sum)														AS total_sum_total
--		,sumIf(total_sum, 	type_id  = 14)									AS total_sum_service		-- type_id = 14  - Услуги
--		,sumIf(total_sum,	type_id != 14)									AS total_sum_not_service	-- type_id != 14 - Товары
	FROM 
		rasp2.open_orders_goods_history_rn
	WHERE
		date_id = (SELECT previous_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_previous_snapshot_date
	GROUP BY 
		order_guid_uid
)
,
-- Открытые заказы на ТЕКУЩУЮ дату
rd_current_all AS 				-- 5000 строк
(
	SELECT 
		order_guid_uid							AS order_guid_uid
		,sum(order_count)						AS order_count_total
		,sumIf(order_count, type_id  = 14)		AS order_count_service		-- type_id = 14  - Услуги
		,sumIf(order_count, type_id != 14)		AS order_count_not_service	-- type_id != 14 - Товары
		,sum(total_sum)							AS total_sum_total
		,sumIf(total_sum, 	type_id  = 14)		AS total_sum_service		-- type_id = 14  - Услуги
		,sumIf(total_sum,	type_id != 14)		AS total_sum_not_service	-- type_id != 14 - Товары
	FROM 
		rasp2.open_orders_goods_history_rn
	WHERE
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
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
		slf.order_roznica_guid_uid				AS order_roznica_guid_uid			
		,slf.order_stage_id						AS order_stage_id						--  ,order_stage_id
		,slf.order_sub_stage_id					AS order_sub_stage_id					--  ,order_sub_stage_id
		,slf.has_std1							AS has_std1								--	,has_std1 
		,slf.is_deficit_std1					AS is_deficit_std1						--	,is_deficit_std1 	
		,slf.total_sum_std1						AS total_sum_std1						--	,total_sum_std1 
		,slf.has_std2							AS has_std2								--	,has_std2 
		,slf.is_deficit_std2					AS is_deficit_std2						--	,is_deficit_std2 
		,slf.total_sum_std2						AS total_sum_std2						--	,total_sum_std2 
		,slf.has_mip							AS has_mip								--	,has_mip 
		,slf.is_deficit_mip						AS is_deficit_mip						--	,is_deficit_mip 
		,slf.total_sum_mip						AS total_sum_mip						--	,total_sum_mip 
		,slf.has_kich							AS has_kich								--	,has_kich 
		,slf.is_deficit_kich					AS is_deficit_kich						--	,is_deficit_kich 
		,slf.total_sum_kich						AS total_sum_kich						--	,total_sum_kich 
		,slf.has_stor							AS has_stor								--	,has_stor 
		,slf.is_deficit_stor					AS is_deficit_stor						--	,is_deficit_stor 
		,slf.total_sum_stor						AS total_sum_stor						--	,total_sum_stor 
		,slf.has_matr							AS has_matr								--	,has_matr 
		,slf.is_deficit_matr					AS is_deficit_matr						--	,is_deficit_matr 
		,slf.total_sum_matr						AS total_sum_matr						--	,total_sum_matr 
		,slf.has_tech							AS has_tech								--	,has_tech 
		,slf.is_deficit_tech					AS is_deficit_tech						--	,is_deficit_tech 
		,slf.total_sum_tech						AS total_sum_tech						--	,total_sum_tech 
		,slf.has_other							AS has_other							--	,has_other 
		,slf.is_deficit_other					AS is_deficit_other						--	,is_deficit_other 
		,slf.total_sum_other					AS total_sum_other						--	,total_sum_other 
		,slf.has_serv							AS has_serv								--	,has_serv 
		,slf.total_sum_serv						AS total_sum_serv						--	,total_sum_serv 
		,slf.has_total_products					AS has_total_products					--  ,has_total_products
		,slf.has_total_deficit					AS has_total_deficit					--  ,has_total_deficit
		,slf.is_partial_full_order_exit			AS is_partial_full_order_exit			--  ,is_partial_full_order_exit
	FROM 
		--rasp3_v02.self_order AS slf
		{{ params.db3 }}.self_order AS slf
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
-- Выбывшие вчера заказы
cte_exit_prev AS				-- 100-1000 строк
(
	SELECT 	
		-- DEFAULT	--	batch_id_dttm							
		-- DEFAULT	--	,batch_id_str							
		-- DEFAULT	--	,create_dttm
--		rd_cur.date_id 									AS date_id								--	,date_id 
--		cte_current_snapshot_date						AS date_id								--	,date_id 
		(SELECT current_date FROM cte_dt_rasp LIMIT 1)	AS date_id								--	,date_id 
		,rd_prev.order_id								AS order_id								--	,order_id
		,rd_prev.order_guid_uid 						AS order_guid_uid						--	,order_roznica_guid_uid 
		,rd_prev.order_doc_num 							AS order_doc_num						--	,order_roznica_doc_num
		,rd_prev.order_doc_datetime 					AS order_doc_datetime						--	,order_roznica_doc_datetime
		,rd_prev.unit_guid_uid 							AS unit_guid_uid						--	,unit_guid_uid
		,rd_prev.unit_name 								AS unit_name							--	,unit_name
		,rd_prev.city_guid_uid 							AS city_guid_uid						--	,city_guid_uid
		,rd_prev.city_name 								AS city_name							--	,city_name
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_prev.order_id	
				,prnt.full_order_id
			)											AS full_order_id						--	,full_order_id
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_prev.order_guid_uid 	
				,prnt.full_order_roznica_guid_uid
			)											AS full_order_roznica_guid_uid			--	,full_order_roznica_guid_uid 
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_prev.order_doc_num
				,prnt.full_order_roznica_number
			)											AS full_order_roznica_number			--	,full_order_roznica_number
		,if(
				prnt.full_order_roznica_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
				,rd_prev.order_doc_datetime	
				,prnt.full_order_roznica_date
			)											AS full_order_roznica_datetime			--	,full_order_roznica_datetime
		,prnt.is_link_order								AS is_link_order						--	,is_link_order 
		,rd_prev.total_sum_total 						AS total_sum_total						--	,total_sum_total 
		,4												AS source_stage_id						--	,source_stage_id 
		,'' 											AS source_stage_name					--	,source_stage_name 
		,90												AS order_stage_id						--	,order_stage_id 
		,'' 											AS order_stage_name						--	,order_stage_name 
		,multiIf(
					(
						self_prev.has_total_products = 1				-- В заказе была продукция (не только услуги)	
						AND ex_prev.sale_total_sum_not_service > 0		-- Была реализация продукции (любой номенклатуры)								 		
					)
												,910									-- 910 - Реализация товаров
					,(
						self_prev.has_total_products = 1				-- В заказе была продукция (не только услуги)	
						AND ex_prev.correct_total_sum_not_service > 0	-- Было закрытие продукции (любой номенклатуры)
					)	
												,920									-- 920 - Закрытие товаров
					,(
						self_prev.has_total_products = 1				-- В заказе была продукция (не только услуги)	
						AND ex_prev.sale_total_sum_not_service <= 0		-- Не было реализации продукции (ни одной номенклатуры)
						AND ex_prev.correct_total_sum_not_service <= 0	-- Не было закрытия продукции (ни одной номенклатуры)
					)
												,930									-- 930 - Прочее исчезновение товаров
					,(
						self_prev.order_sub_stage_id = 810				-- Услуга отгруженного товара
						AND ex_prev.sale_total_sum_service > 0			-- Была реализация услуги (любой номенклатуры)
					)	
												,911									-- 911 - Реализация услуги реализованного товара
					,(
						self_prev.order_sub_stage_id = 810				-- Услуга отгруженного товара
						AND ex_prev.correct_total_sum_service > 0		-- Было закрытие услуги (любой номенклатуры)
					)
												,951									-- 951 - Закрытие услуги отгруженного товара
					,(
						self_prev.order_sub_stage_id = 810				-- Услуга отгруженного товара
						AND ex_prev.sale_total_sum_service <= 0			-- Не было реализации услуг (ни одной номенклатуры)
						AND ex_prev.correct_total_sum_service <= 0		-- Не было закрытия услуг (ни одной номенклатуры)
					)
												,952									-- 952 - Прочее исчезновение услуги отгруженного товара
					,(
						self_prev.order_sub_stage_id = 820				-- Услуга закрытого товара
						AND ex_prev.sale_total_sum_service > 0			-- Была реализация услуги (любой номенклатуры)
					)
												,953									-- 953 - Реализация услуги закрытого товара
					,(
						self_prev.order_sub_stage_id = 820				-- Услуга закрытого товара
						AND ex_prev.correct_total_sum_service > 0		-- Было закрытие услуги (любой номенклатуры)
					)
												,921									-- 921 - Закрытие услуги закрытого товара
					,(
						self_prev.order_sub_stage_id = 820				-- Услуга закрытого товара
						AND ex_prev.sale_total_sum_service <= 0			-- Не было реализации услуг (ни одной номенклатуры)
						AND ex_prev.correct_total_sum_service <= 0		-- Не было закрытия услуг (ни одной номенклатуры)
					)
												,954									-- 954 - Исчезновение услуги закрытого товара
					,(
						self_prev.order_sub_stage_id = 830				-- Услуга исчезнувшего товара
						AND ex_prev.sale_total_sum_service > 0			-- Была реализация услуги (любой номенклатуры)
					)
												,955									-- 955 - Реализация услуги исчезнувшего товара
					,(
						self_prev.order_sub_stage_id = 830				-- Услуга исчезнувшего товара
						AND ex_prev.correct_total_sum_service > 0		-- Было закрытие услуги (любой номенклатуры)
					)
												,956									-- 956 - Закрытие услуги исчезнувшего товара
					,(
						self_prev.order_sub_stage_id = 830				-- Услуга исчезнувшего товара
						AND ex_prev.sale_total_sum_service <= 0			-- Не было реализации услуг (ни одной номенклатуры)
						AND ex_prev.correct_total_sum_service <= 0		-- Не было закрытия услуг (ни одной номенклатуры)
					)
												,957									-- 957 - Исчезновение услуги исчезнувшего товара
					,(
						self_prev.order_sub_stage_id = 840				-- Заказ-услуга
						AND ex_prev.sale_total_sum_service > 0			-- Была реализация услуги (любой номенклатуры)
					)
												,912									-- 912 - Реализация заказа-услуги
					,(
						self_prev.order_sub_stage_id = 840				-- Заказ-услуга
						AND ex_prev.correct_total_sum_service > 0		-- Было закрытие услуги (любой номенклатуры)
					)
												,958									-- 958 - Закрытие заказа-услуги
					,(
						self_prev.order_sub_stage_id = 840				-- Заказ-услуга
						AND ex_prev.sale_total_sum_service <= 0			-- Не было реализации услуг (ни одной номенклатуры)
						AND ex_prev.correct_total_sum_service <= 0		-- Не было закрытия услуг (ни одной номенклатуры)
					)
												,959									-- 959 - Исчезновение заказа-услуги
												,999									-- 999 - Неизвестная причина										
		) 												AS order_sub_stage_id					--	,order_sub_stage_id 
		,''												AS order_sub_stage_name					--	,order_sub_stage_name 
		,self_prev.has_std1								AS has_std1								--	,has_std1 
		,self_prev.is_deficit_std1						AS is_deficit_std1						--	,is_deficit_std1 	
		,self_prev.total_sum_std1						AS total_sum_std1						--	,total_sum_std1 
		,self_prev.has_std2								AS has_std2								--	,has_std2 
		,self_prev.is_deficit_std2						AS is_deficit_std2						--	,is_deficit_std2 
		,self_prev.total_sum_std2						AS total_sum_std2						--	,total_sum_std2 
		,self_prev.has_mip								AS has_mip								--	,has_mip 
		,self_prev.is_deficit_mip						AS is_deficit_mip						--	,is_deficit_mip 
		,self_prev.total_sum_mip						AS total_sum_mip						--	,total_sum_mip 
		,self_prev.has_kich								AS has_kich								--	,has_kich 
		,self_prev.is_deficit_kich						AS is_deficit_kich						--	,is_deficit_kich 
		,self_prev.total_sum_kich						AS total_sum_kich						--	,total_sum_kich 
		,self_prev.has_stor								AS has_stor								--	,has_stor 
		,self_prev.is_deficit_stor						AS is_deficit_stor						--	,is_deficit_stor 
		,self_prev.total_sum_stor						AS total_sum_stor						--	,total_sum_stor 
		,self_prev.has_matr								AS has_matr								--	,has_matr 
		,self_prev.is_deficit_matr						AS is_deficit_matr						--	,is_deficit_matr 
		,self_prev.total_sum_matr						AS total_sum_matr						--	,total_sum_matr 
		,self_prev.has_tech								AS has_tech								--	,has_tech 
		,self_prev.is_deficit_tech						AS is_deficit_tech						--	,is_deficit_tech 
		,self_prev.total_sum_tech						AS total_sum_tech						--	,total_sum_tech 
		,self_prev.has_other							AS has_other							--	,has_other 
		,self_prev.is_deficit_other						AS is_deficit_other						--	,is_deficit_other 
		,self_prev.total_sum_other						AS total_sum_other						--	,total_sum_other 
		,self_prev.has_serv								AS has_serv								--	,has_serv 
		,self_prev.total_sum_serv						AS total_sum_serv						--	,total_sum_serv 
		,self_prev.has_total_products					AS has_total_products					--  ,has_total_products
		,self_prev.has_total_deficit					AS has_total_deficit					--  ,has_total_deficit
		--,self_prev.is_partial_full_order_exit			AS is_partial_full_order_exit			--  ,is_partial_full_order_exit
		,rd_prev.client_id 								AS client_id							--	,client_id								-- V
		,rd_prev.client_name 							AS client_name							--	,client_name							-- V
		,rd_prev.client_guid_uid 						AS client_guid_uid						--	,client_guid_uid						-- V
		,rd_prev.order_phone 							AS order_phone							--	,order_phone							-- V
		,rd_prev.employee_id 							AS employee_id							--	,employee_id							-- V
		,rd_prev.employee_name 							AS employee_name						--	,employee_name							-- V
		,rd_prev.employee_guid_uid 						AS employee_guid_uid					--	,employee_guid_uid						-- V
		,rd_prev.is_mip 								AS is_mip								--	,is_mip									-- V
		,rd_prev.is_kitchen 							AS is_kitchen							--	,is_kitchen								-- V
		,mip.mipik_status_id 							AS mipik_status_id						--	,mipik_status_id						-- V -- 1 - Оплата, 2 - Дизайнер, 3 - Конструктор, 4 - Материалы, 5 - Производство, 6 - Логистика, 7 - Сервис
		,mip.mipik_status_name 							AS mipik_status_name					--	,mipik_status_name						-- V
		,mip.mipik_status_order 						AS mipik_status_order					--	,mipik_status_order						-- V
		,rd_prev.mipik_collection_id 					AS mipik_collection_id					--	,mipik_collection_id					-- V
		,rd_prev.mipik_collection_name 					AS mipik_collection_name				--	,mipik_collection_name 					-- V
		,rd_prev.mipik_collection_guid_uid 				AS mipik_collection_guid_uid			--	,mipik_collection_guid_uid 				-- V
		,rd_prev.mipik_assortiment_group_id 			AS mipik_assortiment_group_id			--	,mipik_assortiment_group_id 			-- V
		,rd_prev.mipik_assortiment_group_name 			AS mipik_assortiment_group_name			--	,mipik_assortiment_group_name 			-- V
		,rd_prev.mipik_assortiment_group_guid_uid 		AS mipik_assortiment_group_guid_uid		--	,mipik_assortiment_group_guid_uid 		-- V
		,rd_prev.mipik_customization_type_id 			AS mipik_customization_type_id			--	,mipik_customization_type_id 			-- V
		,rd_prev.mipik_customization_type_name 			AS mipik_customization_type_name		--	,mipik_customization_type_name 			-- V
		,rd_prev.mipik_customization_type_guid_uid 		AS mipik_customization_type_guid_uid	--	,mipik_customization_type_guid_uid 		-- V
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
		rd_previous_all 													AS rd_prev						-- 5000 строк
		LEFT ANTI JOIN rd_current_all 										AS rd_cur						-- 5000 строк
			ON rd_prev.order_guid_uid = rd_cur.order_guid_uid
		LEFT JOIN rd_exit_previous_all 										AS ex_prev						-- 100-1000 строк
			ON rd_prev.order_guid_uid = ex_prev.order_guid_uid
		LEFT JOIN self_order_previous_all 									AS self_prev					-- 5000 строк
			ON rd_prev.order_guid_uid = self_prev.order_roznica_guid_uid
		LEFT JOIN rd_parent_previous 										AS prnt							-- 5000 строк 
			ON rd_prev.order_guid_uid = prnt.order_guid_uid
		LEFT JOIN mip 														AS mip							-- 2000 строк
			ON rd_prev.order_guid_uid = mip.order_guid_uid
)
-- =========== Для is_partial_delivery  ===================================================================================================
,
-- Для is_partial_delivery: Открытые заказы на ПРЕДЫДУЩУЮ дату (есть Товары, а не только услуги) - исключены Заказы-Услуги 
rd_previous_products AS				-- 5000 строк
(
	SELECT 
		order_guid_uid							AS order_guid_uid
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
	HAVING
		sumIf(order_count, type_id != 14) > 0 		-- Есть товары (шт.)
		OR sumIf(total_sum,	type_id != 14) > 0 		-- Есть товары (руб.)
)
,
-- Для is_partial_delivery: Связка Заказ - Родитель для заказов, содержащих Товары (исключаем Заказы-Услуги).
rd_prev_self_full AS						-- 5000 строк
(
	SELECT
		slf.order_guid_uid										AS order_guid_uid
		,prnt.order_guid_uid									AS parent_order_guid_uid
		,if(
			prnt.order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000')
			,slf.order_guid_uid
			,prnt.order_guid_uid
		)														AS full_order_guid_uid
--		,COALESCE(
--    		nullIf(prnt.order_guid_uid, toUUID('00000000-0000-0000-0000-000000000000')),
--    		slf.order_guid_uid
--		)														AS full_order_guid_uid
	FROM
		rd_previous_products AS slf
		LEFT JOIN rd_parent_previous AS slf_prnt
			ON slf.order_guid_uid = slf_prnt.order_guid_uid
		LEFT JOIN rd_previous_products AS prnt
			ON slf_prnt.parent_order_guid_uid = prnt.order_guid_uid		
)
,
-- Для каждого заказа - все связанные с ним заказы
-- (1) Если заказ order_guid_uid не связан с другими, то есть одна строка, для которой order_guid_uid = linked_order_guid_uid
-- (2) Если заказ А связан с другими заказами (не важно order_guid_uid - главный или нет) и в группе, например, 3 связанных заказа (А, В и С),
-- то будет 3 строки для каждого заказа.
-- Для заказа А: в каждой строке order_guid_uid - А, для строки 1 linked_order_guid_uid - А, для строки 2 - В, для строки 3 - С.
-- Для заказа В: в каждой строке order_guid_uid - В, для строки 1 linked_order_guid_uid - А, для строки 2 - В, для строки 3 - С.
-- Для заказа С: в каждой строке order_guid_uid - С, для строки 1 linked_order_guid_uid - А, для строки 2 - В, для строки 3 - С.
rd_previous_linked AS				-- 7000 строк
(
	SELECT
		ord1.order_guid_uid						AS order_guid_uid
		,ord2.order_guid_uid					AS linked_order_guid_uid
	FROM 
		rd_prev_self_full AS ord1
		INNER JOIN rd_prev_self_full AS ord2
			ON ord1.full_order_guid_uid = ord2.full_order_guid_uid
)
,
-- Преобразования для определения is_partial_delivery
cte_linked AS						-- 7000 строк
(
	SELECT
		link.order_guid_uid				AS order_guid_uid		
    	,if(
        	ex.order_guid_uid = toUUID('00000000-0000-0000-0000-000000000000'),	-- LEFT JOIN для UUID возвращает UUID по умолчанию (0000...)
        	0,
        	1
    	) 								AS is_exited
--    	,link.linked_order_guid_uid		AS linked_order_guid_uid	-- Для проверки
--    	,ex.order_guid_uid				AS ex_order_guid_uid		-- Для проверки
	FROM 
		rd_previous_linked AS link
		LEFT JOIN cte_exit_prev AS ex								-- Завершенные заказы вчера
			ON link.linked_order_guid_uid = ex.order_guid_uid
)
,
-- Признак частичной отгрузки: 
-- all_linked_exited = 0 - частичное завершение (завершены не все заказы из группы) или вообще не один заказ не завершен!
-- all_linked_exited = 1 - полное завершение (завершены все заказы из группы)
cte_partial AS						-- 5000 строк	
(
	SELECT
		order_guid_uid					AS order_guid_uid	
		,min(is_exited)					AS all_linked_orders_exited	-- 1 - все связанные заказы выбыли, 0 - некоторые связанные заказы не выбыли
	FROM 
		cte_linked
	GROUP BY 
		order_guid_uid
)
-- =========== Для is_partial_delivery  ===================================================================================================
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
--	DEFAULT	--,count_perenosov					-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
--	DEFAULT --,row_number_parametry_zakaza		-- !!!!!!!!!!!!!!!!!!!!!!!!!!!! Добавить
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
	--ex.order_guid_uid								AS order_guid_uid
	--,if(pr.all_linked_orders_exited = 1, 0, 1)		AS is_partial_delivery
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
	,any(cte.total_sum_serv) 							AS total_sum_serv					--	,total_sum_serv 
	,any(cte.has_total_products)						AS has_total_products					--  ,has_total_products
	,any(cte.has_total_deficit)							AS has_total_deficit					--  ,has_total_deficit
	,max(pr.all_linked_orders_exited)					AS is_partial_full_order_exit			--  ,is_partial_full_order_exit
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
	cte_exit_prev AS cte
	LEFT JOIN cte_partial AS pr
		ON cte.order_guid_uid = pr.order_guid_uid
GROUP BY 
	cte.date_id								
	,cte.order_guid_uid
	
--SELECT * FROM rd_previous_all
--SELECT * FROM rd_prev_self_full
--SELECT * FROM rd_previous_linked
--SELECT * FROM cte_exit_prev
--SELECT * FROM cte_linked ORDER BY exit_flag DESC
--SELECT * FROM cte_partial ORDER BY exit_flag DESC	
;

