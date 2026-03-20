INSERT INTO {{ params.db3 }}.fct_unit_zayavka_na_otgruzku
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
WITH 
unq AS
	(
	SELECT
		DISTINCT
		pkt.batch_id_dttm										AS batch_id_dttm
		,pkt.batch_id_str										AS batch_id_str
		,toDate(pkt.datetime_id) 								AS date_id
		,pkt.unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_na_otgruzku_all_guid_OPN_uid
		,pkt.unit_zayavka_na_otgruzku_name 						AS unit_na_otgruzku_all_name
		,pkt.city_zayavka_na_otgruzku_guid_OPN_uid				AS city_na_otgruzku_all_guid_OPN_uid
		,pkt.city_zayavka_na_otgruzku_name						AS city_na_otgruzku_all_name
	FROM 
		rasp1.packet AS pkt
		-- mg-20.03.2026 - Новый фильтр Розничных заказлв ---------------------------------------------------
		INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn
			ON pkt.unit_guid_OPN_uid = urn.unit_guid_uid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	WHERE
		pkt.unit_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.city_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		--AND pkt.order_roznica_guid_str != ''						-- Фильтр = Розничный заказ (замена на INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn)
		--AND pkt.unit_zayavka_na_otgruzku_name NOT LIKE '%ФР%'		-- Фильтр = Розничный заказ (замена на INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn)
		AND pkt.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
				
	UNION DISTINCT
	
	SELECT
		DISTINCT
		pkt.batch_id_dttm										
		,pkt.batch_id_str										
		,toDate(pkt.datetime_id)
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
		rasp1.packet AS pkt
		-- mg-20.03.2026 - Новый фильтр Розничных заказлв ---------------------------------------------------
		INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn
			ON pkt.unit_guid_OPN_uid = urn.unit_guid_uid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	WHERE
		pkt.unit_otgruzki_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		--AND pkt.order_roznica_guid_str != ''				-- Фильтр = Розничный заказ (замена на INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn)
		--AND pkt.unit_otgruzki_name NOT LIKE '%ФР%'		-- Фильтр = Розничный заказ (замена на INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn)
		AND pkt.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	--ORDER BY
	--	pkt.unit_otgruzki_name
	--	,pkt.city_otgruzki_name	
	)	
,
rd AS
	(
	SELECT 
		pkt.batch_id_dttm										AS batch_id_dttm
		,pkt.batch_id_str										AS batch_id_str
		,toDate(pkt.datetime_id) 								AS date_id
		,pkt.unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
		,pkt.unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
		,pkt.city_zayavka_na_otgruzku_guid_OPN_uid				AS city_zayavka_na_otgruzku_guid_OPN_uid
		,pkt.city_zayavka_na_otgruzku_name						AS city_zayavka_na_otgruzku_name
		,toDate(pkt.data_otgruzki_zayavka_na_otgruzku_datetime)	AS data_otgruzki_zayavka_na_otgruzku_date
		,pkt.doc_zayavka_na_otgruzku_rasp_guid_1C_uid			AS doc_zayavka_na_otgruzku_rasp_guid_1C_uid			
		,MIN(pkt.ves_itogo_zayavka_na_otgruzku)					AS ves_itogo_zayavka_na_otgruzku
		,MIN(pkt.obyom_itogo_zayavka_na_otgruzku)				AS obyom_itogo_zayavka_na_otgruzku
	FROM	
		rasp1.packet AS pkt
		-- mg-20.03.2026 - Новый фильтр Розничных заказлв ---------------------------------------------------
		--INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn		-- Включить, если не хотим, чтобы добавлялись рекламации!
		--	ON pkt.unit_guid_OPN_uid = urn.unit_guid_uid
		-- mg-20.03.2026 ------------------------------------------------------------------------------------
	WHERE
		pkt.unit_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		AND pkt.city_zayavka_na_otgruzku_guid_OPN_uid != toUUID('00000000-0000-0000-0000-000000000000')
		--AND pkt.order_roznica_guid_str != ''						-- Фильтр = Розничный заказ (замена на INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn)
		--AND pkt.unit_zayavka_na_otgruzku_name NOT LIKE '%ФР%'		-- Фильтр = Розничный заказ (замена на INNER JOIN (SELECT unit_guid_uid FROM rasp2.unit_sale WHERE sales_direction_id = 2) AS urn)									-- Не Франчайзи
		--AND pkt.city_zayavka_na_otgruzku_guid_OPN_uid != '71a708ae-98b2-11e0-856e-000423d2fac4'			-- Не Воронеж
		--AND pkt.city_zayavka_na_otgruzku_guid_OPN_uid != '71a708b4-98b2-11e0-856e-000423d2fac4'			-- Не Липецк
		AND pkt.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	GROUP BY
		pkt.batch_id_dttm								
		,pkt.batch_id_str								
		,toDate(pkt.datetime_id) 						
		,pkt.unit_zayavka_na_otgruzku_guid_OPN_uid		
		,pkt.unit_zayavka_na_otgruzku_name				
		,pkt.city_zayavka_na_otgruzku_guid_OPN_uid		
		,pkt.city_zayavka_na_otgruzku_name				
		,toDate(pkt.data_otgruzki_zayavka_na_otgruzku_datetime)
		,pkt.doc_zayavka_na_otgruzku_rasp_guid_1C_uid																
	--ORDER BY
	--	pkt.city_zayavka_na_otgruzku_name
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
	--ORDER BY
	--	city_zayavka_na_otgruzku_name
	)
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
	,cityHash64(
		unq.date_id
		, unq.unit_na_otgruzku_all_guid_OPN_uid
		, unq.city_na_otgruzku_all_guid_OPN_uid
		, coalesce(za.data_otgruzki_zayavka_na_otgruzku_date, toDate('1970-01-01'))
	) 														AS date_unit_otgruzki_id
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
--ORDER BY
--	unq.unit_na_otgruzku_all_name
