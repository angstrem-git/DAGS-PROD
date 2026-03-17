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
)
WITH 
rd AS
	(
	SELECT 
		batch_id_dttm										AS batch_id_dttm
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id) 								AS date_id
		,doc_zayavka_na_otgruzku_rasp_guid_1C_uid			AS doc_zayavka_na_otgruzku_rasp_guid_1C_uid
		,unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
		,client_zayavka_na_otgruzku_guid_OPN_uid			AS client_zayavka_na_otgruzku_guid_OPN_uid
		,client_zayavka_na_otgruzku_name					AS client_zayavka_na_otgruzku_name
		,city_zayavka_na_otgruzku_guid_OPN_uid				AS city_zayavka_na_otgruzku_guid_OPN_uid
		,city_zayavka_na_otgruzku_name						AS city_zayavka_na_otgruzku_name
		,toDate(data_otgruzki_zayavka_na_otgruzku_datetime)	AS data_otgruzki_zayavka_na_otgruzku_date
		,MIN(ves_itogo_zayavka_na_otgruzku)					AS ves_itogo_zayavka_na_otgruzku
		,MIN(obyom_itogo_zayavka_na_otgruzku)				AS obyom_itogo_zayavka_na_otgruzku
	FROM	
		rasp1.packet
	WHERE
		unit_zayavka_na_otgruzku_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'
		AND city_zayavka_na_otgruzku_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'
		AND order_roznica_guid_str != ''
		AND unit_zayavka_na_otgruzku_name NOT LIKE '%ФР%'											-- Не Франчайзи
		--AND city_zayavka_na_otgruzku_guid_OPN_uid != '71a708ae-98b2-11e0-856e-000423d2fac4'			-- Не Воронеж
		--AND city_zayavka_na_otgruzku_guid_OPN_uid != '71a708b4-98b2-11e0-856e-000423d2fac4'			-- Не Липецк
		AND batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	GROUP BY
		batch_id_dttm								
		,batch_id_str								
		,toDate(datetime_id) 						
		,doc_zayavka_na_otgruzku_rasp_guid_1C_uid	
		,unit_zayavka_na_otgruzku_guid_OPN_uid		
		,unit_zayavka_na_otgruzku_name				
		,client_zayavka_na_otgruzku_guid_OPN_uid	
		,client_zayavka_na_otgruzku_name			
		,city_zayavka_na_otgruzku_guid_OPN_uid		
		,city_zayavka_na_otgruzku_name				
		,toDate(data_otgruzki_zayavka_na_otgruzku_datetime)
	--ORDER BY
	--	city_zayavka_na_otgruzku_name
	) 
,
za AS
	(
	SELECT 
		batch_id_dttm										AS batch_id_dttm
		,batch_id_str										AS batch_id_str
		,date_id											AS date_id
		--,doc_zayavka_na_otgruzku_rasp_guid_1C_uid			AS doc_zayavka_na_otgruzku_rasp_guid_1C_uid
		,unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
		--,client_zayavka_na_otgruzku_guid_OPN_uid			AS client_zayavka_na_otgruzku_guid_OPN_uid
		--,client_zayavka_na_otgruzku_name					AS client_zayavka_na_otgruzku_name
		,city_zayavka_na_otgruzku_guid_OPN_uid				AS city_zayavka_na_otgruzku_guid_OPN_uid
		,city_zayavka_na_otgruzku_name						AS city_zayavka_na_otgruzku_name
		,MIN(data_otgruzki_zayavka_na_otgruzku_date)		AS data_otgruzki_zayavka_na_otgruzku_date
		,SUM(ves_itogo_zayavka_na_otgruzku)					AS ves_itogo_zayavka_na_otgruzku
		,SUM(obyom_itogo_zayavka_na_otgruzku)				AS obyom_itogo_zayavka_na_otgruzku
	FROM
		rd
	GROUP BY
		batch_id_dttm										
		,batch_id_str										
		,date_id											
		--,doc_zayavka_na_otgruzku_rasp_guid_1C_uid			
		,unit_zayavka_na_otgruzku_guid_OPN_uid				
		,unit_zayavka_na_otgruzku_name						
		--,client_zayavka_na_otgruzku_guid_OPN_uid			
		--,client_zayavka_na_otgruzku_name					
		,city_zayavka_na_otgruzku_guid_OPN_uid				
		,city_zayavka_na_otgruzku_name	
	--ORDER BY
	--	city_zayavka_na_otgruzku_name
	)
SELECT 
	batch_id_dttm										AS batch_id_dttm
	,batch_id_str										AS batch_id_str
	,date_id											AS date_id
	--,doc_zayavka_na_otgruzku_rasp_guid_1C_uid			AS doc_zayavka_na_otgruzku_rasp_guid_1C_uid
	,unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
	,unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
	--,client_zayavka_na_otgruzku_guid_OPN_uid			AS client_zayavka_na_otgruzku_guid_OPN_uid
	--,client_zayavka_na_otgruzku_name					AS client_zayavka_na_otgruzku_name
	,city_zayavka_na_otgruzku_guid_OPN_uid				AS city_zayavka_na_otgruzku_guid_OPN_uid
	,city_zayavka_na_otgruzku_name						AS city_zayavka_na_otgruzku_name
	,data_otgruzki_zayavka_na_otgruzku_date				AS data_otgruzki_zayavka_na_otgruzku_date
	,ves_itogo_zayavka_na_otgruzku						AS ves_itogo_zayavka_na_otgruzku
	,obyom_itogo_zayavka_na_otgruzku					AS obyom_itogo_zayavka_na_otgruzku
	,cityHash64(date_id, unit_zayavka_na_otgruzku_guid_OPN_uid) AS date_unit_otgruzki_id
FROM
	za
--ORDER BY
--	city_zayavka_na_otgruzku_name