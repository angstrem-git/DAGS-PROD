INSERT INTO {{ params.db3 }}.fct_unit_zayavka_na_otgruzku
(
	batch_id_dttm ,
	batch_id_str ,
	date_id ,
	unit_zayavka_na_otgruzku_guid_OPN_uid ,
	unit_zayavka_na_otgruzku_name ,
	city_zayavka_na_otgruzku_guid_OPN_uid ,
	city_zayavka_na_otgruzku_name ,
	data_otgruzki_zayavka_na_otgruzku_date ,
	ves_itogo_zayavka_na_otgruzku ,
	obyom_itogo_zayavka_na_otgruzku ,
	date_unit_otgruzki_id 	
)
WITH 
unq AS
	(
	SELECT
		DISTINCT
		batch_id_dttm										AS batch_id_dttm
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id) 								AS date_id
		,unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_na_otgruzku_all_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name 						AS unit_na_otgruzku_all_name
		,city_zayavka_na_otgruzku_guid_OPN_uid				AS city_na_otgruzku_all_guid_OPN_uid
		,city_zayavka_na_otgruzku_name						AS city_na_otgruzku_all_name
	FROM 
		rasp1.packet
	WHERE
		unit_zayavka_na_otgruzku_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'
		AND city_zayavka_na_otgruzku_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'
		AND order_roznica_guid_str != ''
		AND unit_zayavka_na_otgruzku_name NOT LIKE '%ФР%'
		AND batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
				
	UNION DISTINCT
	
	SELECT
		DISTINCT
		batch_id_dttm										
		,batch_id_str										
		,toDate(datetime_id)
		,unit_otgruzki_guid_OPN_uid	
		,unit_otgruzki_name
		,city_otgruzki_guid_OPN_uid			
		,city_otgruzki_name					
	FROM 
		rasp1.packet
	WHERE
		unit_otgruzki_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'
		AND order_roznica_guid_str != ''
		AND unit_otgruzki_name NOT LIKE '%ФР%'
		AND batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	--ORDER BY
		--unit_otgruzki_name
		--city_zayavka_na_otgruzku_name	
	)
,
rd AS
	(
	SELECT 
		batch_id_dttm										AS batch_id_dttm
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id) 								AS date_id
		,unit_zayavka_na_otgruzku_guid_OPN_uid				AS unit_zayavka_na_otgruzku_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name						AS unit_zayavka_na_otgruzku_name
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
		,unit_zayavka_na_otgruzku_guid_OPN_uid		
		,unit_zayavka_na_otgruzku_name				
		,city_zayavka_na_otgruzku_guid_OPN_uid		
		,city_zayavka_na_otgruzku_name				
		,toDate(data_otgruzki_zayavka_na_otgruzku_datetime)
	ORDER BY
		city_zayavka_na_otgruzku_name
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
		,MIN(data_otgruzki_zayavka_na_otgruzku_date)		AS data_otgruzki_zayavka_na_otgruzku_date
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
	ORDER BY
		city_zayavka_na_otgruzku_name
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
	,cityHash64(unq.date_id, unq.unit_na_otgruzku_all_guid_OPN_uid, unq.city_na_otgruzku_all_guid_OPN_uid) AS date_unit_otgruzki_id
FROM
	unq
	LEFT JOIN za
		ON unq.batch_id_dttm = za.batch_id_dttm 
		AND unq.date_id = za.date_id
		AND unq.unit_na_otgruzku_all_guid_OPN_uid = za.unit_zayavka_na_otgruzku_guid_OPN_uid
		AND unq.city_na_otgruzku_all_guid_OPN_uid = za.city_zayavka_na_otgruzku_guid_OPN_uid
ORDER BY
	unq.unit_na_otgruzku_all_name	
