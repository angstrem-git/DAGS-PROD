INSERT INTO rasp3_v01.fct_deficit_packet_order
(
	batch_id_dttm,
	batch_id_str, 
	date_id,
	order_roznica_guid_uid,
	doc_order_rn_number,
	doc_order_rn_datetime,
	total_order_id,
	date_total_order_id,
	unit_name,								
	unit_guid_OPN_uid,
	product_name,
	product_guid_OPN_uid,
	product_property_name,
	product_property_guid_OPN_uid,
	total_product_art_id,
	date_total_product_art_id,
	packet_name,
	packet_guid_OPN_uid,
	packet_property_name,
	packet_property_guid_OPN_uid,
	total_packet_art_id,
	date_total_packet_art_id,
	kolichestvo_dolg, 
	order_total_sum,
	is_ispolzuemaya_today,
	status_modul_today_short_name,
	status_packet_today_short_name,
	is_ispolzuemaya_create_date,
	status_modul_create_date_short_name,
	status_packet_create_date_short_name,
	doc_razmeshhenie_rasp_str,
	doc_razmeshhenie_rasp_number,
	doc_razmeshhenie_rasp_data,
	unit_proizvodstva_name,
	smena_name,
	is_iz_gorizonta,
	data_vyhoda_iz_proizvodstva,
	data_zadaniya
)
WITH
t_rn AS 
	(
	SELECT 
		pkt.batch_id_dttm
		,pkt.batch_id_str
		,pkt.date_id
		,pkt.order_roznica_guid_str
		,pkt.doc_order_rn
		,pkt.doc_order_rn_guid_1C_uid
		,pkt.doc_order_rn_guid_1C_str
		,pkt.doc_order_rn_number
		,pkt.doc_order_rn_datetime	
		,pkt.nomenclature_name
		,pkt.nomenclature_guid_OPN_uid
		,pkt.nomenclature_guid_OPN_str
		,pkt.nomenclature_property_name
		,pkt.nomenclature_property_guid_OPN_uid
		,pkt.nomenclature_property_guid_OPN_str
		,pkt.vid_nomenclature_name
		,pkt.vid_nomenclature_guid_OPN_uid
		,pkt.vid_nomenclature_guid_OPN_str
		,pkt.packet_name
		,pkt.packet_guid_OPN_uid
		,pkt.packet_guid_OPN_str
		,pkt.packet_property_name
		,pkt.packet_property_guid_OPN_uid
		,pkt.packet_property_guid_OPN_str
		,pkt.kolichestvo_packetov_v_module
		,pkt.client_UT
		,pkt.client_name
		,pkt.client_guid_OPN_uid
		,pkt.client_guid_OPN_str
		,pkt.unit_name
		,pkt.unit_guid_OPN_uid
		,pkt.unit_guid_OPN_str
		,pkt.city_name
		,pkt.city_guid_OPN_uid
		,pkt.city_guid_OPN_str
		,pkt.data_dostavki_dogovor_roznica
		,pkt.is_po_prosbe_clienta_roznica
		,pkt.data_dostavki_roznica
		,pkt.doc_osnovnoj_order_roznica_uid
		,pkt.is_strahovoj_zapas_roznica
		,pkt.is_vystavka_roznica
		,pkt.is_oplachen_roznica
		,pkt.is_otgruzka_s_vystavki_roznica
		,pkt.is_reklamacii
		,pkt.is_poligrafiya
		,pkt.kolichestvo_dolg
		,pkt.kolichestvo_v_rezerve
		,pkt.kolichestvo_podtverzhdeno
		,pkt.data_otgruzki_iz_Voronezh_rasp
		,pkt.data_komplekta_rasp
		,pkt.data_celevaya_rasp
		,pkt.tip_zakaza_rasp_str
		,pkt.kolichestvo_raspredeleno_modul_rasp
		,pkt.kolichestvo_iz_vypuska_rasp
		,pkt.kolichestvo_iz_regionov_rasp
		,pkt.kolichestvo_so_sklada_regiona_rasp
		,pkt.kolichestvo_so_sklada_transit_rasp
		,pkt.kolichestvo_ochered_rasp
		,pkt.kolichestvo_v_rezerve_rasp
		,pkt.kolichestvo_v_zayavkah_rasp
		,pkt.kolichestvo_kompleltno_rasp
		,pkt.data_vyhoda_iz_proizvodstva
		,pkt.is_iz_proizvodstva_rasp
		,pkt.is_iz_reserva_rasp
		,pkt.is_iz_regiona_rasp
		,pkt.is_iz_postupleniya_rasp
		,pkt.is_iz_plana_rasp
		,pkt.is_iz_gorizonta_rasp
		,pkt.kolichestvo_raspredeleno_packetov_rasp
		,pkt.is_ispolzuemaya_today
		,pkt.status_modul_today_short_name
		,pkt.status_packet_today_short_name
		,pkt.is_ispolzuemaya_create_date
		,pkt.status_modul_create_date_short_name
		,pkt.status_packet_create_date_short_name
		,pkt.doc_razmeshhenie_rasp_str
		,pkt.doc_razmeshhenie_rasp_guid_1C_uid
		,pkt.doc_razmeshhenie_rasp_guid_1C_str
		,pkt.doc_razmeshhenie_rasp_number
		,pkt.doc_razmeshhenie_rasp_data
		,pkt.unit_proizvodstva_name
		,pkt.unit_proizvodstva_guid_OPN_uid
		,pkt.unit_proizvodstva_guid_OPN_str
		,pkt.smena_name
		,pkt.smena_guid_OPN_uid
		,pkt.smena_guid_OPN_str
		,pkt.is_iz_gorizonta
		,pkt.data_vyhoda_iz_proizvodstva
		,pkt.data_zadaniya
	FROM 
		rasp1.packet AS pkt
		-- На заказ не ссылаются другие заказы (заказ не является основным для других заказов)
		LEFT ANTI JOIN 
			(SELECT doc_osnovnoj_order_roznica_uid
			 FROM rasp1.packet
			 WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}' ) AS osn
			ON pkt.doc_order_rn_guid_1C_uid = osn.doc_osnovnoj_order_roznica_uid
	WHERE 
		pkt.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
		-- Это розничный заказ
		--AND pkt.doc_order_rn_guid_1C_uid != '00000000-0000-0000-0000-000000000000'	
		-- Оплачен
		AND pkt.is_oplachen_roznica = 1	
		-- Клиент не перенес доставку или перенес не более, чем на 2 недели от текущей даты
		AND (pkt.is_po_prosbe_clienta_roznica = 0 OR (pkt.is_po_prosbe_clienta_roznica = 1 AND pkt.data_dostavki_roznica < today() + 15) )
		-- Не ссылается на основной заказ
		AND pkt.doc_osnovnoj_order_roznica_uid = '00000000-0000-0000-0000-000000000000'	
		-- Не на страховой запас
		AND pkt.is_strahovoj_zapas_roznica = 0
		-- На выставку
		AND pkt.is_vystavka_roznica = 0	
		-- Отгрузка с выставки
		AND pkt.is_otgruzka_s_vystavki_roznica = 0
		-- Не рекламации
		AND pkt.is_reklamacii = 0
		-- Не полиграфия
		AND pkt.is_poligrafiya = 0
		-- Только Стандартная продукция (есть пакеты)
		--AND pkt.packet_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'	
		--AND pkt.vid_nomenclature_guid_OPN_uid = 'e77a2503-aa8f-41f0-b85c-b6099fdd1c25'	-- Изделия
	)
,
-- Список заказов, в которых есть дефицит только по Стандартной продукции
t_def_stand AS
	(
	 SELECT 
	 	doc_order_rn_guid_1C_uid
	 	,countIf(
	 				vid_nomenclature_guid_OPN_uid = 'e77a2503-aa8f-41f0-b85c-b6099fdd1c25'		-- Изделия = Стандарт
	 				AND is_iz_proizvodstva_rasp = 1
	 			) 																			AS has_deficit_standart
	 	,countIf(
	 				vid_nomenclature_guid_OPN_uid != 'e77a2503-aa8f-41f0-b85c-b6099fdd1c25'		-- Изделия = Стандарт
	 				AND is_iz_proizvodstva_rasp = 1
	 			) 																			AS has_deficit_non_standart
	 FROM 
		rasp1.packet
	 WHERE 
		batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
		-- Это розничный заказ
		AND doc_order_rn_guid_1C_uid != '00000000-0000-0000-0000-000000000000'
	 GROUP BY
	 	doc_order_rn_guid_1C_uid
	 HAVING
	 	has_deficit_standart > 0
	 	AND has_deficit_non_standart = 0 	
	)
,
cte AS
(
SELECT 
	--DISTINCT t_rn.doc_order_rn_guid_1C_uid 	
	--t_rn.is_iz_proizvodstva_rasp,
	--DISTINCT t_rn.packet_name, t_rn.packet_property_name
	--t_rn.* 
	t_rn.batch_id_dttm
	,t_rn.batch_id_str
	,t_rn.date_id
	,t_rn.order_roznica_guid_str 			AS order_roznica_guid_str
	,toUUID(t_rn.order_roznica_guid_str) 	AS order_roznica_guid_uid
	,t_rn.doc_order_rn
	,t_rn.doc_order_rn_guid_1C_uid
	,t_rn.doc_order_rn_guid_1C_str
	,t_rn.doc_order_rn_number
	,t_rn.doc_order_rn_datetime	
	,t_rn.nomenclature_name
	,t_rn.nomenclature_guid_OPN_uid
	,t_rn.nomenclature_guid_OPN_str
	,t_rn.nomenclature_property_name
	,t_rn.nomenclature_property_guid_OPN_uid
	,t_rn.nomenclature_property_guid_OPN_str
	--,t_rn.vid_nomenclature_name
	--,t_rn.vid_nomenclature_guid_OPN_uid
	--,t_rn.vid_nomenclature_guid_OPN_str
	,t_rn.packet_name
	,t_rn.packet_guid_OPN_uid
	,t_rn.packet_guid_OPN_str
	,t_rn.packet_property_name
	,t_rn.packet_property_guid_OPN_uid
	,t_rn.packet_property_guid_OPN_str
	--,t_rn.kolichestvo_packetov_v_module
	--,t_rn.client_UT
	--,t_rn.client_name
	--,t_rn.client_guid_OPN_uid
	--,t_rn.client_guid_OPN_str
	,t_rn.unit_name
	,t_rn.unit_guid_OPN_uid
	--,t_rn.unit_guid_OPN_str
	--,t_rn.city_name
	--,t_rn.city_guid_OPN_uid
	--,t_rn.city_guid_OPN_str
	--,t_rn.data_dostavki_dogovor_roznica
	--,t_rn.is_po_prosbe_clienta_roznica
	--,t_rn.data_dostavki_roznica
	--,t_rn.doc_osnovnoj_order_roznica_uid
	--,t_rn.is_strahovoj_zapas_roznica
	--,t_rn.is_vystavka_roznica
	--,t_rn.is_oplachen_roznica
	--,t_rn.is_otgruzka_s_vystavki_roznica
	--,t_rn.is_reklamacii
	--,t_rn.is_poligrafiya
	,t_rn.kolichestvo_dolg
	--,t_rn.kolichestvo_v_rezerve
	--,t_rn.kolichestvo_podtverzhdeno
	--,t_rn.data_otgruzki_iz_Voronezh_rasp
	--,t_rn.data_komplekta_rasp
	--,t_rn.data_celevaya_rasp
	,t_rn.tip_zakaza_rasp_str
	--,t_rn.kolichestvo_raspredeleno_modul_rasp
	--,t_rn.kolichestvo_iz_vypuska_rasp
	--,t_rn.kolichestvo_iz_regionov_rasp
	--,t_rn.kolichestvo_so_sklada_regiona_rasp
	--,t_rn.kolichestvo_so_sklada_transit_rasp
	--,t_rn.kolichestvo_ochered_rasp
	--,t_rn.kolichestvo_v_rezerve_rasp
	--,t_rn.kolichestvo_v_zayavkah_rasp
	--,t_rn.kolichestvo_kompleltno_rasp
	--,t_rn.data_vyhoda_iz_proizvodstva
	----,t_rn.is_iz_proizvodstva_rasp
	--,t_rn.is_iz_reserva_rasp
	--,t_rn.is_iz_regiona_rasp
	--,t_rn.is_iz_postupleniya_rasp
	--,t_rn.is_iz_plana_rasp
	--,t_rn.is_iz_gorizonta_rasp
	--,t_rn.kolichestvo_raspredeleno_packetov_rasp
	,t_rn.is_ispolzuemaya_today
	,t_rn.status_modul_today_short_name
	,t_rn.status_packet_today_short_name
	,t_rn.is_ispolzuemaya_create_date
	,t_rn.status_modul_create_date_short_name
	,t_rn.status_packet_create_date_short_name
	,t_rn.doc_razmeshhenie_rasp_str
	----,t_rn.doc_razmeshhenie_rasp_guid_1C_uid
	----,t_rn.doc_razmeshhenie_rasp_guid_1C_str
	,t_rn.doc_razmeshhenie_rasp_number
	,t_rn.doc_razmeshhenie_rasp_data
	,t_rn.unit_proizvodstva_name
	----,t_rn.unit_proizvodstva_guid_OPN_uid
	----,t_rn.unit_proizvodstva_guid_OPN_str
	,t_rn.smena_name
	----,t_rn.smena_guid_OPN_uid
	----,t_rn.smena_guid_OPN_str
	,t_rn.is_iz_gorizonta
	,t_rn.data_vyhoda_iz_proizvodstva
	,t_rn.data_zadaniya
FROM 
	t_rn 
	INNER JOIN t_def_stand	 -- Фильтр по списку заказов, в которых есть дефицит только по Стандартной продукции 
		ON t_rn.doc_order_rn_guid_1C_uid = t_def_stand.doc_order_rn_guid_1C_uid
	--INNER JOIN t_def		-- Фильтр по списку заказов, в которых есть дефицит
	--	ON t_rn.doc_order_rn_guid_1C_uid = t_def.doc_order_rn_guid_1C_uid
	--INNER JOIN t_stand		-- Фильтр по списку заказов, в которых есть Стандартная продукция
	--	ON t_rn.doc_order_rn_guid_1C_uid = t_stand.doc_order_rn_guid_1C_uid-
WHERE
	t_rn.is_iz_proizvodstva_rasp = 1
	--AND 
	--t_rn.status_packet_short_name != 'Opt'
)
SELECT
	cte.batch_id_dttm
	,cte.batch_id_str
	,cte.date_id
	--,cte.order_roznica_guid_str
	,cte.order_roznica_guid_uid
	--,cte.doc_order_rn
	--,cte.doc_order_rn_guid_1C_uid
	--,cte.doc_order_rn_guid_1C_str
	,cte.doc_order_rn_number
	,cte.doc_order_rn_datetime
	,if(rd.order_id != 0, 
		toInt64(rd.order_id), 
		-reinterpretAsInt64( cityHash64(cte.order_roznica_guid_uid) ) 		-- cityHash64() возвращает UInt64, надо преобразовать в Int64     	
		) 										--AS total_order_id	
	,cityHash64(cte.date_id, rd.order_id, order_roznica_guid_uid) 			-- AS date_total_order_id (UInt64)
	,cte.unit_name 								
	,cte.unit_guid_OPN_uid						
	--,rd.unit_name								AS unit_name_open_orders_history
	--,if(trimBoth(cte.unit_name, ' ') == trimBoth(rd.unit_name, ' ') , 1, 0)	AS unit_name_check
	,cte.nomenclature_name
	,cte.nomenclature_guid_OPN_uid
	--,cte.nomenclature_guid_OPN_str
	,cte.nomenclature_property_name
	,cte.nomenclature_property_guid_OPN_uid
	--,cte.nomenclature_property_guid_OPN_str
	--,art_nom.art_id AS nomenclature_art_id
	,if(art_nom.art_id != 0, 
		toInt64(art_nom.art_id), 
		-reinterpretAsInt64( cityHash64(cte.nomenclature_guid_OPN_uid, cte.nomenclature_property_guid_OPN_uid) )	-- cityHash64() возвращает UInt64, надо преобразовать в Int64	
        ) 										 --AS total_nomenclature_art_id
	,cityHash64(cte.date_id, art_nom.art_id, cte.nomenclature_guid_OPN_uid, cte.nomenclature_property_guid_OPN_uid)	-- date_total_product_art_id (UInt64)
    ,cte.packet_name
	,cte.packet_guid_OPN_uid
	--,cte.packet_guid_OPN_str
	,cte.packet_property_name
	,cte.packet_property_guid_OPN_uid
	--,cte.packet_property_guid_OPN_str
	--,art_pack.art_id 							-- AS packet_art_id
	,if(art_pack.art_id != 0, 
		toInt64(art_pack.art_id), 
		-reinterpretAsInt64( cityHash64(cte.packet_guid_OPN_uid, cte.packet_property_guid_OPN_uid) ) 	 -- cityHash64() возвращает UInt64, надо преобразовать в Int64
		) 										 --AS total_packet_art_id
	,cityHash64(cte.date_id, art_pack.art_id, cte.packet_guid_OPN_uid, cte.packet_property_guid_OPN_uid) -- AS date_total_packet_art_id
	,cte.kolichestvo_dolg
	,rd.total_sum								-- AS order_total_sum
	--,cte.tip_zakaza_rasp_str
	,cte.is_ispolzuemaya_today
	,cte.status_modul_today_short_name
	,cte.status_packet_today_short_name
	,cte.is_ispolzuemaya_create_date
	,cte.status_modul_create_date_short_name
	,cte.status_packet_create_date_short_name
	,cte.doc_razmeshhenie_rasp_str
	,cte.doc_razmeshhenie_rasp_number
	,cte.doc_razmeshhenie_rasp_data
	,cte.unit_proizvodstva_name
	,cte.smena_name
	,cte.is_iz_gorizonta
	,cte.data_vyhoda_iz_proizvodstva
	,cte.data_zadaniya
FROM 
	cte
	INNER JOIN 
		(
			SELECT  
				order_id
				,order_guid_uuid
				,unit_name
				,total_sum
			FROM 
				rasp2.open_orders_rn_today
			WHERE 
				date_id = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="date_id") }}'
		) AS rd 
		ON cte.order_roznica_guid_uid = rd.order_guid_uuid
	LEFT JOIN rasp2.art AS art_nom 
		ON cte.nomenclature_guid_OPN_uid = art_nom.nomenclature_guid_uuid 
		AND cte.nomenclature_property_guid_OPN_uid = art_nom.nomenclature_property_guid_uuid 	
	LEFT JOIN rasp2.art AS art_pack 
		ON cte.packet_guid_OPN_uid = art_pack.nomenclature_guid_uuid 
		AND cte.packet_property_guid_OPN_uid = art_pack.nomenclature_property_guid_uuid 