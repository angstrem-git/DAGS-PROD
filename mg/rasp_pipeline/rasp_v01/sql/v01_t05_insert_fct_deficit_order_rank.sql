INSERT INTO {{ params.db3 }}.fct_deficit_order_rank 
(
	batch_id_dttm
	,batch_id_str
	,date_id
	,rank_id
	,total_order_id
	,date_total_order_id
	,doc_order_rn_number
	,doc_order_rn_datetime
	,order_roznica_guid_uid
	,unit_id 
	,unit_name
	,unit_guid_OPN_uid
	,total_sum
	,sort_rank
	-- mg-16.03.2026 ---------------------------------------------------------------------------
	,unit_otgruzki_guid_OPN_uid
	,city_otgruzki_guid_OPN_uid																	-- Добавить
	,ves_total
	,obyom_total
	,date_unit_otgruzki_id
)
SELECT 
	def.batch_id_dttm
	,def.batch_id_str
	,def.date_id
	,def.rank_id
	,def.total_order_id
	,def.date_total_order_id	
	,def.doc_order_rn_number
	,def.doc_order_rn_datetime
	,def.order_roznica_guid_uid
	,def.unit_id
	,def.unit_name
	,def.unit_guid_OPN_uid
	,def.total_sum
	,def.sort_rank	
	-- mg-16.03.2026 ---------------------------------------------------------------------------
	,pkt.unit_otgruzki_guid_OPN_uid
	,pkt.city_otgruzki_guid_OPN_uid																-- Добавить
	,pkt.ves_total
	,pkt.obyom_total
	--,cityHash64(def.date_id, pkt.unit_otgruzki_guid_OPN_uid, pkt.city_otgruzki_guid_OPN_uid) 	-- Изменить
	,hsh.date_unit_otgruzki_id
FROM
	(
		SELECT
    		unit_otgruzki_guid_OPN_uid,
    		city_otgruzki_guid_OPN_uid,
    		order_roznica_guid_str,
    		SUM(kolichestvo_dolg * ves_brutto_na_shtuku)  AS ves_total,
    		SUM(kolichestvo_dolg * obyom_brutto_na_shtuku) AS obyom_total
		FROM
		(
   		 SELECT
        		unit_otgruzki_guid_OPN_uid,

        		multiIf(
            		unit_otgruzki_guid_OPN_uid = toUUID('016a1489-ef2e-11db-8f0b-000423d2fac4'),	-- Склад готовой продукции КомСл
            		toUUID('71a708ae-98b2-11e0-856e-000423d2fac4'),									-- г. Воронеж

            		unit_otgruzki_guid_OPN_uid = toUUID('df928c3a-ec8b-4051-9d9d-3c85d182bec5'),	-- Кострома Офис-Склад
            		toUUID('b6192540-98b1-11e0-856e-000423d2fac4'),									-- г. Кострома

            		city_otgruzki_guid_OPN_uid
        		) AS city_otgruzki_guid_OPN_uid,

        		order_roznica_guid_str,
        		kolichestvo_dolg,
        		ves_brutto_na_shtuku,
        		obyom_brutto_na_shtuku

    		FROM rasp1.packet
    		WHERE 
        		order_roznica_guid_str != ''
        		AND batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
		)
		GROUP BY
    		unit_otgruzki_guid_OPN_uid,
    		city_otgruzki_guid_OPN_uid,
    		order_roznica_guid_str
	) AS pkt
	INNER JOIN
	(
	SELECT 
		drd.batch_id_dttm			AS batch_id_dttm
		,ps.batch_id_str			AS batch_id_str
		,ps.date_id					AS date_id
		,drd.rank_id				AS rank_id
		,drd.total_order_id			AS total_order_id
		,cityHash64(ps.date_id, drd.total_order_id, ps.order_roznica_guid_uid) 	AS date_total_order_id	
		,ps.doc_order_rn_number		AS doc_order_rn_number
		,ps.doc_order_rn_datetime	AS doc_order_rn_datetime
		,ps.order_roznica_guid_uid	AS order_roznica_guid_uid
		,us.unit_id					AS unit_id
		,ps.unit_name				AS unit_name
		,ps.unit_guid_OPN_uid		AS unit_guid_OPN_uid
		,drd.total_sum				AS total_sum
		,drd.sort_rank				AS sort_rank
	FROM
		{{ params.db2 }}.deficit_order_rank AS drd
		ANY LEFT JOIN {{ params.db3 }}.fct_deficit_packet_order AS ps 
			ON drd.batch_id_dttm = ps.batch_id_dttm
			AND drd.total_order_id = ps.total_order_id
		LEFT JOIN rasp2.unit_sale AS us
			ON ps.unit_guid_OPN_uid = us.unit_guid_uid
	WHERE
		drd.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	) AS def
		ON toUUIDOrNull(pkt.order_roznica_guid_str) = def.order_roznica_guid_uid
	LEFT JOIN 
	(
	SELECT
    	unit_zayavka_na_otgruzku_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name
		,city_zayavka_na_otgruzku_guid_OPN_uid
    	,city_zayavka_na_otgruzku_name
    	,argMin(date_unit_otgruzki_id, data_otgruzki_zayavka_na_otgruzku_date) AS date_unit_otgruzki_id
	FROM 
		{{ params.db3 }}.fct_unit_zayavka_na_otgruzku
	WHERE 
    	batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	GROUP BY 
    	unit_zayavka_na_otgruzku_guid_OPN_uid
		,unit_zayavka_na_otgruzku_name
		,city_zayavka_na_otgruzku_guid_OPN_uid
    	,city_zayavka_na_otgruzku_name
	ORDER BY 
		unit_zayavka_na_otgruzku_name
		,city_zayavka_na_otgruzku_name
	) AS hsh
		ON pkt.unit_otgruzki_guid_OPN_uid = hsh.unit_zayavka_na_otgruzku_guid_OPN_uid
		AND pkt.city_otgruzki_guid_OPN_uid = hsh.city_zayavka_na_otgruzku_guid_OPN_uid
--ORDER BY def.unit_name
--ORDER BY hsh.date_unit_otgruzki_id