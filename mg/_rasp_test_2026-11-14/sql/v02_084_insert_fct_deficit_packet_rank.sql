-- v02_084_insert_fct_deficit_packet_rank.sql
-- {{ params.db1 }}: "db1": DB1
-- {{ params.db2 }}: "db2": DB2
-- {{ params.db3 }}: "db3": DB3
WITH 
-- Убрать после отладки -------------------------------------------------------------------------------------------------
cte_dt_rasp AS 
(
	SELECT 
	 	batch_id_dttm										AS batch_id_dttm	
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id)								AS date_id
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-10-13')
	WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="tg_check_data_id.check_001_wait_for_batch_id", key="batch_id_dttm") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
,
-- Убрать после отладки -------------------------------------------------------------------------------------------------
cte_total AS
(
	SELECT 
		hpp.batch_id_dttm							AS batch_id_dttm
		,lb.batch_id_str							AS batch_id_str
		,lb.date_id									AS date_id
		,hpp.rank_id								AS rank_id
		,hpp.total_packet_art_id					AS total_packet_art_id
		--,cityHash64(lb.date_id, hpp.total_packet_art_id, art.nomenclature_guid_uuid, art.nomenclature_property_guid_uuid)
		,toInt64(toYYYYMMDD(lb.date_id)) * 100_000_000_000 
			+ toInt64(hpp.total_packet_art_id)		AS date_total_packet_art_id
		,art.nomenclature_name						AS nomenclature_name
		,art.nomenclature_property_name				AS nomenclature_property_name
		,concat(
	        toString(hpp.rank_id),
			'-',
	        toString(
	        	round(
	           	 		toFloat64(hpp.sum_rank_per_packet) / 1000000,
	            		3
	        		)
	    		),
	        '-[',
	        art.nomenclature_name,
	        '#',
	        art.nomenclature_property_name,
	        ']'
	    ) 											AS packet_rank
		,hpp.sum_rank								AS sum_rank
		,hpp.sum_rank_per_packet					AS sum_rank_per_packet
		,hpp.orders_count_rank						AS orders_count_rank
		,hpp.sum_rank_only_this_set					AS sum_rank_only_this_set
		,hpp.sum_rank_per_packet_only_this_set		AS sum_rank_per_packet_only_this_set
		,hpp.orders_count_only_for_best				AS orders_count_only_for_best
	FROM  
		rasp2.art AS art
		ANY RIGHT JOIN 
			(
				SELECT
					batch_id_dttm,
					rank_id,
					total_packet_art_id,
					sum_rank,
					sum_rank_per_packet,
					orders_count_rank,
					sum_rank_only_this_set,
					sum_rank_per_packet_only_this_set,
					orders_count_only_for_best
				FROM
					{{ params.db2 }}.deficit_packet_rank
					--rasp2_v02.deficit_packet_rank
				WHERE
					batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
			) AS hpp 
			ON art.art_id = hpp.total_packet_art_id
		LEFT JOIN
			(	
				SELECT
					batch_id_dttm		AS batch_id_dttm,
					batch_id_str 		AS batch_id_str,
					toDate(datetime_id) AS date_id
				FROM 
					--rasp1.packet_load_batch
					{{ params.db1 }}.packet_load_batch
				WHERE
					batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
			) AS lb
			ON hpp.batch_id_dttm = lb.batch_id_dttm
)
INSERT INTO {{ params.db3 }}.fct_deficit_packet_rank
--INSERT INTO rasp3_v02.fct_deficit_packet_rank
(
	batch_id_dttm,
	batch_id_str,
	date_id,
	rank_id,
	total_packet_art_id,
	date_total_packet_art_id,
	packet_name,
	packet_property_name,
	packet_rank,
	sum_rank,
	sum_rank_per_packet,
	orders_count_rank,
	sum_rank_only_this_set,
	sum_rank_per_packet_only_this_set,
	orders_count_only_for_best
)
SELECT 
	any(cte.batch_id_dttm) 							AS batch_id_dttm
	,any(cte.batch_id_str) 							AS batch_id_str
	,any(cte.date_id) 								AS date_id
	,any(cte.rank_id) 								AS rank_id
	,any(cte.total_packet_art_id) 					AS total_packet_art_id
	,cte.date_total_packet_art_id					AS date_total_packet_art_id
	,any(cte.nomenclature_name) 					AS nomenclature_name
	,any(cte.nomenclature_property_name) 			AS nomenclature_property_name
	,any(cte.packet_rank) 							AS packet_rank
	,any(cte.sum_rank) 								AS sum_rank
	,any(cte.sum_rank_per_packet) 					AS sum_rank_per_packet
	,any(cte.orders_count_rank) 					AS orders_count_rank
	,any(cte.sum_rank_only_this_set) 				AS sum_rank_only_this_set
	,any(cte.sum_rank_per_packet_only_this_set) 	AS sum_rank_per_packet_only_this_set
	,any(cte.orders_count_only_for_best) 			AS orders_count_only_for_best
FROM 
	cte_total AS cte
GROUP BY 
	cte.date_total_packet_art_id