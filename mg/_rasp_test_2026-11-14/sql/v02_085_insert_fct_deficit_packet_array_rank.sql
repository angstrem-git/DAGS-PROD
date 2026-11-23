-- v02_085_insert_fct_deficit_packet_array_rank.sql
-- {{ params.db1 }}: "db1": DB1
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
ds AS
(
	SELECT 
		DISTINCT
		date_total_packet_art_id
		,doc_razmeshhenie_rasp_str
	FROM
		{{ params.db3 }}.fct_deficit_packet_order
		--rasp3_v02.fct_deficit_packet_order
	WHERE 
		batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
)
,
pr AS
(
	SELECT 
		hp.batch_id_dttm
		,hp.batch_id_str
		,hp.date_id
		,hp.rank_id
		,hp.packet_name
		,hp.packet_property_name
		,hp.sum_rank
		,hp.sum_rank_per_packet
		,hp.orders_count_rank
		,hp.sum_rank_only_this_set
		,hp.sum_rank_per_packet_only_this_set
		,hp.orders_count_only_for_best
		,count(hp.rank_id)			AS plan_count
		,arrayStringConcat(				-- Склеивает массив в текст с переносом строки между элементами (',\n')
			groupArray(
				ds.doc_razmeshhenie_rasp_str
			),
            ', '	
		) 							AS plan_concat
	FROM
		ds
		INNER JOIN 
			(
				SELECT
					batch_id_dttm
					,batch_id_str
					,date_id
					,rank_id
					,date_total_packet_art_id
					,packet_name
					,packet_property_name
					,sum_rank
					,sum_rank_per_packet
					,orders_count_rank
					,sum_rank_only_this_set
					,sum_rank_per_packet_only_this_set
					,orders_count_only_for_best
				FROM
					{{ params.db3 }}.fct_deficit_packet_rank 
					--rasp3_v02.fct_deficit_packet_rank 
				WHERE 
					batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
			) AS hp
			ON ds.date_total_packet_art_id = hp.date_total_packet_art_id
	GROUP BY
		hp.batch_id_dttm
		,hp.batch_id_str
		,hp.date_id
		,hp.rank_id
		,hp.packet_name
		,hp.packet_property_name
		,hp.sum_rank
		,hp.sum_rank_per_packet
		,orders_count_rank
		,sum_rank_only_this_set
		,sum_rank_per_packet_only_this_set
		,orders_count_only_for_best
)
,
pa AS
(
	SELECT
    	batch_id_dttm,
        batch_id_str,
		date_id,
    	rank_id,
    	sum_rank,
    	sum_rank_per_packet,
		orders_count_rank,
		sum_rank_only_this_set,
		sum_rank_per_packet_only_this_set,
		orders_count_only_for_best,
    	arrayStringConcat(							-- 3.Из ОДИНОГО массива кортежей сделали два текстовых массива
       	 	arrayMap(x -> x.3, sorted_arr),
        	concat(',', char(10))
    	) AS plan_concat_array,
    	arrayStringConcat(							-- 3.Из ОДИНОГО массива кортежей сделали два текстовых массива
       	 	arrayMap(x -> concat(x.1, '#', x.2), sorted_arr),
        	concat(',', char(10))
    	) AS packet_array
	FROM	
	(
   	 	SELECT
        	batch_id_dttm,
        	batch_id_str,
   	 		date_id,
        	rank_id,
        	sum_rank,
        	sum_rank_per_packet, 
			orders_count_rank,
			sum_rank_only_this_set,
			sum_rank_per_packet_only_this_set,
			orders_count_only_for_best,
        	arraySort(     							-- 2.Отсортировали ОДИН массив кортежей  							
            	groupArray(							-- 1.Собрали ОДИН массив кортежей
                	(
                    	packet_name,
                    	packet_property_name,
                   		plan_concat
                	)
            	)
        	) AS sorted_arr
    	FROM pr
    	GROUP BY
        	batch_id_dttm,
        	batch_id_str,
    		date_id,
        	rank_id,
        	sum_rank,
        	sum_rank_per_packet,
			orders_count_rank,
			sum_rank_only_this_set,
			sum_rank_per_packet_only_this_set,
			orders_count_only_for_best
	)
	ORDER BY
    	date_id,
    	rank_id
)
,
rd AS
(
	SELECT
		date_id,
		rank_id,
		COUNT(total_order_id) AS orders_count
	FROM
		{{ params.db3 }}.fct_deficit_order_rank
		--rasp3_v02.fct_deficit_order_rank
	WHERE
		batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt_rasp LIMIT 1)
	GROUP BY
		date_id,
		rank_id
)
INSERT INTO {{ params.db3 }}.fct_deficit_packet_array_rank
--INSERT INTO rasp3_v02.fct_deficit_packet_array_rank
(
	batch_id_dttm,
	batch_id_str,
	date_id,
	rank_id,
	orders_count,
	sum_rank,
	sum_rank_per_packet,
	orders_count_rank,
	sum_rank_only_this_set,
	sum_rank_per_packet_only_this_set,
	orders_count_only_for_best,
	packet_array,
	plan_concat_array
)
SELECT
	pa.batch_id_dttm,
    pa.batch_id_str,
	pa.date_id,
	pa.rank_id,
	coalesce(rd.orders_count, 0) AS orders_count,
	pa.sum_rank,
	pa.sum_rank_per_packet,
	pa.orders_count_rank,
	pa.sum_rank_only_this_set,
	pa.sum_rank_per_packet_only_this_set,
	pa.orders_count_only_for_best,
	pa.packet_array,
	pa.plan_concat_array	
FROM 	
	pa LEFT JOIN rd 
		ON pa.date_id = rd.date_id
		AND pa.rank_id = rd.rank_id