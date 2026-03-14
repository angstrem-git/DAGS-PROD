INSERT INTO rasp3_v01.fct_deficit_packet_array_rank
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
WITH
ds AS
(
	SELECT 
		DISTINCT
		date_total_packet_art_id
		,doc_razmeshhenie_rasp_str
	FROM
		{{ params.db3 }}.fct_deficit_packet_order
	WHERE 
		batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
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
				WHERE 
					batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
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
--	SELECT
--    	date_id,
--    	rank_id,
--    	sum_rank,
--    	sum_rank_per_packet,
--		orders_count_rank,
--		sum_rank_only_this_set,
--		sum_rank_per_packet_only_this_set,
--		orders_count_only_for_best,
--    	arrayStringConcat(
--        	arrayMap(x -> x.3, 
--        		arraySort(
--        			groupArray(
--            			(
--               		 		packet_name,
--                			packet_property_name,
--                			plan_concat
--            			)
--        			)
--   			 	)     
--        	),
--        	concat(',', char(10))
--    	) AS plan_concat_array,
--    	arrayStringConcat(
--        	arrayMap(x -> concat(x.1, '#', x.2), 
--        		arraySort(
--        			groupArray(
--            			(
--               		 		packet_name,
--                			packet_property_name,
--                			plan_concat
--            			)
--        			)
--   			 	)   
--        	),
--        	concat(',', char(10))
--    	) AS packet_array
--	FROM pr
--	GROUP BY
--    	date_id,
--    	rank_id,
--    	sum_rank,
--    	sum_rank_per_packet
--		orders_count_rank,
--		sum_rank_only_this_set,
--		sum_rank_per_packet_only_this_set,
--		orders_count_only_for_best,
----------------------------------------------------------------------------
--	SELECT
--    	date_id,
--		rank_id,
--		sum_rank,
--		sum_rank_per_packet,
--		orders_count_rank,
--		sum_rank_only_this_set,
--		sum_rank_per_packet_only_this_set,
--		orders_count_only_for_best,
--		--plan_count,
--		--plan_concat,
--		concat(
--			--toString(plan_count)
--			--,': '
--			--,
--			arrayStringConcat(
--				groupArray(
--					plan_concat
--				)
--				,concat(',', char(10))	-- char(10) (='\n') - перенос строки 
--			)
--		)	AS plan_concat_array ,
--    	--concat(
--        	--toString(rank_id),
--        	--'[',
--        	arrayStringConcat(				-- Склеивает массив в текст с переносом строки между элементами (',\n')
--            	--arraySort(					-- Сортируем массив по алфавиту
--                	groupArray(
--                    	concat(packet_name, '#', packet_property_name)
--                	)
--            	--)
--            	,concat(',', char(10))	-- char(10) (='\n') - перенос строки 
--        	)
--        	--,']'
--    	--) 
--    	AS packet_array
--	FROM 
--		pr
--	GROUP BY 
--		date_id,
--		rank_id,
--		sum_rank,
--		sum_rank_per_packet
--		orders_count_rank,
--		sum_rank_only_this_set,
--		sum_rank_per_packet_only_this_set,
--		orders_count_only_for_best,
--	--	plan_count,
--	--	plan_concat
--	ORDER BY 
--		date_id,
--		rank_id
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
	WHERE
		batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
	GROUP BY
		date_id,
		rank_id
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