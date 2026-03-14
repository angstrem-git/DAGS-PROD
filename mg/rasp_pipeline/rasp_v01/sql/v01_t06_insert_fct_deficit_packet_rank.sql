INSERT INTO rasp3_v01.fct_deficit_packet_rank
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
	hpp.batch_id_dttm
	,lb.batch_id_str
	,lb.date_id
	,hpp.rank_id
	,hpp.total_packet_art_id
	,cityHash64(lb.date_id, hpp.total_packet_art_id, art.nomenclature_guid_uuid, art.nomenclature_property_guid_uuid)
	,art.nomenclature_name
	,art.nomenclature_property_name
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
    ) 
	,hpp.sum_rank
	,hpp.sum_rank_per_packet
	,hpp.orders_count_rank
	,hpp.sum_rank_only_this_set
	,hpp.sum_rank_per_packet_only_this_set
	,hpp.orders_count_only_for_best
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
			WHERE
				batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
		) AS hpp 
		ON art.art_id = hpp.total_packet_art_id
	LEFT JOIN
		(	
			SELECT
				batch_id_dttm		AS batch_id_dttm,
				batch_id_str 		AS batch_id_str,
				toDate(datetime_id) AS date_id
			FROM 
				rasp1.packet_load_batch
			WHERE
				batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
		) AS lb
		ON hpp.batch_id_dttm = lb.batch_id_dttm