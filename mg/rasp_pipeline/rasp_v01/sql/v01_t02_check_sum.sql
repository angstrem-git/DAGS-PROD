WITH cte AS 
(
	SELECT
		ev.aggregate_function_name				AS aggregate_function_name
		,av.aggregate_function_id				AS av_aggregate_function_id
		,av.actual_value						AS actual_value
		,ev.aggregate_function_id				AS ev_aggregate_function_id
		,ev.expected_value						AS expected_value
		,av.actual_value - ev.expected_value	AS diff
	FROM
		(
			SELECT 
				1			AS aggregate_function_id
				,count(*) 	AS actual_value		
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}		
			UNION ALL	
			SELECT 
				2	
				,uniqExactIf(
   	 				nomenclature_guid_OPN_uid,
    				nomenclature_guid_OPN_uid != '00000000-0000-0000-0000-000000000000'
				)
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}			
			UNION ALL		
			SELECT 
				3
				,sum(kolichestvo_dolg)
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}		
			UNION ALL		
			SELECT 
				4
				,min(kolichestvo_dolg)
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}	
			UNION ALL		
			SELECT 
				5
				,max(kolichestvo_dolg)
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}		
			UNION ALL	
			SELECT 
				6
				,uniqExactIf(
   	 				doc_order_rn_guid_1C_uid,
    				doc_order_rn_guid_1C_uid != '00000000-0000-0000-0000-000000000000'
				)
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}		
			UNION ALL		
			SELECT 
				7
				,uniqExactIf(
   	 				doc_order_opn_guid_1C_uid,
    				doc_order_opn_guid_1C_uid != '00000000-0000-0000-0000-000000000000'
				)
			FROM 
				{p_db1}.packet 
			WHERE 
				batch_id_dttm = {p_batch_id_dttm}	
		) AS av	
	INNER JOIN 	
		(
			SELECT
				aggregate_function_id		AS aggregate_function_id
				,aggregate_function_name	AS aggregate_function_name
				,check_value				AS expected_value
			FROM 
				{p_db1}.packet_check_sum
			WHERE
				schema_name = {p_db1}
				AND table_id = 6
				AND batch_id_dttm = {p_batch_id_dttm}		
		) AS ev	
	ON av.aggregate_function_id = ev.aggregate_function_id
)
SELECT
    count() AS total_check
FROM 
	cte
WHERE 
	diff != 0
FORMAT JSON