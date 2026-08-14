-- "db1": DB1
-- "db3": DB3
-- "days_back": 0, 1 или 2
WITH 
--	0 AS days_back
--	--,toDate('2026-07-31') AS cte_current_snapshot_date
--,
cte_dt_rasp AS 
(
	SELECT 
	 	batch_id_dttm										AS batch_id_dttm	
		,batch_id_str										AS batch_id_str
		,toDate(datetime_id)								AS date_id
		-- days_back = 0 (Пн,Вт,Ср,Чт,Пт), days_back = 1 (Вс), days_back = 2 (Сб)
		,subtractDays(
			toDate(datetime_id),
			{{ params.days_back }} 
		) 													AS current_date 
		--,subtractDays(toDate(datetime_id), days_back) 		AS current_date 		-- Текущий день (для которого обрабатываем данные)
	--FROM rasp1_v02.packet_load_batch
	FROM {{ params.db1 }}.packet_load_batch
	--WHERE toDate(datetime_id) = toDate('2026-08-03')
	WHERE batch_id_str = '{{ ti.xcom_pull(task_ids="tg_work_day_01_31_id.r_001_set_xcom_id", key="batch_id_str") }}'	
	ORDER BY batch_id_dttm DESC
	LIMIT 1
)
, 
cte AS
(
	-- Стандарт-1 (СД1)
	SELECT
		batch_id_dttm 				AS batch_id_dttm
		,batch_id_str				AS batch_id_str
		,date_id					AS date_id
		,full_order_id				AS full_order_id
		,1							AS type_id			-- Стандарт-1 (СД1)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_std1 = 1	
	UNION ALL
	-- Стандарт-2 (СД2)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,17										-- Стандарт-2 (СД2)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_std2 = 1	
	UNION ALL
	-- МИП (МИП)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,3										-- МИП (МИП)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_mip = 1	
	UNION ALL
	-- Кухни (КУХ)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,4										-- Кухни (КУХ)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_kich = 1	
	UNION ALL
	-- Сторонняя (СТР)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,6										-- Сторонняя (СТР)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_stor = 1	
	UNION ALL
	-- Матрасы (МТР)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,13										-- Матрасы (МТР)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_matr = 1	
	UNION ALL
	-- Бытовая техника (ТЕХ)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,15										-- Бытовая техника (ТЕХ)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_tech = 1	
	UNION ALL
	-- Прочее (ПРЧ)
	SELECT
		batch_id_dttm 				
		,batch_id_str				
		,date_id						
		,full_order_id				
		,16										-- Прочее (ПРЧ)
	FROM {{ params.db3 }}.full_order
	--FROM rasp3_v02.full_order
	WHERE 
		date_id = (SELECT current_date FROM cte_dt_rasp LIMIT 1)
		--date_id = cte_current_snapshot_date
		--batch_id_dttm = (SELECT batch_id_dttm FROM cte_dt LIMIT 1)
		AND is_deficit_other = 1	
)
INSERT INTO {{ params.db3 }}.full_order_deficit_goods_type_bridge
--INSERT INTO rasp3_v02.full_order_deficit_goods_type_bridge
(
	batch_id_dttm
	,batch_id_str
	,create_dttm
	,date_id
	,full_order_id
	,type_id
)
SELECT 
	batch_id_dttm 				
	,batch_id_str
	,now('Europe/Moscow')
	,date_id
	,full_order_id
	,type_id	
FROM cte
