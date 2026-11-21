-- v02_001_wait_for_batch.sql
SELECT
	(
		SELECT count()	
		FROM rasp2.open_orders_goods_history_rn
		WHERE date_id = toDate('{p_date_id}')
	)												AS count_open_orders
	,
	(
		SELECT count()	
		FROM rasp2.open_orders_discounts_in_goods_rn_history
		WHERE date_id = toDate('{p_date_id}')
	)												AS count_orders_discounts
	,
	(
		SELECT count()	
		FROM rasp2.mg_VPointStatusIndividualOrders
		WHERE point_date = subtractDays(toDate('{p_date_id}'), 1)
	)												AS count_status_mip
LIMIT 1
FORMAT JSON



 