INSERT INTO rasp2.open_orders_discounts_in_goods_rn_history
(
	key_fk_dec_date_id_discounts_in_goods_in_order_id 
	,key_fk_bit_date_id_offset_20250101_order_id_art_id 
	,key_fk_dec_date_id_order_id 
	,key_fk_dec_order_id_art_id 
	,key_fk_dec_date_id_art_id 
	,date_id 
	,order_id 
	,order_doc_date 
	,order_doc_num 
	,order_guid_str 
	,nomenclature_id 
	,nomenclature_guid_str 	
	,nomenclature_name  
	,nomenclature_property_id 
	,nomenclature_property_guid_str 
	,nomenclature_property_name  
	,art_id	
	,type_id 
	,goods_in_order_id 
	,discounts_in_goods_in_order_id 
	,discounts_in_goods_in_order_VTlineNo 
	,discount_id 
	,discount_name 
	,discount_manual_discount_reason_id 
	,manual_discount_reason_name 
	,discount_is_autodiscount 
	,promotion_id 
	,promotion_name 
	,promotion_discount_id  
	,promotion_discount_name 
	,promotion_is_autopromotion 
	,promotion_period_start 
	,promotion_period_stop 
)
SELECT 
	toInt64(toYYYYMMDD(date_id)) * 100_000_000_000 
		+ toInt64(discounts_in_goods_in_order_id)							AS key_fk_dec_date_id_discounts_in_goods_in_order_id
	,( 
	   toInt64(
	    dateDiff('day', toDate('20250101'), date_id)
	   ) * 33_554_432 + toInt64(order_id) 
	) * 33_554_432 									+ toInt64(art_id)  		AS key_fk_bit_date_id_offset_20250101_order_id_art_id
	,toInt64(toYYYYMMDD(date_id)) * 100_000_000_000 + toInt64(order_id) 	AS key_fk_dec_date_id_order_id
	,toInt64(order_id) * 1_000_000_000 				+ toInt64(art_id) 		AS key_fk_dec_order_id_art_id
	,toInt64(toYYYYMMDD(date_id)) * 100_000_000_000 + toInt64(art_id)  		AS key_fk_dec_date_id_art_id
	,date_id 
	,order_id 
	,order_doc_date 
	,order_doc_num 
	,order_guid_str 
	,nomenclature_id 
	,nomenclature_guid_str 	
	,nomenclature_name  
	,nomenclature_property_id 
	,nomenclature_property_guid_str 
	,nomenclature_property_name  
	,art_id	
	,type_id 
	,goods_in_order_id 
	,discounts_in_goods_in_order_id 
	,discounts_in_goods_in_order_VTlineNo 
	,discount_id 
	,discount_name 
	,discount_manual_discount_reason_id 
	,manual_discount_reason_name 
	,discount_is_autodiscount 
	,promotion_id 
	,promotion_name 
	,promotion_discount_id  
	,promotion_discount_name 
	,promotion_is_autopromotion 
	,promotion_period_start 
	,promotion_period_stop 
FROM 
	from_mssql.vw_open_orders_discounts_in_goods_rn_today
WHERE
	date_id NOT IN (SELECT DISTINCT date_id FROM rasp2.open_orders_discounts_in_goods_rn_history)