-- ClickHouse

INSERT INTO rasp2.art
(
	art_id ,
	nomenclature_id ,
	nomenclature_guid_uuid ,
	nomenclature_guid_str ,
	nomenclature_name ,
	nomenclature_property_id ,
	nomenclature_property_guid_uuid ,
	nomenclature_property_guid_str ,
	nomenclature_property_name
)
SELECT 
	toInt64(art_id),
	toInt64(nomenclature_id),
	toUUID(nomenclature_guid_str),
	nomenclature_guid_str,
	nomenclature_name,
	toInt64(nomenclature_property_id),
	toUUID(nomenclature_property_guid_str),
	nomenclature_property_guid_str,
	nomenclature_property_name
FROM 
	from_mssql.vw_art
WHERE
	toDate(art_create_date) >= today()	-- Только art_id, созданые сегодня
SETTINGS
    max_threads = 1,
    max_block_size = 10000