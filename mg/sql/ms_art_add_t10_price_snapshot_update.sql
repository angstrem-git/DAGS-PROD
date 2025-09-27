-- mg, 2025-09-27
-- Изменить [mg1].[price_snapshot].[nomenclature_id] по значению [nomenclature_property_id]

UPDATE cte
SET 
	cte.[nomenclature_id] = prop.[nomenclature_id]
	,cte.[price_snapshot_modify_date] = GETDATE()
FROM 
	[mg1].[price_snapshot] AS cte
	INNER JOIN [catalog].[nomenclature_property] AS prop ON cte.[nomenclature_property_id] = prop.[nomenclature_property_id]
WHERE 
	cte.[nomenclature_id] IS NULL
	AND prop.[nomenclature_id] IS NOT NULL
	AND cte.[nomenclature_property_id] IS NOT NULL