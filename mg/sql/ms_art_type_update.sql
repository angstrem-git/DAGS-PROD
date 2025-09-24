-- mg, 2024-11-28
-- Изменить тип артикулов для МИП "Самолет" на "Корпор.заказы": [type_id] = 2

UPDATE 
	[mg2].[art_type]
SET 
	[type_id] = 2		-- "Корпор.заказы": [type_id] = 2
WHERE 
	[art_id] IN
		(
		SELECT 
			ar.[art_id]
		 --   ,ar.[nomenclature_id]
			--,nom.[nomenclature_name]
		 --   ,ar.[nomenclature_property_id]
			--,prop_nom.[nomenclature_property_name]
		FROM 
			[mg2].[art] AS ar
			LEFT JOIN [catalog].[nomenclature] AS nom ON ar.[nomenclature_id] = nom.[nomenclature_id]
			LEFT JOIN [catalog].[nomenclature_property] AS prop_nom ON ar.[nomenclature_property_id] = prop_nom.[nomenclature_property_id]
			LEFT JOIN [mg2].[art_type] AS atp ON ar.[art_id] = atp.[art_id]
		WHERE 
			ar.[nomenclature_id] IN (   
									48461		-- Комплект фасадов индивидуального заказа
									,142238		-- Индивидуальный заказ Кухня
									,160059		-- Набор универсально-сборной мебели для кухни
									)
			AND 
			-- mg-24.09.2025 -----------------------------------------------------------------
				--(  
				--prop_nom.[nomenclature_property_name] LIKE 'АЛХ%' 
				--OR prop_nom.[nomenclature_property_name] LIKE 'ЯМ3%' 
				--OR prop_nom.[nomenclature_property_name] LIKE 'ЯМ4%' 
				--)
			-- mg-24.09.2025 -----------------------------------------------------------------
				prop_nom.[nomenclature_property_name] LIKE '%[0-9]/[0-9]%[0-9]'	-- Самолет
			AND 
				atp.[type_id] <> 2
			-- mg-24.09.2025 -----------------------------------------------------------------

		)