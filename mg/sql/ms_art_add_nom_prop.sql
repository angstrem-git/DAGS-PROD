-- mg, 2024-06-22
-- Заполнение справочника Артикулов. Этап-1.
INSERT INTO [mg2].[art] ( 
	    	        [nomenclature_id]
	    	        ,[nomenclature_property_id]
	    	        ,[art_create_date]
	    	        ,[art_modify_date]
	          )
-- Поиск новых сочетаний номенклатур и характеристик в справочнике [nomenclature_property]
SELECT   
		nom_prop.[nomenclature_id]	
		,nom_prop.[nomenclature_property_id]
		,GETDATE()
		,GETDATE()
FROM 
		[catalog].[nomenclature_property] AS nom_prop 
		LEFT JOIN [mg2].[art] AS ar
		ON ( nom_prop.[nomenclature_id] = ar.[nomenclature_id] ) 
		    AND ( ( nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] )   
		    		OR  
		    		( nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) 
		    	)
WHERE 
		nom_prop.[nomenclature_id] IS NOT NULL
		AND ar.[art_id] IS NULL