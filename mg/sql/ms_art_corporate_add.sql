-- mg, 2025-09-26
-- Заполнение справочника продукции корп.клиентов.

WITH cte AS
(
	SELECT 
		ar.[art_id]							AS art_id
		,ar.[nomenclature_id]				AS nomenclature_id
		,nom.[nomenclature_name]			AS nomenclature_name
		,CASE
			WHEN nom.[nomenclature_name] LIKE '%BL%'	THEN 'BL'
			WHEN nom.[nomenclature_name] LIKE '%Comf%'	THEN 'Comf'
			WHEN nom.[nomenclature_name] LIKE '%Cls%'	THEN 'Cls'
			WHEN nom.[nomenclature_name] LIKE '%Sst%'	THEN 'Sst'
			WHEN nom.[nomenclature_name] LIKE '%WG%'	THEN 'WG'
			ELSE '#'
		END									AS abbr
		,ar.[nomenclature_property_id]		AS nomenclature_property_id
		,prop.[nomenclature_property_name]	AS nomenclature_property_name
		,atp.[type_id]						AS a_type_id
		,acr.[corporate_id]					AS corporate_id	
		,par1.[nomenclature_id]				AS par_id_1
		,par1.[nomenclature_name]			AS par_name_1
		--,par2.[nomenclature_id]				AS par_id_2
		--,par2.[nomenclature_name]			AS par_name_2
		--,par3.[nomenclature_id]				AS par_id_3
		--,par3.[nomenclature_name]			AS par_name_3
		--,par4.[nomenclature_id]				AS par_id_4
		--,par4.[nomenclature_name]			AS par_name_4
		--,par5.[nomenclature_id]				AS par_id_5
		--,par5.[nomenclature_name]			AS par_name_5
		--,par6.[nomenclature_id]				AS par_id_6
		--,par6.[nomenclature_name]			AS par_name_6
	FROM 
		[mg2].[art] AS ar
		LEFT JOIN [mg2].[art_corporate] AS acr ON ar.[art_id] = acr.[art_id]
		LEFT JOIN [mg2].[art_type] AS atp ON ar.[art_id] = atp.[art_id]
		LEFT JOIN [catalog].[nomenclature] AS nom ON ar.[nomenclature_id] = nom.[nomenclature_id]
		LEFT JOIN [catalog].[nomenclature] AS par1 ON nom.[nomenclature_parent_id] = par1.[nomenclature_id]
		--LEFT JOIN [catalog].[nomenclature] AS par2 ON par1.[nomenclature_parent_id] = par2.[nomenclature_id]
		--LEFT JOIN [catalog].[nomenclature] AS par3 ON par2.[nomenclature_parent_id] = par3.[nomenclature_id]
		--LEFT JOIN [catalog].[nomenclature] AS par4 ON par3.[nomenclature_parent_id] = par4.[nomenclature_id]
		--LEFT JOIN [catalog].[nomenclature] AS par5 ON par4.[nomenclature_parent_id] = par5.[nomenclature_id]
		--LEFT JOIN [catalog].[nomenclature] AS par6 ON par5.[nomenclature_parent_id] = par6.[nomenclature_id]
		LEFT JOIN [catalog].[nomenclature_property] AS prop ON ar.[nomenclature_property_id] = prop.[nomenclature_property_id]
	WHERE  
		atp.[type_id] = 2				-- 2 = Корп.клиенты
		AND acr.[corporate_id] IS NULL	-- этот [art_id] ещё не записан в [mg2].[art_corporate]
)

INSERT INTO [mg2].[art_corporate] ([art_id], [corporate_id])
SELECT 
	cte.art_id
	,rp1.corporate_id
	--,cte.a_type_id
	--,cte.nomenclature_name
	--,cte.nomenclature_property_name
	--,cte.par_id_1
	--,cte.par_name_1
	--,cte.par_id_2
	--,cte.par_name_2
	--,rp1.[corp_client_name]
	--,rp1.[corp_category_name]
	--,rp1.[corp_subcategory_name]
FROM  
	cte
	INNER JOIN [mg2].[corporate] AS rp1 ON cte.[par_id_1] = rp1.[parent_id_1]

UNION ALL

SELECT 
	cte.art_id
	,rp2.corporate_id
	--,cte.a_type_id
	--,cte.nomenclature_name
	--,cte.nomenclature_property_name
	--,cte.par_id_1
	--,cte.par_name_1
	--,cte.par_id_2
	--,cte.par_name_2
	--,rp2.[corp_client_name]
	--,rp2.[corp_category_name]
	--,rp2.[corp_subcategory_name]
FROM  
	cte
	INNER JOIN [mg2].[corporate] AS rp2 ON cte.[par_id_1] = rp2.[parent_id_2]

UNION ALL

SELECT 
	cte.art_id
	,rp3.corporate_id
	--,cte.a_type_id
	--,cte.nomenclature_name
	--,cte.nomenclature_property_name
	--,cte.par_id_1
	--,cte.par_name_1
	--,cte.par_id_2
	--,cte.par_name_2
	--,rp3.[corp_client_name]
	--,rp3.[corp_category_name]
	--,rp3.[corp_subcategory_name]
FROM  
	cte
	INNER JOIN [mg2].[corporate] AS rp3 
		ON cte.[par_id_1] = rp3.[parent_id_3]
		AND 
			(  
				cte.[abbr] = rp3.[abbr_3_1]
				OR cte.[abbr] = rp3.[abbr_3_2]
				OR cte.[abbr] = rp3.[abbr_3_3]
			)