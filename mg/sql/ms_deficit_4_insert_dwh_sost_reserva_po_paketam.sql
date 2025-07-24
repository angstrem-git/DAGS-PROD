-- mg, 2024-07-07
-- Заполнение таблицы "Состояние резерва по пакетам" с кодами DWH.
WITH cte AS (
	SELECT 
			-- Удаляем кавычки ('"'), которые Excel может подставить при выгрузке в txt-файл
			-- При выгрузке в txt-файл Excel преобразует [Кухня "Версаль"] -> ["Кухня ""Версаль"""]: (1) Добавляет ["] в начало и в конец; (2) Заменяет ["] на [""]
			REPLACE(
				CASE LEFT( REPLACE([paсket]	, '""', '#' ), 1) 
					WHEN '"' THEN TRIM ('"' FROM REPLACE([paсket]	, '""', '#' ) ) 
					ELSE REPLACE([paсket]	, '""', '#' )
				END
				, '#'
				, '"'
			)																						AS [paсket]		

			,REPLACE(
				CASE LEFT( REPLACE([kharakteristika]	, '""', '#' ), 1) 
					WHEN '"' THEN TRIM ('"' FROM REPLACE([kharakteristika]	, '""', '#' ) ) 
					ELSE REPLACE([kharakteristika]	, '""', '#' )
				END
				, '#'
				, '"'
			)																						AS [kharakteristika]

			,[status_paсketa]
			,[na_sklade]
			,[v_zakazakh]
			,[dolg_pr_va]
			,[obchiy_vykhod_pr_va]
	FROM 
			[mg1].[csv_sost_reserva_po_paketam]
)

INSERT INTO [mg2].[dwh_sost_reserva_po_paketam] (  
		[deficit_snapshot_id]
		,[art_id]
		,[nomenclature_id]
		,[nomenclature_property_id]
		,[status_art]
		,[na_sklade]
		,[v_zakazakh]
		,[dolg_pr_va]
		,[obchiy_vykhod_pr_va]
)

SELECT
		fk.[deficit_snapshot_id]
		,csv.[art_id]
		,csv.[nomenclature_id]
		,csv.[nomenclature_property_id]
		,csv.[status_paсketa]
		,ISNULL(csv.[na_sklade], 0) 
		,ISNULL(csv.[v_zakazakh], 0) 
		,ISNULL(csv.[dolg_pr_va], 0) 
		,ISNULL(csv.[obchiy_vykhod_pr_va], 0)
FROM 
		-- id последней строки, которую только что добавили
		(
		SELECT 
				MAX([deficit_snapshot_id]) AS [deficit_snapshot_id]
		FROM 
				[mg2].[deficit_snapshot] 
		) AS fk	
		CROSS JOIN
		(
		SELECT 
				cte.[paсket]
				,cte.[kharakteristika]
				,cte.[status_paсketa]
				,cte.[na_sklade]
				,cte.[v_zakazakh]
				,cte.[dolg_pr_va]
				,cte.[obchiy_vykhod_pr_va]
				,art.[art_id]
				,art.[nomenclature_id]
				,art.[nomenclature_name]
				,art.[nomenclature_property_id]
				,art.[nomenclature_property_name]
		FROM 
				cte 
				LEFT JOIN (
							SELECT 
									ar.[art_id]
									,nom.[nomenclature_id]
									,nom.[nomenclature_name]
									,nom_prop.[nomenclature_property_id]
									,nom_prop.[nomenclature_property_name]
							FROM 
									[mg2].[art] AS ar 
									LEFT JOIN [catalog].[nomenclature] AS nom 
										ON ar.[nomenclature_id] = nom.[nomenclature_id]
									LEFT JOIN [catalog].[nomenclature_property] AS nom_prop 
										ON ar.[nomenclature_property_id] = nom_prop.[nomenclature_property_id]
						) AS art
					ON	( ( cte.[paсket] = art.[nomenclature_name] )
							AND 
							(	( cte.[kharakteristika] = art.[nomenclature_property_name] ) OR 
								( cte.[kharakteristika] IS NULL AND art.[nomenclature_property_name] IS NULL ) )
						)
		) AS csv
		

