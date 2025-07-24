-- mg, 2024-11-28
-- Заполнение справочника Типов Артикулов.

WITH cte AS
(
	SELECT 
			COALESCE(
				s10.[nomenclature_id]
				,s09.[nomenclature_id]
				,s08.[nomenclature_id]			
				,s07.[nomenclature_id]	
				,s06.[nomenclature_id]
				,s05.[nomenclature_id]	
				,s04.[nomenclature_id]	
				,s03.[nomenclature_id]	
				,s02.[nomenclature_id]	
				,s01.[nomenclature_id]					
					)						AS [nomenclature_id]
			,COALESCE(
				s10.[nomenclature_name]
				,s09.[nomenclature_name]
				,s08.[nomenclature_name]
				,s07.[nomenclature_name]
				,s06.[nomenclature_name]
				,s05.[nomenclature_name]
				,s04.[nomenclature_name]
				,s03.[nomenclature_name]
				,s02.[nomenclature_name]
				,s01.[nomenclature_name]
					)						AS [nomenclature_name]
			,s00.[nomenclature_id]			AS s00_id
			,s00.[nomenclature_name]		AS s00_name
			,s00.[nomenclature_parent_id]	AS s00_parent
			,s00.[nomenclature_is_Folder]	AS s00_is_folder
			,s01.[nomenclature_id]			AS s01_id
			,s01.[nomenclature_name]		AS s01_name
			,s02.[nomenclature_id]			AS s02_id
			,s02.[nomenclature_name]		AS s02_name
			,s03.[nomenclature_id]			AS s03_id
			,s03.[nomenclature_name]		AS s03_name
			,s04.[nomenclature_id]			AS s04_id
			,s04.[nomenclature_name]		AS s04_name
			,s05.[nomenclature_id]			AS s05_id
			,s05.[nomenclature_name]		AS s05_name
			,s06.[nomenclature_id]			AS s06_id
			,s06.[nomenclature_name]		AS s06_name
			,s07.[nomenclature_id]			AS s07_id
			,s07.[nomenclature_name]		AS s07_name
			,s08.[nomenclature_id]			AS s08_id
			,s08.[nomenclature_name]		AS s08_name
			,s09.[nomenclature_id]			AS s09_id
			,s09.[nomenclature_name]		AS s09_name
			,s10.[nomenclature_id]			AS s10_id
			,s10.[nomenclature_name]		AS s10_name
	FROM [catalog].[nomenclature] AS s00
			LEFT JOIN [catalog].[nomenclature] AS s01 ON s00.[nomenclature_id] = s01.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s02 ON s01.[nomenclature_id] = s02.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s03 ON s02.[nomenclature_id] = s03.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s04 ON s03.[nomenclature_id] = s04.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s05 ON s04.[nomenclature_id] = s05.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s06 ON s05.[nomenclature_id] = s06.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s07 ON s06.[nomenclature_id] = s07.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s08 ON s07.[nomenclature_id] = s08.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s09 ON s08.[nomenclature_id] = s09.[nomenclature_parent_id]
			LEFT JOIN [catalog].[nomenclature] AS s10 ON s09.[nomenclature_id] = s10.[nomenclature_parent_id]
			
	WHERE 
			s00.[nomenclature_parent_id] IS NULL  
)

INSERT INTO [mg2].[art_type] ([art_id], [type_id])

-- 01.СТАНДАРТ
SELECT 
		ar.[art_id]						
		,1	AS [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND ( 
				cte.s02_id IN	(
							290				-- Гостиные
							,2918			-- Детские
							,156439			-- Диванные подушки
							,7996			-- Диваны
							,12408			-- Журнальные и кофейные столы
							,664			-- Кабинеты
							,30128			-- Кресла
							,112941			-- Кровати мягкие
							,4521			-- Малые формы
							,120350			-- Мебель для общей комнаты
							,15174			-- Отдельные предметы мебели
							,773			-- Прихожие
							,25167			-- Пуфы
							,366			-- Спальни
							-- mg-2025-07-24
							,172871			-- СиУ	
							-- mg-2025-07-24 
						)
				OR cte.s01_id = 1048			-- Пакеты универсальные
		)

UNION 

-- 02.Корпор.заказы
SELECT 
		ar.[art_id]
		,2									-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND ( 
				cte.s02_id IN	(
							1408			-- Гостиничная мебель
							,121662			-- Грин-3
							,15372			-- ИКЕА
							,141944			-- Мария
							,148920			-- Мир Дерева
							,141942			-- Самсон
							,138551			-- Церсанит
							,54830			-- Шатура
							,145241			-- ЭкоНива
							,160650			-- Экстраверт
							,134211			-- Эльба (Каркасы)
							,128468			-- Эльба (Фасады)
							,157608			-- Яндекс Маркет
							,160097			-- Самолет
							,163402			-- ИП Мамыкин
						)
				OR cte.s01_id = 270			-- Корпоративные заказы
				OR cte.s01_id = 160097		-- Самолет
		)

UNION 

-- 03.МИП
SELECT 
		ar.[art_id]
		,3								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND (cte.s02_id = 30468							-- Индивидуальные заказы
			OR cte.s02_id = 133							-- Индивидуальные заказы
			OR cte.[nomenclature_name] LIKE '% ИЗ' )	-- Анника ИЗ, Клио ИЗ, Ладога ИЗ

UNION 

-- 04.Кухни
SELECT 
		ar.[art_id]
		,4								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND (
				cte.s01_id = 198			-- Кухни
				OR cte.s02_id = 133023		-- Пакеты / Кухни
		)

UNION 

-- 05.Ванные
SELECT 
		ar.[art_id]
		,5 								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND cte.s02_id = 5738		-- Мебель для ванных комнат

UNION 

-- 06.Сторонняя
SELECT 
		ar.[art_id]
		,6								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND ( 
				cte.s00_id = 10			-- Сторонняя продукция
				OR cte.s03_id = 137		-- Подушки 40*40
		)

UNION 

-- 07.Пакеты МиК
-- Отдельные номенклатуры


-- 08.Экспер.образцы
SELECT 
		ar.[art_id]
		,8								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND cte.s02_id = 8770			-- Экспериментальные образцы / Ангстрем
		
UNION 

-- 09.Рекламации
SELECT 
		ar.[art_id]
		,9								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND (
				cte.s00_id = 53443			-- Деталь по рекламации 
				OR cte.s01_id = 1440			-- Рекламации
		)

UNION 
		
-- 10.Экспозиторы
SELECT 
		ar.[art_id]
		,10								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND cte.s03_id = 76895 

UNION 
		
-- 11.Оформление ТТ
SELECT 
		ar.[art_id]
		,11								-- [type_id]
		--,cte.[nomenclature_id]
		--,cte.[nomenclature_name]
		--,nom_prop.[nomenclature_property_id]
		--,nom_prop.[nomenclature_property_name]
FROM
		cte	
		LEFT JOIN [mg2].[art] AS ar ON cte.[nomenclature_id] = ar.[nomenclature_id] 	
		LEFT JOIN [catalog].[nomenclature_property] AS nom_prop ON 
				( (nom_prop.[nomenclature_property_id] = ar.[nomenclature_property_id] ) OR  
				  (nom_prop.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) )			
WHERE 
		ar.[art_id] IS NOT NULL
		AND ar.[art_id] NOT IN (SELECT [art_id] FROM [mg2].[art_type])
		AND (
				cte.s00_id = 53501		-- Оформление торговой точки (рекламная продукция)  
				OR cte.s00_id = 56629		-- Оформление торговой точки (тренд-борды и макеты ТВ)  
				OR cte.s01_id = 93			-- Оформление торговой точки   
		)


-- 12.Подложки ДСП
-- Отдельные номенклатуры