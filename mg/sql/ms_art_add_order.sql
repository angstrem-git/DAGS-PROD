-- mg, 2024-06-22
-- Заполнение справочника Артикулов. Этап-2.
WITH cte 
AS
    (
    -- Поиск новых сочетаний номенклатур и характеристик в заказах
    SELECT DISTINCT
            g.[nomenclature_id]
            ,g.[nomenclature_Property_id] AS [nomenclature_property_id]
    FROM    
            [core].[goods_in_order] AS g  
            INNER JOIN [core].[order] AS rd 
            ON g.[order_id] = rd.[order_id] 
    WHERE	
            rd.[order_doc_date] BETWEEN DATEADD(day, -7, GETDATE()) AND GETDATE() -- за последние 7 дней
)
INSERT INTO [mg2].[art] ( 
        [nomenclature_id]
        ,[nomenclature_property_id]
        ,[art_create_date]
        ,[art_modify_date]
    )
SELECT 
        cte.[nomenclature_id]
        ,cte.[nomenclature_property_id]
        ,GETDATE()
        ,GETDATE()
FROM  
        cte LEFT JOIN [mg2].[art] AS ar
        ON ( cte.[nomenclature_id] = ar.[nomenclature_id] ) 
                AND ( ( cte.[nomenclature_property_id] = ar.[nomenclature_property_id] )   
                    OR  
                    ( cte.[nomenclature_property_id] IS NULL AND ar.[nomenclature_property_id] IS NULL) 
                    )
WHERE 
		cte.[nomenclature_id] IS NOT NULL
		AND ar.[art_id] IS NULL