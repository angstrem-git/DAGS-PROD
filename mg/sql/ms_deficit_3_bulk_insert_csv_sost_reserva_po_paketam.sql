-- mg, 2024-07-07
-- Импорт файла данных в таблицу базы данных 
BULK INSERT [mg1].[csv_sost_reserva_po_paketam]
FROM 'C:\Data\mg\sost_reserva_po_paketam.txt'
WITH (fieldterminator = '\t', rowterminator = '\n', datafiletype = 'widechar');   -- '\t' - табуляция