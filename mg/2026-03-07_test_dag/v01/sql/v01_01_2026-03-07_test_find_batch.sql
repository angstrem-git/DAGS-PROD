SELECT batch_id_dttm
FROM {p_db1}.test1
WHERE batch_id_dttm NOT IN
(
    SELECT batch_id_dttm
    FROM {p_db2}.test1_processed
)
ORDER BY batch_id_dttm
LIMIT 1
FORMAT JSON
 