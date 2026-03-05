INSERT INTO {{ params.db2 }}.test2
SELECT
    batch_id_dttm,
    now64(3),
    col_a * 2
FROM {{ params.db1 }}.test1
WHERE batch_id_dttm = '{{ params.batch_id_dttm }}'