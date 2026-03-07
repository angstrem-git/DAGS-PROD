INSERT INTO {{ params.db2 }}.test2
SELECT
    batch_id_dttm,
    now64(3),
    col_a * 20
FROM {{ params.db1 }}.test1
WHERE batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'