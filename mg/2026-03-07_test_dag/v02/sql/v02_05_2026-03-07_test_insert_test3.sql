INSERT INTO {{ params.db3 }}.test3
SELECT
    t1.batch_id_dttm,
    now64(3),
    t1.col_a + t2.col_b
FROM {{ params.db1 }}.test1 AS t1
	JOIN {{ params.db2 }}.test2 AS t2
		USING(batch_id_dttm)
WHERE t1.batch_id_dttm = '{{ ti.xcom_pull(task_ids="wait_for_batch", key="batch_id_dttm") }}'
       