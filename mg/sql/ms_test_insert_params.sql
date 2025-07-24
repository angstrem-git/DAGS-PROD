-- mg, 2024-07-06
-- Передача параметра.
INSERT INTO [mg3].[test_params](       
	[param_text]
    ,[param_int]
)
VALUES ( 
	'airflow_param_test'
	, {{params.test_int}}
)