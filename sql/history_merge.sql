MERGE `__HISTORY_TABLE__` T
USING (
  SELECT
    SAFE_CAST(emp_id AS INT64) AS emp_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    TRIM(department) AS department,
    TRIM(job_title) AS job_title,
    SAFE_CAST(hire_date AS DATE) AS hire_date,
    SAFE_CAST(salary AS NUMERIC) AS salary,
    TRIM(city) AS city,
    TRIM(state) AS state,
    TRIM(country) AS country,
    TRIM(employment_status) AS employment_status,
    SAFE_CAST(NULLIF(TRIM(manager_id), '') AS INT64) AS manager_id
  FROM `__STAGE_TABLE__`
  WHERE SAFE_CAST(emp_id AS INT64) IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SAFE_CAST(emp_id AS INT64)
    ORDER BY SAFE_CAST(hire_date AS DATE) DESC
  ) = 1
) S
ON T.emp_id = S.emp_id
WHEN MATCHED THEN UPDATE SET
  first_name = S.first_name,
  last_name = S.last_name,
  email = S.email,
  department = S.department,
  job_title = S.job_title,
  hire_date = S.hire_date,
  salary = S.salary,
  city = S.city,
  state = S.state,
  country = S.country,
  employment_status = S.employment_status,
  manager_id = S.manager_id,
  batch_id = TO_HEX(SHA256(CAST('{{ run_id }}' AS BYTES))),
  loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  emp_id, first_name, last_name, email, department, job_title,
  hire_date, salary, city, state, country, employment_status,
  manager_id, batch_id, loaded_at
) VALUES (
  S.emp_id, S.first_name, S.last_name, S.email, S.department, S.job_title,
  S.hire_date, S.salary, S.city, S.state, S.country, S.employment_status,
  S.manager_id,
  TO_HEX(SHA256(CAST('{{ run_id }}' AS BYTES))),
  CURRENT_TIMESTAMP()
);
