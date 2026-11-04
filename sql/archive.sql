MERGE `__ARCHIVE_TABLE__` T
USING (
  SELECT *
  FROM `__HISTORY_TABLE__`
  WHERE batch_id = TO_HEX(SHA256(CAST('{{ run_id }}' AS BYTES)))
) S
ON T.emp_id = S.emp_id AND T.batch_id = S.batch_id
WHEN NOT MATCHED THEN
  INSERT (
    emp_id, first_name, last_name, email, department, job_title,
    hire_date, salary, city, state, country, employment_status,
    manager_id, batch_id, loaded_at
  )
  VALUES (
    S.emp_id, S.first_name, S.last_name, S.email, S.department, S.job_title,
    S.hire_date, S.salary, S.city, S.state, S.country, S.employment_status,
    S.manager_id, S.batch_id, S.loaded_at
  );
