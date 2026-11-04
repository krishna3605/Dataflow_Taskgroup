CREATE OR REPLACE TABLE `__AUTHORIZED_TABLE__` AS
SELECT
  emp_id, first_name, last_name, email, department, job_title,
  hire_date, salary, city, state, country, employment_status,
  manager_id, batch_id, loaded_at,
  CASE
    WHEN salary >= 1000000 THEN 'High Salary'
    WHEN salary >= 500000 THEN 'Medium Salary'
    ELSE 'Standard Salary'
  END AS salary_band
FROM `__HISTORY_TABLE__`;
