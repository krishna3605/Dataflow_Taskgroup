CREATE OR REPLACE VIEW `__REPORTING_VIEW__` AS
SELECT
  emp_id, first_name, last_name, department, job_title, hire_date,
  salary, salary_band, city, state, country, employment_status, manager_id
FROM `__AUTHORIZED_TABLE__`;
