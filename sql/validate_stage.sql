ASSERT (SELECT COUNT(*) > 0 FROM `__STAGE_TABLE__`) AS 'Stage table is empty';
ASSERT (
  SELECT COUNTIF(SAFE_CAST(emp_id AS INT64) IS NULL) = 0
  FROM `__STAGE_TABLE__`
) AS 'Invalid employee IDs found';
