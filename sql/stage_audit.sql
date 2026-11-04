INSERT INTO `__STAGE_AUDIT_TABLE__`
(run_id, batch_id, table_name, operation, status, row_count, source_uri,
 started_at, completed_at, message)
SELECT
  '{{ run_id }}',
  TO_HEX(SHA256(CAST('{{ run_id }}' AS BYTES))),
  '__STAGE_TABLE__',
  'STAGE_LOAD',
  'SUCCESS',
  COUNT(*),
  'gs://__SOURCE_BUCKET__/__LANDING_OBJECT__',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  'Stage load completed successfully'
FROM `__STAGE_TABLE__`;
