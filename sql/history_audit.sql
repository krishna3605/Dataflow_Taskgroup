INSERT INTO `__HISTORY_AUDIT_TABLE__`
(run_id, batch_id, table_name, operation, status, row_count, source_uri,
 started_at, completed_at, message)
SELECT
  '{{ run_id }}',
  TO_HEX(SHA256(CAST('{{ run_id }}' AS BYTES))),
  '__HISTORY_TABLE__',
  'STAGE_TO_HISTORY',
  'SUCCESS',
  COUNT(*),
  'gs://__SOURCE_BUCKET__/__LANDING_OBJECT__',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  'History merge completed successfully'
FROM `__HISTORY_TABLE__`
WHERE batch_id = TO_HEX(SHA256(CAST('{{ run_id }}' AS BYTES)));
