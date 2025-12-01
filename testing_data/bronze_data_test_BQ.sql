  bq query --use_legacy_sql=false "
SELECT 
  transaction_id, 
  amount, 
  delta_change_type,
  delta_version
FROM bronze_dataset.transactions
WHERE transaction_id='TEST001'
"