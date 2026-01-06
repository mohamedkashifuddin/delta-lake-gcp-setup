# Task 19: Create BigQuery Datasets
bq mk --location=us-central1 --project_id=grand-jigsaw-476820-t1 bronze_dataset
bq mk --location=us-central1 --project_id=grand-jigsaw-476820-t1 silver_dataset
bq mk --location=us-central1 --project_id=grand-jigsaw-476820-t1 gold_dataset

echo "✅ Datasets created"

# Task 20: Bronze External Tables (3 tables)
bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE bronze_dataset.transactions
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/bronze/transactions']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE bronze_dataset.job_control
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE bronze_dataset.quarantine
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions']
);"

echo "✅ Bronze external tables created (3)"

# Task 21: Silver External Tables (3 tables)
bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE silver_dataset.transactions
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/silver/transactions']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE silver_dataset.job_control
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/metadata/silver_job_control']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE silver_dataset.quarantine
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/quarantine/silver_transactions']
);"

echo "✅ Silver external tables created (3)"

# Task 22: Gold External Tables (7 tables)
bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.fact_transactions
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/fact_transactions']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_customer
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_customer']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_merchant
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_merchant']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_payment_method
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_payment_method']
);
"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_status
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_status']
);
"
bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_date
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_date']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.job_control
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/metadata/gold_job_control']
);"
-- 1. GOLD FACT TRANSACTIONS STAGING
CREATE EXTERNAL TABLE gold_dataset.fact_transactions_staging
WITH CONNECTION grand-jigsaw-476820-t1.us-central1.delta_biglake_connection
OPTIONS (
format = 'DELTA_LAKE',
uris = ['gs://delta-lake-payment-gateway-476820/gold/fact_transactions_staging']
);

-- 2. GOLD CUSTOMER DIMENSION STAGING
CREATE EXTERNAL TABLE gold_dataset.dim_customer_staging
WITH CONNECTION grand-jigsaw-476820-t1.us-central1.delta_biglake_connection
OPTIONS (
format = 'DELTA_LAKE',
uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_customer_staging']
);

-- 3. GOLD MERCHANT DIMENSION STAGING
CREATE EXTERNAL TABLE gold_dataset.dim_merchant_staging
WITH CONNECTION grand-jigsaw-476820-t1.us-central1.delta_biglake_connection
OPTIONS (
format = 'DELTA_LAKE',
uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_merchant_staging']
);

-- 4. GOLD PAYMENT METHOD DIMENSION STAGING
-- NOTE: We are assuming this DDL was previously run in Spark to create the Delta path.
CREATE EXTERNAL TABLE gold_dataset.dim_payment_method_staging
WITH CONNECTION grand-jigsaw-476820-t1.us-central1.delta_biglake_connection
OPTIONS (
format = 'DELTA_LAKE',
uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_payment_method_staging']
);

-- 5. GOLD TRANSACTION STATUS DIMENSION STAGING
-- NOTE: We are assuming this DDL was previously run in Spark to create the Delta path.
CREATE EXTERNAL TABLE gold_dataset.dim_transaction_status_staging
WITH CONNECTION grand-jigsaw-476820-t1.us-central1.delta_biglake_connection
OPTIONS (
format = 'DELTA_LAKE',
uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_transaction_status_staging']
);



echo "✅ Gold external tables created (10)"
echo "✅ Total external tables: 19 (4 bronze + 4 silver + 10 gold)"
