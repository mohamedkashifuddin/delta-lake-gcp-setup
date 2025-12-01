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
CREATE EXTERNAL TABLE gold_dataset.dim_customers
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_customers']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_merchants
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_merchants']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_payment_methods
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_payment_methods']
);"

bq query --use_legacy_sql=false --project_id=grand-jigsaw-476820-t1 "
CREATE EXTERNAL TABLE gold_dataset.dim_transaction_status
WITH CONNECTION \`grand-jigsaw-476820-t1.us-central1.delta_biglake_connection\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/gold/dim_transaction_status']
);"

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

echo "✅ Gold external tables created (7)"
echo "✅ Total external tables: 13 (3 bronze + 3 silver + 7 gold)"