#!/bin/bash
# =============================================================================
# Script: Create BigQuery External Tables via BigLake
# Description: Creates external tables pointing to Delta tables on GCS
# =============================================================================

set -euo pipefail

# Source environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../configs/environment.sh"

echo "========================================="
echo "Creating BigQuery External Tables"
echo "========================================="

# Check if BigLake connection exists
echo "Checking BigLake connection..."
if ! bq show --connection \
    --location=${REGION} \
    --project_id=${PROJECT_ID} \
    ${BIGLAKE_CONNECTION} &>/dev/null; then
    
    echo "⚠️  BigLake connection '${BIGLAKE_CONNECTION}' does not exist"
    echo "Creating connection..."
    
    bq mk --connection \
        --location=${REGION} \
        --project_id=${PROJECT_ID} \
        --connection_type=CLOUD_RESOURCE \
        ${BIGLAKE_CONNECTION}
    
    echo "✅ Connection created"
    
    # Get service account
    SA_EMAIL=$(bq show --connection \
        --location=${REGION} \
        --project_id=${PROJECT_ID} \
        ${BIGLAKE_CONNECTION} \
        --format=json | grep serviceAccountId | cut -d'"' -f4)
    
    echo "Service Account: ${SA_EMAIL}"
    echo ""
    echo "Granting GCS read permissions..."
    
    # Grant objectViewer role
    gsutil iam ch serviceAccount:${SA_EMAIL}:objectViewer gs://${BUCKET_NAME}
    
    echo "✅ Permissions granted"
else
    echo "✅ Connection already exists: ${BIGLAKE_CONNECTION}"
fi

echo ""
echo "Creating BigQuery datasets..."

# Create datasets
datasets=("bronze_dataset" "silver_dataset" "gold_dataset")

for dataset in "${datasets[@]}"; do
    if bq ls -d --project_id=${PROJECT_ID} | grep -q ${dataset}; then
        echo "  ${dataset} already exists"
    else
        bq mk --location=${REGION} --project_id=${PROJECT_ID} ${dataset}
        echo "  ✅ Created ${dataset}"
    fi
done

echo ""
echo "Creating external tables..."

# Connection string for queries
CONNECTION_STRING="${PROJECT_ID}.${REGION}.${BIGLAKE_CONNECTION}"

# Bronze external tables
echo ""
echo "Bronze Dataset..."

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE bronze_dataset.transactions
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/bronze/transactions']
);" && echo "  ✅ bronze_dataset.transactions"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE bronze_dataset.job_control
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/metadata/bronze_job_control']
);" && echo "  ✅ bronze_dataset.job_control"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE bronze_dataset.quarantine
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/quarantine/bronze_transactions']
);" && echo "  ✅ bronze_dataset.quarantine"

# Silver external tables
echo ""
echo "Silver Dataset..."

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE silver_dataset.transactions
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/silver/transactions']
);" && echo "  ✅ silver_dataset.transactions"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE silver_dataset.job_control
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/metadata/silver_job_control']
);" && echo "  ✅ silver_dataset.job_control"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE silver_dataset.quarantine
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/quarantine/silver_transactions']
);" && echo "  ✅ silver_dataset.quarantine"

# Gold external tables
echo ""
echo "Gold Dataset..."

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.fact_transactions
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/gold/fact_transactions']
);" && echo "  ✅ gold_dataset.fact_transactions"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.dim_customers
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/gold/dim_customers']
);" && echo "  ✅ gold_dataset.dim_customers"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.dim_merchants
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/gold/dim_merchants']
);" && echo "  ✅ gold_dataset.dim_merchants"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.dim_payment_methods
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/gold/dim_payment_methods']
);" && echo "  ✅ gold_dataset.dim_payment_methods"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.dim_transaction_status
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/gold/dim_transaction_status']
);" && echo "  ✅ gold_dataset.dim_transaction_status"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.dim_date
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/gold/dim_date']
);" && echo "  ✅ gold_dataset.dim_date"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} "
CREATE OR REPLACE EXTERNAL TABLE gold_dataset.job_control
WITH CONNECTION \`${CONNECTION_STRING}\`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://${BUCKET_NAME}/metadata/gold_job_control']
);" && echo "  ✅ gold_dataset.job_control"

echo ""
echo "========================================="
echo "✅ ALL BIGQUERY EXTERNAL TABLES CREATED"
echo "========================================="
echo ""
echo "Total external tables: 13"
echo "  Bronze: 3"
echo "  Silver: 3"
echo "  Gold: 7"
echo ""
echo "Query example:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM bronze_dataset.transactions'"
echo ""
echo "⚠️  Note: Set BigQuery location to '${REGION}' in the UI"
echo "   (Query settings → Processing location → ${REGION})"
echo ""
echo "✅ SETUP COMPLETE!"