#!/bin/bash
# =============================================================================
# Script: Create GCS Buckets and Folder Structure
# Description: Sets up medallion architecture (bronze/silver/gold) on GCS
# =============================================================================

set -euo pipefail

# Source environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../configs/environment.sh"

echo "========================================="
echo "Creating GCS Bucket Structure"
echo "========================================="

# Check if bucket already exists
if gsutil ls -b gs://${BUCKET_NAME} &>/dev/null; then
    echo "⚠️  Bucket 'gs://${BUCKET_NAME}' already exists"
    echo "Skipping bucket creation, will create folders..."
else
    echo "Creating GCS bucket: gs://${BUCKET_NAME}"
    echo "Location: ${REGION}"
    echo ""
    
    # Create main bucket
    gsutil mb -c STANDARD -l ${REGION} gs://${BUCKET_NAME}/
    
    echo "✅ Bucket created"
fi

echo ""
echo "Creating medallion architecture folders..."

# Create folder structure with .keep files
folders=(
    "raw"
    "bronze/transactions"
    "silver/transactions"
    "gold/fact_transactions"
    "gold/dim_customers"
    "gold/dim_merchants"
    "gold/dim_payment_methods"
    "gold/dim_transaction_status"
    "gold/dim_date"
    "warehouse"
    "quarantine/bronze_transactions"
    "quarantine/silver_transactions"
    "metadata/bronze_job_control"
    "metadata/silver_job_control"
    "metadata/gold_job_control"
    "metadata/bronze_job_control_archive"
    "metadata/silver_job_control_archive"
    "metadata/gold_job_control_archive"
    "secrets"
)

for folder in "${folders[@]}"; do
    echo "  Creating: ${folder}/"
    echo "" | gsutil cp - gs://${BUCKET_NAME}/${folder}/.keep 2>/dev/null || true
done

echo ""
echo "✅ Folder structure created"
echo ""
echo "Bucket structure:"
gsutil ls gs://${BUCKET_NAME}/ | head -10
echo "... and more"

echo ""
echo "Now encrypting and uploading hive password..."

# Generate random password
HIVE_PASSWORD=$(openssl rand -base64 16)
echo "Generated hive password: ${HIVE_PASSWORD}"
echo "⚠️  Save this password somewhere safe (you won't see it again)"

# Encrypt password
echo -n "${HIVE_PASSWORD}" | gcloud kms encrypt \
    --location=${REGION} \
    --keyring=${KMS_KEYRING} \
    --key=${KMS_KEY} \
    --plaintext-file=- \
    --ciphertext-file=/tmp/hive-password.encrypted \
    --project=${PROJECT_ID}

# Upload to GCS
gsutil cp /tmp/hive-password.encrypted gs://${BUCKET_NAME}/secrets/

# Clean up local copy
rm /tmp/hive-password.encrypted

echo ""
echo "✅ Password encrypted and uploaded to gs://${BUCKET_NAME}/secrets/"
echo ""
echo "Next step: Run 04-create-dataproc.sh"