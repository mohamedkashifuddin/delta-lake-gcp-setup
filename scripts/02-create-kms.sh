#!/bin/bash
# =============================================================================
# Script: Create KMS Encryption Key
# Description: Sets up KMS keyring and encryption key for hive password
# =============================================================================

set -euo pipefail

# Source environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../configs/environment.sh"

echo "========================================="
echo "Creating KMS Keyring and Encryption Key"
echo "========================================="

# Create keyring (ignore error if exists)
echo "Creating KMS keyring: ${KMS_KEYRING}"
gcloud kms keyrings create ${KMS_KEYRING} \
    --location=${REGION} \
    --project=${PROJECT_ID} 2>/dev/null || echo "Keyring already exists, continuing..."

echo ""
echo "Creating KMS encryption key: ${KMS_KEY}"

# Check if key already exists
if gcloud kms keys describe ${KMS_KEY} \
    --location=${REGION} \
    --keyring=${KMS_KEYRING} \
    --project=${PROJECT_ID} &>/dev/null; then
    echo "⚠️  KMS key '${KMS_KEY}' already exists"
    echo "Skipping key creation..."
else
    # Create encryption key
    gcloud kms keys create ${KMS_KEY} \
        --location=${REGION} \
        --keyring=${KMS_KEYRING} \
        --purpose=encryption \
        --project=${PROJECT_ID}
    
    echo "✅ KMS key created successfully"
fi

echo ""
echo "Granting decrypt permission to Dataproc service account..."

# Grant decrypt permission to Dataproc service account
gcloud kms keys add-iam-policy-binding ${KMS_KEY} \
    --location=${REGION} \
    --keyring=${KMS_KEYRING} \
    --member=serviceAccount:${SERVICE_ACCOUNT} \
    --role=roles/cloudkms.cryptoKeyDecrypter \
    --project=${PROJECT_ID}

echo ""
echo "✅ KMS setup complete"
echo ""
echo "Key URI: ${KMS_KEY_URI}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo ""
echo "Next step: Run 03-create-buckets.sh"