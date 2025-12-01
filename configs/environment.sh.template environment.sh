#!/bin/bash
# =============================================================================
# Delta Lake on GCP - Environment Configuration Template
# =============================================================================
# Instructions:
# 1. Copy this file: cp environment.sh.template environment.sh
# 2. Fill in your project details below
# 3. Source the file: source environment.sh
# 4. Run setup scripts in order (01 -> 05)
# =============================================================================

# ----- GCP Project Configuration -----
export PROJECT_ID="your-project-id"                    # Replace with your GCP project ID
export REGION="us-central1"                            # Region for all resources
export ZONE="us-central1-b"                            # Zone for Dataproc cluster

# ----- Cloud SQL Configuration -----
export CLOUDSQL_INSTANCE="hive-metastore-mysql"        # Cloud SQL instance name
export CLOUDSQL_TIER="db-n1-standard-1"                # Machine type
export CLOUDSQL_STORAGE_SIZE="10GB"                    # Storage size

# ----- KMS Configuration -----
export KMS_KEYRING="dataproc-keys"                     # KMS keyring name
export KMS_KEY="hive-password-key"                     # Encryption key name

# ----- GCS Configuration -----
export BUCKET_NAME="delta-lake-${PROJECT_ID}"          # GCS bucket name (must be globally unique)

# ----- Dataproc Configuration -----
export CLUSTER_NAME="dataproc-delta-cluster-final"     # Dataproc cluster name
export DATAPROC_IMAGE="2.2-debian12"                   # Dataproc image version (CRITICAL: must be 2.2)
export MASTER_MACHINE_TYPE="n4-standard-2"             # Master node machine type
export WORKER_MACHINE_TYPE="n4-standard-2"             # Worker node machine type
export NUM_WORKERS="2"                                 # Number of worker nodes (use 0 for single-node)
export MASTER_DISK_SIZE="50"                           # Master boot disk size in GB
export WORKER_DISK_SIZE="50"                           # Worker boot disk size in GB

# ----- Auto-calculated Variables (DO NOT MODIFY) -----
export PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)" 2>/dev/null || echo "")
export SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
export KMS_KEY_URI="projects/${PROJECT_ID}/locations/${REGION}/keyRings/${KMS_KEYRING}/cryptoKeys/${KMS_KEY}"
export CLOUDSQL_CONNECTION="${PROJECT_ID}:${REGION}:${CLOUDSQL_INSTANCE}"

# ----- BigQuery Configuration -----
export BIGLAKE_CONNECTION="delta_biglake_connection"   # BigLake connection name

# ----- Validation -----
if [ -z "$PROJECT_NUMBER" ]; then
    echo "⚠️  Warning: Could not retrieve PROJECT_NUMBER. Make sure you're authenticated:"
    echo "   gcloud auth login"
    echo "   gcloud config set project ${PROJECT_ID}"
else
    echo "✅ Environment configured:"
    echo "   Project: ${PROJECT_ID}"
    echo "   Region: ${REGION}"
    echo "   Bucket: ${BUCKET_NAME}"
    echo ""
    echo "Ready to run setup scripts!"
fi