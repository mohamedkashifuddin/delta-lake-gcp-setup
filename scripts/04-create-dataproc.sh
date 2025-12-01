#!/bin/bash
# =============================================================================
# Script: Create Dataproc Cluster with Delta Lake
# Description: Creates Dataproc 2.2 cluster with Cloud SQL metastore integration
# =============================================================================

set -euo pipefail

# Source environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../configs/environment.sh"

echo "========================================="
echo "Creating Dataproc Cluster with Delta Lake"
echo "========================================="

# Check if cluster already exists
if gcloud dataproc clusters describe ${CLUSTER_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} &>/dev/null; then
    echo "⚠️  Cluster '${CLUSTER_NAME}' already exists"
    echo "Delete it first if you want to recreate:"
    echo "   gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet"
    exit 1
fi

echo "Cluster: ${CLUSTER_NAME}"
echo "Region: ${REGION}"
echo "Zone: ${ZONE}"
echo "Image: ${DATAPROC_IMAGE}"
echo "Workers: ${NUM_WORKERS}"
echo ""

# Determine if single-node or multi-node
if [ "${NUM_WORKERS}" -eq "0" ]; then
    WORKER_CONFIG="--single-node"
    echo "Mode: Single-node (no workers)"
else
    WORKER_CONFIG="--num-workers ${NUM_WORKERS} --worker-machine-type ${WORKER_MACHINE_TYPE} --worker-boot-disk-type hyperdisk-balanced --worker-boot-disk-size ${WORKER_DISK_SIZE}"
    echo "Mode: Multi-node (${NUM_WORKERS} workers)"
fi

echo ""
echo "Creating cluster (this takes 8-10 minutes)..."
echo ""

# Create Dataproc cluster
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --zone ${ZONE} \
    --enable-component-gateway \
    --master-machine-type ${MASTER_MACHINE_TYPE} \
    --master-boot-disk-type hyperdisk-balanced \
    --master-boot-disk-size ${MASTER_DISK_SIZE} \
    ${WORKER_CONFIG} \
    --image-version ${DATAPROC_IMAGE} \
    --optional-components JUPYTER,DELTA \
    --properties hive:hive.metastore.warehouse.dir=gs://${BUCKET_NAME}/warehouse \
    --scopes https://www.googleapis.com/auth/cloud-platform \
    --metadata hive-metastore-instance=${CLOUDSQL_CONNECTION} \
    --metadata kms-key-uri=${KMS_KEY_URI} \
    --metadata db-hive-password-uri=gs://${BUCKET_NAME}/secrets/hive-password.encrypted \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
    --project ${PROJECT_ID}

echo ""
echo "✅ Dataproc cluster created successfully"
echo ""
echo "Cluster details:"
gcloud dataproc clusters describe ${CLUSTER_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --format="table(clusterName,config.gceClusterConfig.zoneUri,status.state)"

echo ""
echo "SSH command:"
echo "  gcloud compute ssh ${CLUSTER_NAME}-m --zone=${ZONE} --project=${PROJECT_ID}"
echo ""
echo "Component Gateway (Jupyter):"
gcloud dataproc clusters describe ${CLUSTER_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --format="value(config.endpointConfig.httpPorts)"

echo ""
echo "⚠️  CRITICAL: This cluster costs ~$0.40/hour"
echo "   Delete when not using: gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet"
echo ""
echo "Next step: Run jobs from jobs/ folder to create Delta tables"
echo "Then: Run 05-create-bigquery-external.sh"