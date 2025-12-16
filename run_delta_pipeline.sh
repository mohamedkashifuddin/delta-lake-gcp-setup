#!/bin/bash

# --- Configuration ---
CLUSTER_NAME="dataproc-delta-cluster-final"
REGION="us-central1"
# The base directory where all folders (bronze, silver, gold, etc.) reside
BASE_DIR="." 
LOG_DIR="${BASE_DIR}/logs"

# Create log directory if it doesn't exist
mkdir -p "${LOG_DIR}"
echo "--- Starting Sequential Delta Pipeline Execution ---"
echo "Log files will be stored in: ${LOG_DIR}"
echo "Dataproc Cluster: ${CLUSTER_NAME} (${REGION})"

# Function to submit a PySpark job, wait for it to complete, and log the output.
# The script will exit if the job fails.
submit_pyspark_job() {
    local SCRIPT_PATH=$1
    # Create a log file name from the script name
    local LOG_FILE="${LOG_DIR}/$(basename "${SCRIPT_PATH}" | sed 's/\.py/\.log/')"
    
    echo ""
    echo "=================================================="
    echo "Submitting Spark Job (Synchronous): ${SCRIPT_PATH}"
    echo "This script will wait for the job to complete on the cluster..."
    echo "Logging output to: ${LOG_FILE}"
    
    # Check if the script file exists
    if [ ! -f "${BASE_DIR}/${SCRIPT_PATH}" ]; then
        echo "Error: Script not found at ${BASE_DIR}/${SCRIPT_PATH}" | tee -a "${LOG_FILE}"
        return 1
    fi

    # Submit the job WITHOUT --async flag. This command will block 
    # until the Dataproc job finishes (success or failure).
    gcloud dataproc jobs submit pyspark "${BASE_DIR}/${SCRIPT_PATH}" \
        --cluster="${CLUSTER_NAME}" \
        --region="${REGION}" \
        2>&1 | tee "${LOG_FILE}" 
        
    local EXIT_CODE=${PIPESTATUS[0]} # Get exit code from gcloud
    
    if [ ${EXIT_CODE} -eq 0 ]; then
        echo "Successfully Completed ${SCRIPT_PATH}. Proceeding to next job."
    else
        echo "FATAL ERROR: Job ${SCRIPT_PATH} failed with exit code: ${EXIT_CODE}" | tee -a "${LOG_FILE}"
        echo "Stopping the sequential pipeline."
        # Exit the entire shell script immediately upon failure
        exit 1 
    fi
}

# --------------------------------------------------
# --- 1. Bronze Layer (Sequential) ---
# --------------------------------------------------

echo "### Starting Bronze Layer ###"
submit_pyspark_job "bronze/bronze_transactions_create.py"
submit_pyspark_job "bronze/bronze_transactions_staging.py"
submit_pyspark_job "bronze/bronze_quarantine_create.py"
submit_pyspark_job "bronze/bronze_watermark_create.py"


# --------------------------------------------------
# --- 2. Silver Layer (Sequential) ---
# --------------------------------------------------

echo "### Starting Silver Layer ###"
submit_pyspark_job "silver/silver_transactions_create.py"
submit_pyspark_job "silver/silver_transactions_staging.py"
submit_pyspark_job "silver/silver_quarantine_create.py"
submit_pyspark_job "silver/silver_watermark_create.py"
submit_pyspark_job "silver/validate_bronze_silver.py"



# --------------------------------------------------
# --- 3. Gold Layer (Sequential) ---
# --------------------------------------------------

echo "### Starting Gold Layer - Fact and Dim ###"
# Gold Fact
submit_pyspark_job "gold/fact/gold_fact_transactions_create.py"
# Gold fact stg 
submit_pyspark_job "gold/fact/gold_fact_transactions_staging.py"
# Gold Dimension
submit_pyspark_job "gold/dim/gold_dimensions_create.py"
# Gold Dimension customer 
submit_pyspark_job "gold/dim/gold_dim_customer_staging.py"
# Gold Dimension merchant
submit_pyspark_job "gold/dim/gold_dim_merchant_staging.py"
# Gold Watermark
submit_pyspark_job "gold/gold_watermark_create.py"


# --------------------------------------------------
# --- 4. Final Validation & Archive Scripts (Sequential) ---
# --------------------------------------------------

echo "### Starting Testing Data Archive ###"
# 4.1 Run the archive script
submit_pyspark_job "testing_data_achive_tables/create_archive_tables.py"

echo "### Starting Final Pipeline Validation ###"
# 4.2 Run the main validation script
submit_pyspark_job "validate_all_tables.py"


# --------------------------------------------------
# --- 5. BigQuery Verification (Shell Script) ---
# --------------------------------------------------

echo "### Starting BigQuery Verification Script ###"
BQ_SCRIPT="documentation/verify_bigquery_tables.sh"
BQ_LOG_FILE="${LOG_DIR}/verify_bigquery_tables.log"

if [ -f "${BASE_DIR}/${BQ_SCRIPT}" ]; then
    echo "Executing ${BQ_SCRIPT}..."
    # Run the shell script and pipe output to a log file
    bash "${BASE_DIR}/${BQ_SCRIPT}" 2>&1 | tee "${BQ_LOG_FILE}"
    
    if [ $? -eq 0 ]; then
        echo "Successfully executed ${BQ_SCRIPT}."
    else
        echo "Warning: ${BQ_SCRIPT} finished with errors. Check ${BQ_LOG_FILE}"
    fi
else
    echo "Error: BigQuery verification script not found at ${BASE_DIR}/${BQ_SCRIPT}"
fi

echo ""
echo "--- Sequential Delta Pipeline Execution Complete ---"
echo "All steps executed successfully. Check the log files in the **${LOG_DIR}** folder."