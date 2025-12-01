from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Bronze Quarantine Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")    
    # 1. Silver Transactions Table Schema (Enforcing NOT NULL on transaction_id)
TRANSACTIONS_DDL = """
(
    transaction_id STRING NOT NULL,
    customer_id STRING,
    transaction_timestamp TIMESTAMP,
    merchant_id STRING,
    merchant_name STRING,
    product_category STRING,
    product_name STRING,
    amount DOUBLE,
    fee_amount DOUBLE,
    cashback_amount DOUBLE,
    loyalty_points BIGINT,
    payment_method STRING,
    transaction_status STRING,
    device_type STRING,
    location_type STRING,
    currency STRING,
    updated_at TIMESTAMP,
    _change_type STRING,
    _delta_version BIGINT
)
"""

# 2. Silver Job Control Table Schema (Enforcing NOT NULL on job execution fields)
JOB_CONTROL_DDL = """
(
    job_name STRING NOT NULL,
    layer STRING NOT NULL,
    batch_id STRING NOT NULL,
    run_mode STRING NOT NULL,
    status STRING NOT NULL,
    processing_date DATE,
    start_date DATE,
    end_date DATE,
    last_processed_timestamp TIMESTAMP,
    last_processed_batch_id STRING,
    records_read BIGINT,
    records_written BIGINT,
    records_failed BIGINT,
    records_quarantined BIGINT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds BIGINT,
    retry_count BIGINT,
    max_retries BIGINT,
    error_message STRING,
    triggered_by STRING,
    dataproc_cluster STRING,
    spark_app_id STRING
)
"""

# 3. Silver Quarantine Table Schema (Enforcing NOT NULL on error tracking fields)
QUARANTINE_DDL = """
(
    transaction_id STRING,
    customer_id STRING,
    transaction_timestamp TIMESTAMP,
    merchant_id STRING,
    merchant_name STRING,
    product_category STRING,
    product_name STRING,
    amount DOUBLE,
    fee_amount DOUBLE,
    cashback_amount DOUBLE,
    loyalty_points BIGINT,
    payment_method STRING,
    transaction_status STRING,
    device_type STRING,
    location_type STRING,
    currency STRING,
    updated_at TIMESTAMP,
    _change_type STRING,
    _delta_version BIGINT,
    error_reason STRING NOT NULL,
    error_tier STRING,
    quarantined_at TIMESTAMP NOT NULL,
    source_file STRING,
    processing_batch_id STRING
)
"""

# Define all 3 silver tables for creation
tables = [
    ("gs://delta-lake-payment-gateway-476820/silver/transactions", "silver.transactions", TRANSACTIONS_DDL),
    ("gs://delta-lake-payment-gateway-476820/metadata/silver_job_control", "silver.job_control", JOB_CONTROL_DDL),
    ("gs://delta-lake-payment-gateway-476820/quarantine/silver_transactions", "silver.quarantine", QUARANTINE_DDL)
]

print("Starting creation of Silver layer Delta tables...")

for delta_path, table_name, schema_ddl in tables:
    
    # --- Explicit GCS Directory Deletion ---
    uri = spark._jvm.java.net.URI(delta_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(delta_path)

    if fs.exists(path):
        print(f"Deleting old Delta directory for {table_name} at {delta_path}...")
        fs.delete(path, True)
    else:
        print(f"Delta directory for {table_name} does not exist, skipping delete.")
    # ---------------------------------------

    # 1. Drop table to ensure clean metastore entry
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    # 2. Use CREATE TABLE DDL to define the schema and enforce NOT NULL constraints
    print(f"\nCreating table: {table_name} at {delta_path}")
    spark.sql(f"""
    CREATE TABLE {table_name}
    {schema_ddl}
    USING DELTA
    LOCATION '{delta_path}'
    """)

    print(f"✅ Created: {table_name}")

print("\n✅ All 3 silver tables created successfully")

# --- Validation ---
print("\n--- Validating All Created Tables ---")
for delta_path, table_name, _ in tables:
    print(f"\n--- Validation for {table_name} ---")

    # Validations
    spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`").show(truncate=False)
    spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").show(truncate=False)
    
    # Validate: Show schema
    print("\n📊 Schema:")
    spark.read.format("delta").load(delta_path).printSchema()

spark.sql("SHOW TABLES IN silver").show()

spark.stop()