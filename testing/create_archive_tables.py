from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Archive Watermark Tables") \
    .getOrCreate()

# --- Spark SQL DDL Definition for Archive Tables ---
# Using DDL ensures NOT NULL constraints are explicitly defined in the Delta metadata,
# even when bootstrapping with an empty dataset.
ARCHIVE_DDL = """
(
    -- Job identification (MANDATORY FIELDS)
    job_name STRING NOT NULL,
    layer STRING NOT NULL,
    
    -- Execution tracking (MANDATORY FIELDS)
    batch_id STRING NOT NULL,
    run_mode STRING NOT NULL,
    status STRING NOT NULL,
    
    -- Date range tracking
    processing_date DATE,
    start_date DATE,
    end_date DATE,
    
    -- Watermark (incremental only)
    last_processed_timestamp TIMESTAMP,
    last_processed_batch_id STRING,
    
    -- Progress tracking
    records_read BIGINT,
    records_written BIGINT,
    records_failed BIGINT,
    records_quarantined BIGINT,
    
    -- Timing
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds BIGINT,
    
    -- Error handling
    retry_count BIGINT,
    max_retries BIGINT,
    error_message STRING,
    
    -- Metadata
    triggered_by STRING,
    dataproc_cluster STRING,
    spark_app_id STRING
)
"""

# Define all 3 archive tables
archives = [
    ("gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive", "bronze.job_control_archive"),
    ("gs://delta-lake-payment-gateway-476820/metadata/silver_job_control_archive", "silver.job_control_archive"),
    ("gs://delta-lake-payment-gateway-476820/metadata/gold_job_control_archive", "gold.job_control_archive")
]

print("Starting creation of Archive Job Control Tables using DDL...")

for delta_path, table_name in archives:
    # 1. Clean up old table registration if it exists
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # 2. Create the Delta table using DDL, which enforces NOT NULL constraints
    print(f"\nCreating table: {table_name} at {delta_path}")
    spark.sql(f"""
    CREATE TABLE {table_name}
    {ARCHIVE_DDL}
    USING DELTA
    LOCATION '{delta_path}'
    """)
    
    print(f"✅ Created: {table_name}")

print("\n✅ All 3 archive tables created successfully")

# Validate schema (This reads the DDL-enforced schema directly from the Delta path)
print("\n📊 Archive table schema (all 3 have same schema and NOT NULL constraints):")
spark.read.format("delta") \
    .load("gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive") \
    .printSchema()

spark.stop()