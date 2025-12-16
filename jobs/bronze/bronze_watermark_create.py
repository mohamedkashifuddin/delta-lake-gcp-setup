from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Bronze Watermark Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure the 'bronze' schema (database) exists
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Delta table location and name
delta_path = "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control"
table_name = "bronze.job_control"

# 2. Bronze Job Control Table DDL
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

print("Starting creation of Bronze Job Control table (Watermark)...")

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

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

print(f"\nCreating table: {table_name} at {delta_path}")
spark.sql(f"""
CREATE TABLE {table_name}
{JOB_CONTROL_DDL}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"✅ Created: {table_name}")

# --- Validation ---
print("\n--- Validating Created Table ---")
spark.read.format("delta").load(delta_path).printSchema()
spark.sql("SHOW TABLES IN bronze").show()
spark.stop()