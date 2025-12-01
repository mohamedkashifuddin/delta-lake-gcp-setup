from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Bronze Quarantine Table (Final Fix)") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure the 'bronze' schema (database) exists
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Delta table location
delta_path = "gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions"
table_name = "bronze.quarantine"

# --- DDL Schema Definition for Bronze Quarantine (FIXED: Renamed change/version columns for BigQuery) ---
BRONZE_QUARANTINE_DDL = """
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
    delta_change_type STRING, -- RENAMED from _change_type
    delta_version BIGINT,     -- RENAMED from _delta_version
    error_reason STRING NOT NULL,
    error_tier STRING,
    quarantined_at TIMESTAMP NOT NULL,
    source_file STRING,
    processing_batch_id STRING
)
"""

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
{BRONZE_QUARANTINE_DDL}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"✅ Bronze quarantine table created at: {delta_path}")

# --- Validation ---
print("\n--- Validating Table Creation ---")
spark.read.format("delta").load(delta_path).printSchema()

spark.stop()