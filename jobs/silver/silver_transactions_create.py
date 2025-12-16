from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Silver Transactions Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure the 'silver' schema (database) exists
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Delta table location and name
delta_path = "gs://delta-lake-payment-gateway-476820/silver/transactions"
table_name = "silver.transactions"

# 1. Silver Transactions Table DDL (FIXED: Renamed change/version columns for BigQuery)
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
    delta_change_type STRING, -- Renamed from _change_type
    delta_version BIGINT,      -- Renamed from _delta_version
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP
)
"""

print("Starting creation of Silver Transactions table...")

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
{TRANSACTIONS_DDL}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"✅ Created: {table_name}")

# --- Validation ---
print("\n--- Validating Created Table ---")
spark.read.format("delta").load(delta_path).printSchema()

spark.stop()