from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Gold Fact Transactions Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure the 'gold' schema (database) exists
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Delta table location
delta_path = "gs://delta-lake-payment-gateway-476820/gold/fact_transactions"
table_name = "gold.fact_transactions"

# --- DDL Schema Definition for the Fact Transactions Table (FIXED: Renamed change/version columns) ---
FACT_TRANSACTIONS_DDL = """
(
    -- Surrogate keys (foreign keys to dimensions)
    customer_key BIGINT,
    merchant_key BIGINT,
    payment_method_key BIGINT,
    status_key BIGINT,
    date_key BIGINT,

    -- Degenerate dimensions (high cardinality, kept in fact)
    transaction_id STRING NOT NULL,
    product_category STRING,
    product_name STRING,
    device_type STRING,

    -- Measures (numeric values to aggregate)
    amount DOUBLE,
    fee_amount DOUBLE,
    cashback_amount DOUBLE,
    loyalty_points BIGINT,

    -- Calculated measures
    net_customer_amount DOUBLE,
    merchant_net_amount DOUBLE,
    gateway_revenue DOUBLE,

    -- Time dimensions
    transaction_timestamp TIMESTAMP,
    currency STRING,

    -- Flags
    is_refunded BOOLEAN,
    refund_amount DOUBLE,
    refund_date DATE,
    attempt_number BIGINT,

    -- Audit columns
    loaded_at TIMESTAMP,
    source_system STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    -- Delta/CDC columns (RENAMED to avoid BigQuery conflicts)
    delta_change_type STRING, -- RENAMED: was _change_type
    delta_version BIGINT,     -- RENAMED: was _delta_version

    -- Soft delete
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP
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
{FACT_TRANSACTIONS_DDL}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"✅ Gold fact_transactions table created at: {delta_path}")

# --- Validation ---
print("\n--- Validating Table Creation ---")
spark.read.format("delta").load(delta_path).printSchema()

spark.stop()