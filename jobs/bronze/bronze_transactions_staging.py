from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Bronze Transactions Staging Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure the 'bronze' schema (database) exists
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Delta table location and name
delta_path = "gs://delta-lake-payment-gateway-476820/bronze/transactions_staging"
table_name = "bronze.transactions_staging"

# Schema is identical to bronze.transactions
STAGING_DDL = """
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
    delta_change_type STRING, 
    delta_version BIGINT,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP,
    is_late_arrival BOOLEAN,
    arrival_delay_hours INT      
)
"""

print(f"Starting creation of staging table: {table_name}...")

# --- Explicit GCS Directory Deletion (Restored as requested) ---
uri = spark._jvm.java.net.URI(delta_path)
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(delta_path)

if fs.exists(path):
    print(f"Deleting old Delta directory for {table_name} at {delta_path}...")
    fs.delete(path, True)
else:
    print(f"Delta directory for {table_name} does not exist, skipping delete.")
# -------------------------------------------------------------

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

spark.sql(f"""
CREATE TABLE {table_name}
{STAGING_DDL}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"✅ Created: {table_name} at {delta_path}")

# --- Detailed Validation (Restored/Added as requested) ---
print("\n--- Validating Created Table Schema ---")
try:
    spark.read.format("delta").load(delta_path).printSchema()
except Exception as e:
    print(f"❌ Failed to read/validate schema for {table_name}: {e}")

# Request to list all tables after creation
print("\n--- Current Tables in Metastore (SHOW TABLES) ---")
spark.sql("SHOW TABLES IN bronze").show()

spark.stop()