from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Gold Merchant Dimension Staging Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Delta table location and name
delta_path = "gs://delta-lake-payment-gateway-476820/gold/dim_merchant_staging"
table_name = "gold.dim_merchant_staging"

# Schema includes fields necessary for SCD Type 2 logic (Keys/History tracking)
MERCHANT_DIM_DDL = """
(
    merchant_id STRING NOT NULL,
    merchant_key BIGINT,
    merchant_name STRING,
    business_type STRING,
    region STRING,
    service_tier STRING,
    is_active BOOLEAN,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    updated_at TIMESTAMP
)
"""

print(f"Creating dim staging table: {table_name}...")

# --- Explicit GCS Directory Deletion ---
uri = spark._jvm.java.net.URI(delta_path)
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(delta_path)

if fs.exists(path):
    fs.delete(path, True)
# ---------------------------------------

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

spark.sql(f"""
CREATE TABLE {table_name}
{MERCHANT_DIM_DDL}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"✅ Created: {table_name}")

spark.stop()