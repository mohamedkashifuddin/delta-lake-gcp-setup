from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Create Gold Dimension Tables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure the 'gold' schema (database) exists
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# --- DDL Schema Definitions for Gold Dimension tables ---

# 1. Dim Customer Table
DIM_CUSTOMER_DDL = """
(
    customer_key BIGINT NOT NULL,
    customer_id STRING NOT NULL,
    customer_tier STRING,
    is_active BOOLEAN,
    first_transaction_date DATE,
    last_transaction_date DATE,
    lifetime_value DOUBLE,
    loaded_at TIMESTAMP,
    source_system STRING,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
)
"""

# 2. Dim Merchant Table
DIM_MERCHANT_DDL = """
(
    merchant_key BIGINT NOT NULL,
    merchant_id STRING NOT NULL,
    merchant_name STRING,
    category STRING,
    location_type STRING,
    loaded_at TIMESTAMP,
    source_system STRING,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
)
"""

# 3. Dim Date Table
DIM_DATE_DDL = """
(
    date_key BIGINT NOT NULL,
    full_date DATE NOT NULL,
    day_of_month INT,
    day_name STRING,
    month_number INT,
    month_name STRING,
    year INT,
    quarter INT,
    day_of_week INT,
    day_of_year INT,
    is_weekend BOOLEAN
)
"""

# 4. Dim Payment Method Table
DIM_PAYMENT_METHOD_DDL = """
(
    payment_method_key BIGINT NOT NULL,
    payment_method STRING NOT NULL,
    description STRING,
    loaded_at TIMESTAMP,
    source_system STRING
)
"""

# 5. Dim Transaction Status Table
DIM_STATUS_DDL = """
(
    status_key BIGINT NOT NULL,
    transaction_status STRING NOT NULL,
    status_category STRING,
    is_successful BOOLEAN,
    loaded_at TIMESTAMP,
    source_system STRING
)
"""

# Define all Gold dimension tables for creation
tables = [
    ("gs://delta-lake-payment-gateway-476820/gold/dim_customer", "gold.dim_customer", DIM_CUSTOMER_DDL),
    ("gs://delta-lake-payment-gateway-476820/gold/dim_merchant", "gold.dim_merchant", DIM_MERCHANT_DDL),
    ("gs://delta-lake-payment-gateway-476820/gold/dim_date", "gold.dim_date", DIM_DATE_DDL),
    ("gs://delta-lake-payment-gateway-476820/gold/dim_payment_method", "gold.dim_payment_method", DIM_PAYMENT_METHOD_DDL),
    ("gs://delta-lake-payment-gateway-476820/gold/dim_status", "gold.dim_status", DIM_STATUS_DDL)
]

print("Starting creation of Gold Dimension layer Delta tables...")

for delta_path, table_name, schema_ddl in tables:
    
    # --- Explicit GCS Directory Deletion ---
    uri = spark._jvm.java.net.URI(delta_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(delta_path)

    if fs.exists(path):
        print(f"\nDeleting old Delta directory for {table_name} at {delta_path}...")
        fs.delete(path, True)
    else:
        print(f"\nDelta directory for {table_name} does not exist, skipping delete.")
    # ---------------------------------------

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    print(f"Creating table: {table_name} at {delta_path}")
    spark.sql(f"""
    CREATE TABLE {table_name}
    {schema_ddl}
    USING DELTA
    LOCATION '{delta_path}'
    """)

    print(f"✅ Created: {table_name}")

# --- Validation ---
print("\n--- Validating All Created Dimension Tables Schemas ---")
for delta_path, table_name, _ in tables:
    print(f"\n--- Validation for {table_name} ---")
    spark.read.format("delta").load(delta_path).printSchema()

print("\n✅ All Gold Dimension tables created and validated successfully.")

spark.stop()