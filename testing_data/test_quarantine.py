from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType, BooleanType # Changed IntegerType to LongType

# Initialize Spark Session (with Delta extensions)
spark = SparkSession.builder \
    .appName("Test Quarantine") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create bad record (NULL transaction_id = Tier 1 failure)
bad_data = [(
    None,                                            # transaction_id (NULL - critical error!)
    "CUST004",                                       # customer_id
    datetime(2024, 11, 28, 13, 0, 0),                # transaction_timestamp
    "MERCH004",                                      # merchant_id
    "Quarantine Merchant",                           # merchant_name
    "Retail",                                        # product_category
    "Shoes",                                         # product_name
    75.00,                                           # amount
    3.75,                                            # fee_amount
    0.00,                                            # cashback_amount
    15,                                              # loyalty_points (Python int)
    "PayPal",                                        # payment_method
    "Failed",                                        # transaction_status
    "Mobile",                                        # device_type
    "Online",                                        # location_type
    "USD",                                           # currency
    datetime(2024, 11, 28, 13, 5, 0),                # updated_at
    "INSERT",                                        # delta_change_type
    1,                                               # delta_version (Python int)
    "NULL_PRIMARY_KEY",                              # error_reason
    "TIER_1",                                        # error_tier
    datetime(2024, 11, 28, 13, 10, 0),               # quarantined_at
    "raw_transactions_20241128.csv",                 # source_file
    "batch_uuid_12345"                               # processing_batch_id
)]

# Define the explicit schema for the quarantine table to ensure correct data types
quarantine_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("cashback_amount", DoubleType(), True),
    StructField("loyalty_points", LongType(), True),       # FIXED: Changed from IntegerType to LongType
    StructField("payment_method", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("delta_change_type", StringType(), True),
    StructField("delta_version", LongType(), True),        # FIXED: Changed from IntegerType to LongType
    StructField("error_reason", StringType(), True),
    StructField("error_tier", StringType(), True),
    StructField("quarantined_at", TimestampType(), True),
    StructField("source_file", StringType(), True),
    StructField("processing_batch_id", StringType(), True),
])

bad_df = spark.createDataFrame(bad_data, quarantine_schema)

# Write to QUARANTINE table (NOT bronze.transactions)
quarantine_path = "gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions"

print(f"--- Writing bad record to quarantine path: {quarantine_path} ---")

bad_df.write.format("delta") \
    .mode("append") \
    .save(quarantine_path)

print("✅ Bad record sent to quarantine")

# Verify quarantine table
print("\n📊 Quarantine data:")
spark.read.format("delta").load(quarantine_path) \
    .select("transaction_id", "customer_id", "amount", "error_reason", "error_tier", "quarantined_at") \
    .show(truncate=False)

# Verify main bronze table unchanged
bronze_path = "gs://delta-lake-payment-gateway-476820/bronze/transactions"
bronze_count = spark.read.format("delta").load(bronze_path).count()
print(f"\n📊 Bronze table rows: {bronze_count} (expected: 2, should be unchanged)")

# Quarantine count
quarantine_count = spark.read.format("delta").load(quarantine_path).count()
print(f"Quarantine table rows: {quarantine_count} (expected: 1)")

spark.stop()