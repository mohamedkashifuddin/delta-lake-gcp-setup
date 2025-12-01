from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, datediff, col, current_timestamp, current_date
from delta.tables import DeltaTable
from pyspark.sql.types import TimestampType, BooleanType
from datetime import datetime, timedelta

# Initialize Spark Session (with Delta extensions)
spark = SparkSession.builder \
    .appName("Test Late Arrival") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define test data (1 row with original 19 columns - using corrected delta column names)
late_data = [(
    "TEST002",                                       # transaction_id
    "CUST002",                                       # customer_id
    datetime(2024, 11, 25, 9, 0, 0),                 # transaction_timestamp (3 days ago)
    "MERCH002",                                      # merchant_id
    "Late Merchant",                                 # merchant_name
    "Food",                                          # product_category
    "Pizza",                                         # product_name
    50.00,                                           # amount
    2.50,                                            # fee_amount
    0.00,                                            # cashback_amount
    10,                                              # loyalty_points
    "Debit Card",                                    # payment_method
    "Completed",                                     # transaction_status
    "Desktop",                                       # device_type
    "In-store",                                      # location_type
    "USD",                                           # currency
    datetime(2024, 11, 28, 14, 0, 0),                # updated_at (today)
    "INSERT",                                        # delta_change_type (FIXED)
    2                                                # delta_version (FIXED - this is the 3rd operation, but we'll use 2 for simplicity)
)]

columns = [
    "transaction_id", "customer_id", "transaction_timestamp", "merchant_id",
    "merchant_name", "product_category", "product_name", "amount",
    "fee_amount", "cashback_amount", "loyalty_points", "payment_method",
    "transaction_status", "device_type", "location_type", "currency",
    "updated_at", "delta_change_type", "delta_version" # FIXED column names
]

late_df = spark.createDataFrame(late_data, columns)

# --- CRITICAL FIX 1: Align with existing Bronze Schema (Add soft delete columns) ---
late_df = late_df \
    .withColumn("is_deleted", lit(False).cast(BooleanType())) \
    .withColumn("deleted_at", lit(None).cast(TimestampType()))

# --- CRITICAL FIX 2: Add late arrival columns and calculate delay ---
late_df = late_df.withColumn("is_late_arrival", lit(True).cast(BooleanType())) \
    .withColumn("original_event_date", col("transaction_timestamp").cast("date")) \
    .withColumn("arrival_delay_hours", datediff(current_date(), col("original_event_date")) * 24)
    # Note: Using datediff * 24 provides a simple hours estimate for testing

# Append to bronze with schema evolution
delta_path = "gs://delta-lake-payment-gateway-476820/bronze/transactions"

print(f"--- Inserting late arrival row and evolving schema at {delta_path} ---")

late_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(delta_path)

print("✅ Late arrival row inserted (Schema evolved successfully)")

# Verify late arrival flags
print("\n📊 Late arrival data (TEST002):")
spark.read.format("delta").load(delta_path) \
    .filter("transaction_id = 'TEST002'") \
    .select("transaction_id", "transaction_timestamp", "is_late_arrival", "arrival_delay_hours", "original_event_date") \
    .show(truncate=False)

# Count total rows (expected 2: TEST001 (deleted) and TEST002 (late))
count = spark.read.format("delta").load(delta_path).count()
print(f"\n📊 Total rows: {count} (expected: 2)")

spark.stop()