from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = SparkSession.builder \
    .appName("Test Bronze Insert") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define test data (1 row with all 19 columns)
test_data = [(
    "TEST001",                                       # transaction_id
    "CUST001",                                       # customer_id
    datetime(2024, 11, 28, 10, 30, 0),               # transaction_timestamp
    "MERCH001",                                      # merchant_id
    "Test Merchant Inc",                             # merchant_name
    "Electronics",                                   # product_category
    "Laptop",                                        # product_name
    999.99,                                          # amount
    19.99,                                           # fee_amount
    10.00,                                           # cashback_amount
    100,                                             # loyalty_points
    "Credit Card",                                   # payment_method
    "Completed",                                     # transaction_status
    "Mobile",                                        # device_type
    "Online",                                        # location_type
    "USD",                                           # currency
    datetime(2024, 11, 28, 10, 35, 0),               # updated_at
    "INSERT",                                        # delta_change_type (FIXED)
    1                                                # delta_version (FIXED)
)]

# Column names (must match schema)
columns = [
    "transaction_id", "customer_id", "transaction_timestamp", "merchant_id",
    "merchant_name", "product_category", "product_name", "amount",
    "fee_amount", "cashback_amount", "loyalty_points", "payment_method",
    "transaction_status", "device_type", "location_type", "currency",
    "updated_at", "delta_change_type", "delta_version" # FIXED column names
]

# Create DataFrame
test_df = spark.createDataFrame(test_data, columns)

# Delta path
delta_path = "gs://delta-lake-payment-gateway-476820/bronze/transactions"

print(f"Attempting to insert test row into Delta path: {delta_path}")

# Append to bronze table
test_df.write.format("delta") \
    .mode("append") \
    .save(delta_path)

print("✅ Test row inserted into bronze.transactions")

# Verify: Read back
result_df = spark.read.format("delta").load(delta_path)

print(f"\n📊 Row count: {result_df.count()} (expected: 1)")

print("\n📊 Test data:")
result_df.show(truncate=False)

print("\n📊 Schema validation:")
result_df.printSchema()

# Verify specific columns (using FIXED column names)
print("\n🔍 Verify key columns:")
row = result_df.first()
print(f"transaction_id: {row['transaction_id']}")
print(f"amount: {row['amount']}")
print(f"delta_change_type: {row['delta_change_type']}") # FIXED
print(f"delta_version: {row['delta_version']}")         # FIXED

spark.stop()