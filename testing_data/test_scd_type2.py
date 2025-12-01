from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, date_sub, col
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, BooleanType, IntegerType, TimestampType

# Initialize Spark Session (with Delta extensions)
spark = SparkSession.builder \
    .appName("Test SCD Type 2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 1. Define Target Schema for dim_customers ---
customer_schema = StructType([
    StructField("customer_key", LongType(), False), # Surrogate Key
    StructField("customer_id", StringType(), False), # Business Key
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("customer_segment", StringType(), True), # SCD-triggering field
    StructField("registration_date", DateType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("effective_start_date", TimestampType(), False),
    StructField("effective_end_date", TimestampType(), False),
    StructField("is_current", BooleanType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

delta_path = "gs://delta-lake-payment-gateway-476820/gold/dim_customers"

# --- Setup: Clear table for clean test run (if it exists) ---
try:
    DeltaTable.forPath(spark, delta_path).delete("1=1")
    print("Setup: Cleared existing Delta table.")
except Exception as e:
    # If table doesn't exist, we create it later.
    pass

# --- STEP 1: Initial Insert (First-time customer) ---

# This is the incoming data (Source)
source_data_v1 = [(
    "C001",                                      # customer_id
    "John Doe",                                  # customer_name
    "john@example.com",
    "+1234567890",
    "USA",
    "New York",
    "Regular",                                   # customer_segment
    datetime(2024, 1, 1).date(),                 # registration_date
    True,
    50,
    datetime(2024, 11, 20, 10, 0, 0),            # updated_at (arrival time)
)]

source_cols = [
    "customer_id", "customer_name", "email", "phone", "country", "city",
    "customer_segment", "registration_date", "is_verified", "risk_score", "updated_at"
]

source_df_v1 = spark.createDataFrame(source_data_v1, source_cols)
# FIX: Explicitly cast columns to prevent Spark's default inference (e.g., LongType for int)
source_df_v1 = source_df_v1.withColumn("risk_score", col("risk_score").cast(IntegerType())) \
                           .withColumn("is_verified", col("is_verified").cast(BooleanType()))
                           

# Create the initial Gold table if it doesn't exist by appending the first record
source_df_v1.withColumn("customer_key", lit(1).cast(LongType())) \
    .withColumn("effective_start_date", col("updated_at")) \
    .withColumn("effective_end_date", lit(datetime(9999, 12, 31, 23, 59, 59)).cast(TimestampType())) \
    .withColumn("is_current", lit(True).cast(BooleanType())) \
    .withColumn("created_at", col("updated_at")) \
    .write.format("delta") \
    .mode("append") \
    .save(delta_path)

# Initialize DeltaTable object for subsequent MERGE operations
target_table = DeltaTable.forPath(spark, delta_path)

print("✅ Step 1: Initial version of C001 inserted (Regular, customer_key=1)")

# --- STEP 2: SCD Type 2 Update (Segment change) - TWO-STEP PROCESS ---

# Incoming data (Source) V2: Premium segment, new risk_score
source_data_v2 = [(
    "C001",                                      # customer_id (SAME)
    "John Doe",
    "john@example.com",
    "+1234567890",
    "USA",
    "New York",
    "Premium",                                   # customer_segment (CHANGED!)
    datetime(2024, 1, 1).date(),
    True,
    30,
    datetime(2024, 11, 28, 10, 0, 0),            # updated_at (New arrival time)
)]

source_df_v2 = spark.createDataFrame(source_data_v2, source_cols)
source_df_v2 = source_df_v2.withColumn("risk_score", col("risk_score").cast(IntegerType())) \
                           .withColumn("is_verified", col("is_verified").cast(BooleanType()))

# --- Extract necessary constants for the update (to avoid complex subqueries) ---
customer_id_to_close = source_data_v2[0][0] # 'C001'
v2_arrival_timestamp = source_data_v2[0][-1]
v2_arrival_timestamp_str = str(v2_arrival_timestamp)

print("\n--- Step 2a: Closing Old Record using UPDATE ---")

# 2a. Identify and CLOSE the existing current record (V1)
# Execute UPDATE to close the current record (V1) for C001 where data changed
# FIX: Use literal customer_id and remove 'target.' alias
target_table.update(
    condition=f"customer_id = '{customer_id_to_close}' AND is_current = true",
    set={
        # Set end date to one second before the new version's arrival time
        "effective_end_date": f"timestamp('{v2_arrival_timestamp_str}') - interval 1 second", 
        "is_current": "false",
        "updated_at": f"timestamp('{v2_arrival_timestamp_str}')"
    }
)

print("✅ Step 2a: Old record (Regular) closed.")

# 2b. INSERT the new record (V2)
# Determine the max existing customer_key and add 1 for the new key (Simulated surrogate key)
new_key = spark.read.format("delta").load(delta_path) \
    .select(col("customer_key").alias("key")) \
    .agg({"key": "max"}) \
    .collect()[0][0] + 1 if spark.read.format("delta").load(delta_path).count() > 0 else 1

source_df_v2_insert = source_df_v2 \
    .withColumn("customer_key", lit(new_key).cast(LongType())) \
    .withColumn("effective_start_date", col("updated_at")) \
    .withColumn("effective_end_date", lit(datetime(9999, 12, 31, 23, 59, 59)).cast(TimestampType())) \
    .withColumn("is_current", lit(True).cast(BooleanType())) \
    .withColumn("created_at", col("updated_at")) \
    .withColumnRenamed("updated_at", "updated_at_source") # Avoid conflict with target schema column

# Select columns in the correct order/format for target table
source_df_v2_final = source_df_v2_insert.select(
    "customer_key", 
    "customer_id", 
    "customer_name", 
    "email", 
    "phone", 
    "country", 
    "city", 
    "customer_segment", 
    "registration_date", 
    "is_verified", 
    "risk_score", 
    "effective_start_date", 
    "effective_end_date", 
    "is_current", 
    "created_at", 
    col("updated_at_source").alias("updated_at")
)


source_df_v2_final.write.format("delta") \
    .mode("append") \
    .save(delta_path)

print("✅ Step 2b: New record (Premium) inserted.")

# --- Verification ---

print("\n📊 All versions of customer C001 (Should see two rows: one closed, one current):")
spark.read.format("delta").load(delta_path) \
    .filter("customer_id = 'C001'") \
    .select("customer_key", "customer_id", "customer_segment", "effective_start_date", "effective_end_date", "is_current") \
    .orderBy("effective_start_date") \
    .show(truncate=False)

# Verify: Only 1 current version
current_count = spark.read.format("delta").load(delta_path) \
    .filter("customer_id = 'C001' AND is_current = true") \
    .count()

print(f"\n📊 Current versions: {current_count} (expected: 1)")

# Total rows (should be 2)
total_count = spark.read.format("delta").load(delta_path).count()
print(f"Total rows: {total_count} (expected: 2)")

spark.stop()