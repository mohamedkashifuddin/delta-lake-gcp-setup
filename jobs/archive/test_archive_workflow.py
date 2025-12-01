from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
from delta.tables import DeltaTable

# Recreate the schema from the DDL in bronze_watermark_create.py to ensure type compatibility
JOB_CONTROL_SCHEMA = StructType([
    StructField("job_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("run_mode", StringType(), False),
    StructField("status", StringType(), False),
    StructField("processing_date", DateType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("last_processed_timestamp", TimestampType(), True),
    StructField("last_processed_batch_id", StringType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("records_failed", LongType(), True),
    StructField("records_quarantined", LongType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("completed_at", TimestampType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("retry_count", LongType(), True),
    StructField("max_retries", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("triggered_by", StringType(), True),
    StructField("dataproc_cluster", StringType(), True),
    StructField("spark_app_id", StringType(), True)
])

spark = SparkSession.builder \
    .appName("Test Archive Workflow") \
    .getOrCreate()

# --- Configuration ---
LIVE_TABLE_PATH = "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control"
ARCHIVE_TABLE_PATH = "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive"
RETENTION_DAYS = 90
# ---------------------

# Simulate: Insert old records into live bronze watermark (older than 90 days)
old_records = [
    (
        "bronze_load", # job_name
        "bronze", # layer
        "batch_old_001", # batch_id
        "incremental", # run_mode
        "completed",  # status
        (datetime.now() - timedelta(days=95)).date(), # processing_date (95 days old)
        None,# start_date
        None, # end_date
        datetime.now() - timedelta(days=95), # last_processed_timestamp
        None, # last_processed_batch_id
        1000, # records_read
        1000, # records_written
        0,  # records_failed
        0,  # records_quarantined
        datetime.now() - timedelta(days=95, hours=2), # started_at
        datetime.now() - timedelta(days=95, hours=1), # completed_at
        3600, # duration_seconds
        0, # retry_count
        3,  # max_retries
        None, # error_message
        "scheduler",  # triggered_by
        "dataproc-delta-cluster-final", # dataproc_cluster
        "app_12345" # spark_app_id
    ),
    (
        "bronze_load",
        "bronze",
        "batch_old_002",
        "incremental",
        "completed",
        (datetime.now() - timedelta(days=100)).date(),
        None,
        None,
        datetime.now() - timedelta(days=100),
        None,
        1500,
        1500,
        0,
        0,
        datetime.now() - timedelta(days=100, hours=2),
        datetime.now() - timedelta(days=100, hours=1),
        3700,
        0,
        3,
        None,
        "scheduler",
        "dataproc-delta-cluster-final",
        "app_12346"
    ),
    # Add one current record that should NOT be archived
    (
        "bronze_load",
        "bronze",
        "batch_current_001",
        "incremental",
        "completed",
        (datetime.now()).date(),
        None,
        None,
        datetime.now(),
        None,
        500,
        500,
        0,
        0,
        datetime.now() - timedelta(minutes=5),
        datetime.now(),
        300,
        0,
        3,
        None,
        "scheduler",
        "dataproc-delta-cluster-final",
        "app_55555"
    )
]

# Use the column names from the schema for DataFrame creation
columns = [field.name for field in JOB_CONTROL_SCHEMA]

# Create DataFrame with explicit schema to enforce correct types
old_df = spark.createDataFrame(old_records, JOB_CONTROL_SCHEMA)

# Write test data to the live table
old_df.write.format("delta") \
    .mode("append") \
    .save(LIVE_TABLE_PATH)

print("✅ Inserted 3 records (2 old, 1 current) into live bronze watermark")

# Step 1: Read records older than 90 days from live table
print(f"\n📊 Step 1: Finding records older than {RETENTION_DAYS} days...")
cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)

# Read from the live path
live_df = spark.read.format("delta").load(LIVE_TABLE_PATH)

# Filter based on the 'started_at' timestamp
old_records_df = live_df.filter(f"started_at < '{cutoff_date}'")

old_count = old_records_df.count()
print(f"Found {old_count} old records to archive (Expected: 2)")

# Step 2: Insert into archive table
print("\n📊 Step 2: Moving records to archive...")

old_records_df.write.format("delta") \
    .mode("append") \
    .save(ARCHIVE_TABLE_PATH)

print(f"✅ {old_count} records archived into {ARCHIVE_TABLE_PATH.split('/')[-1]}")

# Step 3: Delete from live table
print("\n📊 Step 3: Deleting archived records from live table...")
live_table = DeltaTable.forPath(spark, LIVE_TABLE_PATH)

# Use the same filter for deletion as for selection
live_table.delete(f"started_at < '{cutoff_date}'")

print("✅ Old records deleted from live table")

# Step 4: Verify
print("\n📊 Step 4: Verification...")

live_count = spark.read.format("delta").load(LIVE_TABLE_PATH).count()
archive_count = spark.read.format("delta").load(ARCHIVE_TABLE_PATH).count()

print(f"Live table rows: {live_count} (expected: 1)")
print(f"Archive table rows: {archive_count} (expected: 2)")

# Show archived data for confirmation
print("\n📊 Archived records (should only show batch_old_001 and batch_old_002):")
spark.read.format("delta").load(ARCHIVE_TABLE_PATH) \
    .select("job_name", "batch_id", "started_at", "records_written") \
    .show(truncate=False)

# Show remaining live data for confirmation
print("\n📊 Live records (should only show batch_current_001):")
spark.read.format("delta").load(LIVE_TABLE_PATH) \
    .select("job_name", "batch_id", "started_at", "records_written") \
    .show(truncate=False)

spark.stop()