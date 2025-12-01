from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable
from pyspark.sql.types import BooleanType, TimestampType
from datetime import datetime

spark = SparkSession.builder \
    .appName("Test Soft Delete") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "gs://delta-lake-payment-gateway-476820/bronze/transactions"

print("--- Ensuring schema includes 'is_deleted' and 'deleted_at' for soft delete ---")

existing_df = spark.read.format("delta").load(delta_path)

if "is_deleted" not in existing_df.columns:
    print("Schema missing 'is_deleted' and 'deleted_at'. Performing schema evolution...")
    
    soft_delete_ready_df = existing_df \
        .withColumn("is_deleted", lit(False).cast(BooleanType())) \
        .withColumn("deleted_at", lit(None).cast(TimestampType()))
    
    soft_delete_ready_df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(delta_path)
    
    print("Schema evolution complete. Columns added.")

delta_table = DeltaTable.forPath(spark, delta_path)

print("\nBefore soft delete (showing only relevant columns):")
spark.read.format("delta").load(delta_path) \
    .select("transaction_id", "is_deleted", "deleted_at") \
    .show(truncate=False)

print("--- Performing Soft Delete Update ---")
delta_table.update(
    condition="transaction_id = 'TEST001'",
    set={
        "is_deleted": "true",
        "deleted_at": "current_timestamp()",
        "delta_change_type": "'UPDATE'"
    }
)

print("✅ Soft delete performed")

print("\nAfter soft delete (showing updated status):")
result_df = spark.read.format("delta").load(delta_path) \
    .select("transaction_id", "is_deleted", "deleted_at", "delta_change_type")

result_df.show(truncate=False)

count = spark.read.format("delta").load(delta_path).count()
print(f"\n📊 Row count: {count} (expected: 1, row still exists)")

deleted_count = spark.read.format("delta").load(delta_path) \
    .filter(col("is_deleted") == True) \
    .count()
print(f"Deleted rows: {deleted_count} (expected: 1)")

print("\n📊 Delta History:")
delta_table.history().select("version", "operation", "operationParameters").show(truncate=False)

spark.stop()