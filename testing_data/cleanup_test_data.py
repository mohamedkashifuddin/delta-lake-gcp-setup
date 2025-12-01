from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# Initialize Spark Session (assuming Delta extensions are configured globally or in session builder)
spark = SparkSession.builder \
    .appName("Cleanup Test Data") \
    .getOrCreate()

# Tables to clean
tables = [
    "gs://delta-lake-payment-gateway-476820/bronze/transactions",
    "gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions",
    "gs://delta-lake-payment-gateway-476820/gold/dim_customers"
]

print("--- Starting Delta Lake Cleanup ---")

for delta_path in tables:
    try:
        # Check if the path exists and is a Delta table before trying to read/delete
        delta_table = DeltaTable.forPath(spark, delta_path)
        
        # Get row count before
        before_count = spark.read.format("delta").load(delta_path).count()
        
        # Delete all rows: MUST use condition="1=1" for TRUNCATE equivalent
        delta_table.delete(condition="1=1")
        
        # Verify
        after_count = spark.read.format("delta").load(delta_path).count()
        
        print(f"✅ Cleaned: {delta_path}")
        print(f"    Before: {before_count} rows, After: {after_count} rows")
        
    except AnalysisException as e:
        # This typically catches errors if the path does not contain a Delta table
        print(f"⚠️ Skipped: {delta_path} (Table not found or invalid Delta path)")
        # Optionally, you might check for specific error messages if needed

print("\n✅ All specified paths reviewed and cleaned.")

spark.stop()