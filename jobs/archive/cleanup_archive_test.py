from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder \
    .appName("Cleanup Archive Test Data") \
    .getOrCreate()

archive_path = "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive"

print(f"--- Starting Cleanup for: {archive_path.split('/')[-1]} ---")

try:
    # Get row count before deletion
    before_count = spark.read.format("delta").load(archive_path).count()
    
    # Delete all test records: MUST use condition="1=1" for TRUNCATE equivalent
    delta_table = DeltaTable.forPath(spark, archive_path)
    delta_table.delete(condition="1=1")
    
    print(f"✅ Archive test data cleaned up. ({before_count} rows deleted)")

except AnalysisException:
    print(f"⚠️ Warning: Delta table not found at {archive_path}. Skipping cleanup.")
    
except Exception as e:
    print(f"❌ Error during cleanup: {e}")

# Verify
try:
    count = spark.read.format("delta").load(archive_path).count()
    print(f"Archive table rows: {count} (expected: 0)")
    if count == 0:
        print("✅ Verification successful.")
    else:
        print("❌ Verification failed: Table is not empty.")
except AnalysisException:
    # If the table wasn't created yet, the verification will fail, which is okay for cleanup.
    pass


spark.stop()