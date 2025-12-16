from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder \
    .appName("Validate All Data Lakehouse Tables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# List of all tables created across Bronze, Silver, and Gold layers
tables = [
    # Bronze Layer
    "bronze.transactions",
    "bronze.transactions_staging",
    "bronze.job_control",
    "bronze.quarantine",

    

    # Silver Layer
    "silver.transactions",
    "silver.transactions_staging",
    "silver.job_control",
    "silver.quarantine",


    # Gold Layer
    "gold.fact_transactions",
    "gold.fact_transactions_staging",
    "gold.dim_customer",
    "gold.dim_merchant",
    "gold.dim_date",
    "gold.dim_payment_method",
    "gold.dim_status",
    "gold.job_control",
    
    #job_control_archive tables
    "bronze.job_control_archive",
    "silver.job_control_archive",
    "gold.job_control_archive"
    
]

print("Starting validation of all Data Lakehouse tables...")
print("-" * 50)

for table in tables:
    try:
        # 1. Validate Table Existence and Schema
        df = spark.table(table)
        print(f"\n--- Validation for: {table} ---")
        df.printSchema()

        # 2. Validate Row Count (Should be 0 after DDL creation)
        count = df.count()
        
        # Check if the problematic columns are absent/correctly named
        schema_fields = [f.name for f in df.schema.fields]
        status = "✅ OK"
        if "_change_type" in schema_fields or "_delta_version" in schema_fields:
            status = "❌ ERROR: Found old reserved column names!"
        elif "delta_change_type" in schema_fields or "delta_version" in schema_fields:
            status = "✅ OK: BigQuery-compliant columns found."
        
        print(f"Schema Status: {status}")
        print(f"Row Count: {count} (expected: 0 after DDL setup)")

    except AnalysisException as e:
        print(f"\n--- Validation Failed for: {table} ---")
        print(f"❌ ERROR: Table does not exist in the metastore.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"\n--- Validation Failed for: {table} ---")
        print(f"❌ UNEXPECTED ERROR: {e}")

print("-" * 50)
print("Validation complete. Check the output above for any errors or missing tables.")

spark.stop()