from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Validate Complete Setup") \
    .getOrCreate()

print("=" * 80)
print("DELTA LAKE SETUP VALIDATION")
print("=" * 80)

# Define all expected Delta tables
expected_tables = [
    # Bronze (3)
    ("bronze.transactions", "gs://delta-lake-payment-gateway-476820/bronze/transactions"),
    ("bronze.job_control", "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control"),
    ("bronze.quarantine", "gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions"),
    
    # Silver (3)
    ("silver.transactions", "gs://delta-lake-payment-gateway-476820/silver/transactions"),
    ("silver.job_control", "gs://delta-lake-payment-gateway-476820/metadata/silver_job_control"),
    ("silver.quarantine", "gs://delta-lake-payment-gateway-476820/quarantine/silver_transactions"),
    
    # Gold (7)
    ("gold.fact_transactions", "gs://delta-lake-payment-gateway-476820/gold/fact_transactions"),
    ("gold.dim_customers", "gs://delta-lake-payment-gateway-476820/gold/dim_customers"),
    ("gold.dim_merchants", "gs://delta-lake-payment-gateway-476820/gold/dim_merchants"),
    ("gold.dim_payment_methods", "gs://delta-lake-payment-gateway-476820/gold/dim_payment_methods"),
    ("gold.dim_transaction_status", "gs://delta-lake-payment-gateway-476820/gold/dim_transaction_status"),
    ("gold.dim_date", "gs://delta-lake-payment-gateway-476820/gold/dim_date"),
    ("gold.job_control", "gs://delta-lake-payment-gateway-476820/metadata/gold_job_control"),
    
    # Archives (3)
    ("bronze.job_control_archive", "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive"),
    ("silver.job_control_archive", "gs://delta-lake-payment-gateway-476820/metadata/silver_job_control_archive"),
    ("gold.job_control_archive", "gs://delta-lake-payment-gateway-476820/metadata/gold_job_control_archive"),
]

passed = 0
failed = 0
total = len(expected_tables)

print(f"\nValidating {total} Delta tables...\n")

for table_name, delta_path in expected_tables:
    try:
        # Test 1: Table exists in Hive metastore
        tables_df = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}")
        table_exists = tables_df.filter(f"tableName = '{table_name.split('.')[1]}'").count() > 0
        
        if not table_exists:
            print(f"❌ {table_name}: NOT in Hive metastore")
            failed += 1
            continue
        
        # Test 2: Can read Delta table
        df = spark.read.format("delta").load(delta_path)
        
        # Test 3: Check row count (should be 0)
        count = df.count()
        
        # Test 4: Check schema exists
        column_count = len(df.columns)
        
        # Test 5: Get Delta details
        details = spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`").collect()[0]
        
        print(f"✅ {table_name}")
        print(f"   Location: {delta_path}")
        print(f"   Columns: {column_count}")
        print(f"   Rows: {count} (expected: 0)")
        print(f"   Files: {details['numFiles']}")
        print(f"   Size: {details['sizeInBytes']} bytes")
        print()
        
        passed += 1
        
    except Exception as e:
        print(f"❌ {table_name}: FAILED")
        print(f"   Error: {str(e)}")
        print()
        failed += 1

# Summary
print("=" * 80)
print("VALIDATION SUMMARY")
print("=" * 80)
print(f"Total tables: {total}")
print(f"Passed: {passed}")
print(f"Failed: {failed}")
print()

if failed == 0:
    print("✅ ALL DELTA TABLES VALIDATED SUCCESSFULLY")
    print("✅ SETUP COMPLETE - READY FOR BLOG 3A IMPLEMENTATION")
else:
    print(f"❌ {failed} TABLES FAILED VALIDATION")
    print("⚠️  FIX ERRORS BEFORE PROCEEDING")

spark.stop()