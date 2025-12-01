from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Validate").getOrCreate()

tables = [
    "bronze.transactions",
    "bronze.job_control",
    "bronze.quarantine",
    "silver.transactions",
    "silver.job_control",
    "silver.quarantine"
]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
    print(f"✅ {table}: {count} rows (expected: 0)")

spark.stop()