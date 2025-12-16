from pyspark.sql import SparkSession
import sys

# Initialize Spark Session with Hive support for Metastore/Catalog access
spark = SparkSession.builder \
    .appName("DescribeAllDeltaTablesDynamic") \
    .enableHiveSupport() \
    .getOrCreate()

print("--- Starting Dynamic Schema Description for All Layers ---")

# 1. Get a list of all schemas (databases)
try:
    schema_rows = spark.sql("SHOW DATABASES").collect()
    
    # FIX: Access the database name by positional index [0] instead of named attribute 
    # '.databaseName' to handle potential inconsistencies in Metastore result sets.
    schemas = [row[0] for row in schema_rows] 
    
    print(f"Found {len(schemas)} schemas in the Metastore: {', '.join(schemas)}")
except Exception as e:
    print(f"CRITICAL ERROR: Could not list databases. Metastore connection may be down. Details: {str(e)}")
    # If this fails, we cannot proceed, so we stop and exit.
    spark.stop()
    sys.exit(1)


total_tables_described = 0

# 2. Iterate through each schema
for schema in schemas:
    # Skip system schemas that don't hold project data
    if schema.startswith("default") or schema.startswith("system"): 
        continue
    
    print(f"\n==============================================")
    print(f"=== PROCESSING SCHEMA: {schema.upper()} ===")
    print(f"==============================================")

    # 3. List all tables in the current schema
    try:
        # Note: SHOW TABLES IN <schema> returns columns: database, tableName, isTemporary
        table_rows = spark.sql(f"SHOW TABLES IN {schema}").collect()
        
        if not table_rows:
            print(f"No tables found in schema '{schema}'.")
            continue
            
        # 4. Iterate through each table and describe it
        for row in table_rows:
            # We access the table name by the second positional index [1]
            table_name_simple = row[1] 
            full_table_name = f"{schema}.{table_name_simple}"
            
            print(f'\n--- SCHEMA FOR: {full_table_name} ---')
            
            # Print the schema (data types and nullability)
            spark.sql(f'DESCRIBE {full_table_name}').show(100, False)
            
            # Print table properties (useful for Delta location/version)
            print(f'--- DETAIL FOR: {full_table_name} ---')
            spark.sql(f'DESCRIBE DETAIL {full_table_name}').show(1, False)
            
            total_tables_described += 1

    except Exception as e:
        print(f"ERROR: Could not describe tables in schema {schema}. Details: {str(e)}")

print(f"\n--- Dynamic Schema Description Complete. Total Tables Described: {total_tables_described} ---")

spark.stop()