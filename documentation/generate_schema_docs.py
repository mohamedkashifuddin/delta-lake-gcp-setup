from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import json
from delta.tables import DeltaTable # Needed to access Delta-specific features

spark = SparkSession.builder \
    .appName("Generate Schema Documentation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Utility Function for Schema Formatting ---
def format_schema_to_markdown(df_schema):
    """Formats a PySpark StructType schema into a Markdown table."""
    schema_lines = ["| Column Name | Data Type | Nullable (Spark Schema) |", "|---|---|---|"]
    
    for field in df_schema:
        # Extract the type (e.g., string, long, timestamp)
        data_type = str(field.dataType).split('Type')[0].upper()
        
        # Note: PySpark's schema 'nullable' is the maximum possibility, not the applied constraint.
        nullable_status = "TRUE" if field.nullable else "**FALSE** (NOT NULL inferred)"
        
        schema_lines.append(f"| {field.name} | {data_type} | {nullable_status} |")
        
    return "\n".join(schema_lines)

# --- Define all tables with business context (Metadata remains the same) ---
tables_metadata = {
    "bronze.transactions": {
        "layer": "Bronze",
        "purpose": "Raw transactional data as ingested from source systems",
        "location": "gs://delta-lake-payment-gateway-476820/bronze/transactions",
        "update_frequency": "Real-time / Batch (hourly)",
        "retention": "3 years",
        "contains_pii": True,
        "key_columns": ["transaction_id"],
        "partitioned_by": None,
        "business_owner": "Payments Team",
        "technical_owner": "Data Engineering"
    },
    "silver.transactions": {
        "layer": "Silver",
        "purpose": "Cleaned, validated, deduplicated transactions (business-ready data)",
        "location": "gs://delta-lake-payment-gateway-476820/silver/transactions",
        "update_frequency": "Hourly (after bronze processing)",
        "retention": "3 years",
        "contains_pii": True,
        "key_columns": ["transaction_id"],
        "partitioned_by": None,
        "business_owner": "Analytics Team",
        "technical_owner": "Data Engineering"
    },
    "gold.fact_transactions": {
        "layer": "Gold",
        "purpose": "Star schema fact table with transaction measurements and dimension foreign keys",
        "location": "gs://delta-lake-payment-gateway-476820/gold/fact_transactions",
        "update_frequency": "Daily",
        "retention": "5 years",
        "contains_pii": False,
        "key_columns": ["transaction_id"],
        "partitioned_by": "date_key",
        "business_owner": "Finance & Analytics",
        "technical_owner": "Data Engineering"
    },
    "gold.dim_customers": {
        "layer": "Gold",
        "purpose": "Customer dimension with SCD Type 2 (tracks customer segment changes)",
        "location": "gs://delta-lake-payment-gateway-476820/gold/dim_customers",
        "update_frequency": "Daily",
        "retention": "Permanent",
        "contains_pii": True,
        "key_columns": ["customer_key"],
        "partitioned_by": None,
        "business_owner": "Customer Success",
        "technical_owner": "Data Engineering"
    },
    # (Rest of tables_metadata omitted for brevity, but assumed to be complete)
}

# Add all original tables back for the full script run
tables_metadata.update({
    "bronze.job_control": {
        "layer": "Bronze",
        "purpose": "Tracks bronze layer job execution metadata for incremental processing",
        "location": "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control",
        "update_frequency": "Every job run",
        "retention": "90 days (then archived)",
        "contains_pii": False,
        "key_columns": ["job_name", "batch_id"],
        "partitioned_by": None,
        "business_owner": "Data Engineering",
        "technical_owner": "Data Engineering"
    },
    "bronze.quarantine": {
        "layer": "Bronze",
        "purpose": "Stores records that failed validation (Tier 1 errors)",
        "location": "gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions",
        "update_frequency": "Every job run",
        "retention": "1 year",
        "contains_pii": True,
        "key_columns": ["processing_batch_id"],
        "partitioned_by": None,
        "business_owner": "Data Quality Team",
        "technical_owner": "Data Engineering"
    },
    "silver.job_control": {
        "layer": "Silver",
        "purpose": "Tracks silver layer job execution metadata",
        "location": "gs://delta-lake-payment-gateway-476820/metadata/silver_job_control",
        "update_frequency": "Every job run",
        "retention": "90 days (then archived)",
        "contains_pii": False,
        "key_columns": ["job_name", "batch_id"],
        "partitioned_by": None,
        "business_owner": "Data Engineering",
        "technical_owner": "Data Engineering"
    },
    "silver.quarantine": {
        "layer": "Silver",
        "purpose": "Stores records that failed silver-specific validations",
        "location": "gs://delta-lake-payment-gateway-476820/quarantine/silver_transactions",
        "update_frequency": "Every job run",
        "retention": "1 year",
        "contains_pii": True,
        "key_columns": ["processing_batch_id"],
        "partitioned_by": None,
        "business_owner": "Data Quality Team",
        "technical_owner": "Data Engineering"
    },
    "gold.dim_merchants": {
        "layer": "Gold",
        "purpose": "Merchant dimension with SCD Type 2 (tracks business type changes)",
        "location": "gs://delta-lake-payment-gateway-476820/gold/dim_merchants",
        "update_frequency": "Daily",
        "retention": "Permanent",
        "contains_pii": True,
        "key_columns": ["merchant_key"],
        "partitioned_by": None,
        "business_owner": "Merchant Relations",
        "technical_owner": "Data Engineering"
    },
    "gold.dim_payment_methods": {
        "layer": "Gold",
        "purpose": "Payment method lookup table (Credit Card, UPI, etc.)",
        "location": "gs://delta-lake-payment-gateway-476820/gold/dim_payment_methods",
        "update_frequency": "Ad-hoc (rarely changes)",
        "retention": "Permanent",
        "contains_pii": False,
        "key_columns": ["payment_method_key"],
        "partitioned_by": None,
        "business_owner": "Product Team",
        "technical_owner": "Data Engineering"
    },
    "gold.dim_transaction_status": {
        "layer": "Gold",
        "purpose": "Transaction status lookup table (Pending, Completed, Failed)",
        "location": "gs://delta-lake-payment-gateway-476820/gold/dim_transaction_status",
        "update_frequency": "Ad-hoc (rarely changes)",
        "retention": "Permanent",
        "contains_pii": False,
        "key_columns": ["status_key"],
        "partitioned_by": None,
        "business_owner": "Operations Team",
        "technical_owner": "Data Engineering"
    },
    "gold.dim_date": {
        "layer": "Gold",
        "purpose": "Calendar dimension (date, day of week, fiscal periods)",
        "location": "gs://delta-lake-payment-gateway-476820/gold/dim_date",
        "update_frequency": "Yearly (extend for new years)",
        "retention": "Permanent",
        "contains_pii": False,
        "key_columns": ["date_key"],
        "partitioned_by": None,
        "business_owner": "Analytics Team",
        "technical_owner": "Data Engineering"
    },
    "gold.job_control": {
        "layer": "Gold",
        "purpose": "Tracks gold layer job execution metadata",
        "location": "gs://delta-lake-payment-gateway-476820/metadata/gold_job_control",
        "update_frequency": "Every job run",
        "retention": "90 days (then archived)",
        "contains_pii": False,
        "key_columns": ["job_name", "batch_id"],
        "partitioned_by": None,
        "business_owner": "Data Engineering",
        "technical_owner": "Data Engineering"
    },
    "bronze.job_control_archive": {
        "layer": "Archive",
        "purpose": "Long-term storage of bronze job execution history (compliance)",
        "location": "gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive",
        "update_frequency": "Monthly (archival job)",
        "retention": "3 years",
        "contains_pii": False,
        "key_columns": ["job_name", "batch_id"],
        "partitioned_by": None,
        "business_owner": "Compliance Team",
        "technical_owner": "Data Engineering"
    },
    "silver.job_control_archive": {
        "layer": "Archive",
        "purpose": "Long-term storage of silver job execution history",
        "location": "gs://delta-lake-payment-gateway-476820/metadata/silver_job_control_archive",
        "update_frequency": "Monthly (archival job)",
        "retention": "3 years",
        "contains_pii": False,
        "key_columns": ["job_name", "batch_id"],
        "partitioned_by": None,
        "business_owner": "Compliance Team",
        "technical_owner": "Data Engineering"
    },
    "gold.job_control_archive": {
        "layer": "Archive",
        "purpose": "Long-term storage of gold job execution history",
        "location": "gs://delta-lake-payment-gateway-476820/metadata/gold_job_control_archive",
        "update_frequency": "Monthly (archival job)",
        "retention": "3 years",
        "contains_pii": False,
        "key_columns": ["job_name", "batch_id"],
        "partitioned_by": None,
        "business_owner": "Compliance Team",
        "technical_owner": "Data Engineering"
    }
})


markdown_output = [
    "# Delta Lake Data Dictionary and Schema Documentation",
    f"**Generated:** {spark.sql('SELECT current_timestamp()').collect()[0][0]}",
    "\n---",
    "\nThis document details the purpose, structure, and governance of all key Delta Lake tables across the Bronze, Silver, and Gold layers of the Payment Gateway data warehouse."
]

for table_name, metadata in tables_metadata.items():
    delta_path = metadata['location']
    markdown_output.append(f"\n\n## 🧊 {table_name}")
    markdown_output.append(f"\n**Layer:** {metadata['layer']}")
    markdown_output.append(f"\n**Purpose:** {metadata['purpose']}")
    
    # Business Metadata Table
    markdown_output.append("\n### 🏷️ Business & Governance Metadata")
    markdown_output.append("| Attribute | Value |")
    markdown_output.append("|---|---|")
    
    for key, value in metadata.items():
        if key not in ['location', 'layer', 'purpose']: # Exclude keys already used
            markdown_output.append(f"| {key.replace('_', ' ').title()} | {value} |")
    
    markdown_output.append("\n### 🏛️ Physical Schema and Constraints")
    
    try:
        # 1. Read the DataFrame and Schema
        df = spark.read.format("delta").load(delta_path)
        
        # 2. Get Delta Lake table details
        details = spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`").collect()[0]
        
        markdown_output.append(f"\n**Storage Location:** `{delta_path}`")
        markdown_output.append(f"**Format:** `{details['format']}`")
        # NOTE: df.count() can be slow on large tables; use table statistics for better performance in production.
        markdown_output.append(f"**Current Row Count:** `{df.count()}` (Approx)") 
        markdown_output.append(f"**Size (Bytes):** `{details['sizeInBytes']}`")
        
        # 3. Schema as a Markdown table
        markdown_output.append("\n#### Column Definitions (Includes Nullability from initial creation)")
        markdown_output.append(format_schema_to_markdown(df.schema))
        
        # 4. Check for Check Constraints (This is how NOT NULL is enforced in Delta)
        
        # We need to register the table if it's not already in the catalog to use the DESCRIBE command
        # Note: In a real environment, you'd CREATE TABLE using LOCATION for this to work perfectly.
        # For simplicity here, we'll try to directly query properties related to constraints.
        
        constraint_props = spark.sql(f"SHOW TBLPROPERTIES delta.`{delta_path}` ('delta.constraints')").collect()
        
        markdown_output.append("\n#### Data Quality Constraints (NOT NULL & Check Constraints)")
        
        if constraint_props:
            # Format the constraints nicely
            markdown_output.append("| Constraint Name | Definition |")
            markdown_output.append("|---|---|")
            
            # The constraints are stored in a map property called 'delta.constraints', 
            # where key is the constraint name and value is the SQL expression.
            # We'll parse the table properties output to find them.
            
            # Since SHOW TBLPROPERTIES returns key/value pairs, we look for 'delta.constraints.*'
            # A common way to enforce NOT NULL on 'transaction_id' is:
            # ALTER TABLE ... ADD CONSTRAINT not_null_tx_id CHECK (transaction_id IS NOT NULL)
            
            # Due to the complexity of getting nested constraints via SHOW TBLPROPERTIES in all environments,
            # we demonstrate how to list them if they were standard properties:
            
            found_constraints = False
            for row in constraint_props:
                key, value = row['key'], row['value']
                if key.startswith('delta.constraints.'):
                    constraint_name = key.split('.')[-1]
                    markdown_output.append(f"| {constraint_name} | `{value}` |")
                    found_constraints = True

            if not found_constraints:
                 markdown_output.append("| *No explicit CHECK or NOT NULL constraints found in properties.* | |")
                 
        else:
            markdown_output.append("\n*No explicit CHECK or NOT NULL constraints found via table properties.*")
            markdown_output.append("\n**Action:** If this table is in the Silver or Gold layer, ensure key columns are enforced using `ALTER TABLE ... ADD CONSTRAINT ...`.")


    except AnalysisException as e:
        if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
            markdown_output.append("\n**Status:** ⚠️ **Table Not Yet Created**")
            markdown_output.append(f"The physical Delta table has not been initialized yet at location: `{delta_path}`.")
            markdown_output.append("\n*Run the table creation scripts and re-run this documentation script to capture the schema.*")
        else:
            markdown_output.append(f"\n**Status:** ❌ **Error Reading Schema**")
            markdown_output.append(f"\n*An unexpected error occurred: {e}*")

# --- Final Output ---

# 1. Print a summary to the console
print("=" * 80)
print("DELTA LAKE SCHEMA DOCUMENTATION REPORT")
print("Report generated and saved to /tmp/schema_documentation.md")
print("=" * 80)

# 2. Save documentation as Markdown
doc_content = "\n".join(markdown_output)
try:
    with open('/tmp/schema_documentation.md', 'w') as f:
        f.write(doc_content)
    print("\n✅ Documentation saved to /tmp/schema_documentation.md")
except Exception as e:
    print(f"\n❌ Failed to save Markdown file: {e}")

# 3. Save metadata as JSON (for programmatic access)
try:
    with open('/tmp/schema_docs.json', 'w') as f:
        json.dump(tables_metadata, f, indent=2)
    print("✅ Metadata JSON saved to /tmp/schema_docs.json")
except Exception as e:
    print(f"\n❌ Failed to save JSON file: {e}")

spark.stop()