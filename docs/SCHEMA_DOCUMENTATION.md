# Schema Documentation

Complete reference for all 16 Delta tables in the lakehouse.

---

## Bronze Layer (3 tables)

### bronze.transactions

**Purpose:** Raw transactional data as ingested from source systems

**Location:** `gs://bucket/bronze/transactions`

**Update Frequency:** Real-time / Hourly batch

**Retention:** 3 years

**Contains PII:** Yes (customer_id)

**Schema (19 columns):**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | STRING | No | Primary key, unique transaction identifier |
| customer_id | STRING | Yes | Customer identifier |
| transaction_timestamp | TIMESTAMP | Yes | When transaction occurred |
| merchant_id | STRING | Yes | Merchant identifier |
| merchant_name | STRING | Yes | Merchant business name |
| product_category | STRING | Yes | Product category (Electronics, Food, etc.) |
| product_name | STRING | Yes | Specific product purchased |
| amount | DOUBLE | Yes | Transaction amount |
| fee_amount | DOUBLE | Yes | Gateway processing fee |
| cashback_amount | DOUBLE | Yes | Cashback given to customer |
| loyalty_points | LONG | Yes | Loyalty points earned |
| payment_method | STRING | Yes | Payment method used |
| transaction_status | STRING | Yes | Status (Pending, Completed, Failed) |
| device_type | STRING | Yes | Device used (Mobile, Desktop, API) |
| location_type | STRING | Yes | Location type (Online, In-store) |
| currency | STRING | Yes | Currency code (USD, EUR, etc.) |
| updated_at | TIMESTAMP | Yes | Last update timestamp |
| _change_type | STRING | Yes | CDC operation (INSERT, UPDATE, DELETE) |
| _delta_version | LONG | Yes | Delta table version that wrote this row |

**Additional columns (not in base schema, added during processing):**
- `is_deleted` BOOLEAN - Soft delete flag
- `deleted_at` TIMESTAMP - When marked as deleted
- `is_late_arrival` BOOLEAN - Late-arriving data flag (>3 days)
- `arrival_delay_hours` INT - Hours between event and processing
- `data_quality_flag` STRING - Validation status (PASSED/FAILED_VALIDATION)
- `validation_errors` STRING - Error descriptions

---

### bronze.job_control

**Purpose:** Tracks bronze layer job execution metadata for incremental processing

**Location:** `gs://bucket/metadata/bronze_job_control`

**Update Frequency:** Every job run

**Retention:** 90 days (then archived)

**Contains PII:** No

**Schema (23 columns):**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| job_name | STRING | No | Job identifier |
| layer | STRING | No | Layer name (bronze) |
| batch_id | STRING | No | UUID for this run |
| run_mode | STRING | No | incremental, backfill, full_refresh |
| status | STRING | No | running, completed, failed |
| processing_date | DATE | Yes | Date being processed |
| start_date | DATE | Yes | Backfill range start |
| end_date | DATE | Yes | Backfill range end |
| last_processed_timestamp | TIMESTAMP | Yes | Watermark for incremental |
| last_processed_batch_id | STRING | Yes | Previous successful batch |
| records_read | LONG | Yes | Records read from source |
| records_written | LONG | Yes | Records written to Delta |
| records_failed | LONG | Yes | Records that failed |
| records_quarantined | LONG | Yes | Records sent to quarantine |
| started_at | TIMESTAMP | Yes | Job start time |
| completed_at | TIMESTAMP | Yes | Job completion time |
| duration_seconds | LONG | Yes | Job duration |
| retry_count | LONG | Yes | Number of retries |
| max_retries | LONG | Yes | Max retries allowed (default: 3) |
| error_message | STRING | Yes | Error details if failed |
| triggered_by | STRING | Yes | scheduler, manual, retry |
| dataproc_cluster | STRING | Yes | Cluster name |
| spark_app_id | STRING | Yes | Spark application ID |

---

### bronze.quarantine

**Purpose:** Stores records that failed validation (Tier 1 errors)

**Location:** `gs://bucket/quarantine/bronze_transactions`

**Update Frequency:** Every job run

**Retention:** 1 year

**Schema:** Same as bronze.transactions + error fields:
- `error_reason` STRING (required) - Why validation failed
- `error_tier` STRING - TIER_1, TIER_2, TIER_3
- `quarantined_at` TIMESTAMP (required) - When quarantined
- `source_file` STRING - Original file name
- `processing_batch_id` STRING - Batch that found this error

---

## Silver Layer (3 tables)

### silver.transactions

**Purpose:** Cleaned, validated, deduplicated transactions (business-ready data)

**Location:** `gs://bucket/silver/transactions`

**Schema:** Identical to bronze.transactions (19 columns)

**Difference from Bronze:** Data quality
- Deduplicated (latest record by transaction_id)
- Excludes is_deleted=true
- Excludes data_quality_flag='FAILED_VALIDATION'

---

### silver.job_control

**Purpose:** Tracks silver layer processing

**Schema:** Same as bronze.job_control

---

### silver.quarantine

**Purpose:** Silver-specific validation failures

**Schema:** Same as bronze.quarantine

---

## Gold Layer (7 tables)

### gold.fact_transactions

**Purpose:** Star schema fact table with measurements and dimension foreign keys

**Location:** `gs://bucket/gold/fact_transactions`

**Partitioned By:** date_key

**Schema:**

**Foreign Keys:**
- customer_key → dim_customers
- merchant_key → dim_merchants
- payment_method_key → dim_payment_methods
- status_key → dim_transaction_status
- date_key → dim_date

**Degenerate Dimensions:**
- transaction_id, product_category, product_name, device_type

**Measures:**
- amount, fee_amount, cashback_amount, loyalty_points
- net_customer_amount (calculated)
- merchant_net_amount (calculated)
- gateway_revenue (calculated)

---

### gold.dim_customers (SCD Type 2)

**Purpose:** Customer dimension with historical tracking

**Location:** `gs://bucket/gold/dim_customers`

**Key Columns:**
- customer_key (PK, surrogate key)
- customer_id (business key)

**SCD Type 2 Fields:**
- effective_start_date
- effective_end_date (9999-12-31 for current)
- is_current (true for latest version)

**Tracks Changes In:** customer_segment, risk_score

---

### gold.dim_merchants (SCD Type 2)

**Purpose:** Merchant dimension with historical tracking

**Key Columns:**
- merchant_key (PK, surrogate key)
- merchant_id (business key)

**SCD Type 2 Fields:**
- effective_start_date
- effective_end_date
- is_current

**Tracks Changes In:** business_type, industry, is_active

---

### gold.dim_payment_methods

**Purpose:** Payment method lookup table (static)

**No SCD Type 2** - rarely changes

---

### gold.dim_transaction_status

**Purpose:** Transaction status lookup table (static)

**No SCD Type 2** - never changes

---

### gold.dim_date

**Purpose:** Calendar dimension for date-based queries

**Key:** date_key (YYYYMMDD format, e.g., 20241128)

**Attributes:** day_of_week, month, quarter, year, is_weekend, is_holiday

---

## Archive Layer (3 tables)

Long-term storage of job metadata (3 years retention).

- bronze.job_control_archive
- silver.job_control_archive
- gold.job_control_archive

**Schema:** Identical to live job_control tables

**Purpose:** Compliance, historical analysis, trend tracking

---

## Data Lineage
Source Systems (CSV/Parquet/JSON)
↓
Bronze (raw ingestion, all records)
↓
Silver (cleaned, deduplicated, validated)
↓
Gold (star schema, SCD Type 2, aggregations)

---

## Query Examples

### Check latest transactions
```sql
SELECT * FROM bronze_dataset.transactions 
WHERE transaction_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY transaction_timestamp DESC
LIMIT 10;
```

### Check job execution history
```sql
SELECT 
  job_name,
  status,
  records_written,
  duration_seconds / 60 as duration_minutes,
  started_at
FROM bronze_dataset.job_control
WHERE started_at >= CURRENT_DATE - 7
ORDER BY started_at DESC;
```

### Check data quality issues
```sql
SELECT 
  error_reason,
  COUNT(*) as error_count
FROM bronze_dataset.quarantine
WHERE quarantined_at >= CURRENT_DATE - 1
GROUP BY error_reason
ORDER BY error_count DESC;
```

### Query customer history (SCD Type 2)
```sql
SELECT 
  customer_id,
  customer_segment,
  effective_start_date,
  effective_end_date,
  is_current
FROM gold_dataset.dim_customers
WHERE customer_id = 'C001'
ORDER BY effective_start_date;
```