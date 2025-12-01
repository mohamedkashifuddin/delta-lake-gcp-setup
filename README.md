# Delta Lake on GCP Setup

Production-ready Delta Lake setup on Google Cloud Platform using Dataproc and Cloud SQL as external Hive metastore.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![GCP](https://img.shields.io/badge/cloud-GCP-blue)](https://cloud.google.com)
[![Delta Lake](https://img.shields.io/badge/delta-2.0-orange)](https://delta.io)

## What This Repo Does

Automates the setup of:
- ✅ Cloud SQL MySQL as external Hive metastore (metadata persists across clusters)
- ✅ Dataproc cluster with Delta Lake 2.0 support
- ✅ KMS-encrypted password management
- ✅ Medallion architecture on GCS (Bronze/Silver/Gold)
- ✅ BigQuery external tables via BigLake (zero-copy querying)

**Why this exists:** Official docs make it look easy. It's not. This repo documents the real issues and provides working scripts.

## Quick Start (30 minutes)

### Prerequisites
- GCP project with billing enabled
- `gcloud` CLI installed and authenticated
- `bq` CLI installed (comes with gcloud)
- $300 free credits or budget for ~$53/month base cost

### Setup
```bash
# 1. Clone repo
git clone https://github.com/yourusername/delta-lake-gcp-setup.git
cd delta-lake-gcp-setup

# 2. Configure environment
cp configs/environment.sh.template configs/environment.sh
nano configs/environment.sh  # Fill in your PROJECT_ID and other details

# 3. Authenticate
gcloud auth login
gcloud config set project YOUR-PROJECT-ID
source configs/environment.sh

# 4. Run setup scripts in order
chmod +x scripts/*.sh

./scripts/01-create-cloud-sql.sh           # 5 min  - MySQL metastore
./scripts/02-create-kms.sh                 # 2 min  - Encryption key
./scripts/03-create-buckets.sh             # 3 min  - GCS structure + password
./scripts/04-create-dataproc.sh            # 10 min - Dataproc cluster
# Wait for cluster creation, then create Delta tables from jobs/
./scripts/05-create-bigquery-external.sh   # 5 min  - BigQuery tables

# Total: ~25 minutes + table creation time
```

### Create Delta Tables
```bash
# Submit jobs to create empty Delta tables (schemas validated)
cd jobs/

# Bronze layer
gcloud dataproc jobs submit pyspark bronze_transactions_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Silver layer
gcloud dataproc jobs submit pyspark silver_all_tables_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Gold layer
gcloud dataproc jobs submit pyspark gold_fact_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

gcloud dataproc jobs submit pyspark gold_dimensions_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Archives
gcloud dataproc jobs submit pyspark create_archive_tables.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Validate setup
gcloud dataproc jobs submit pyspark validate_complete_setup.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1
```

## Architecture

Raw Data (GCS)
↓
Bronze Layer (Delta on GCS) → Silver Layer (Delta on GCS) → Gold Layer (Delta on GCS)
↓                              ↓                              ↓
BigQuery (via BigLake) ← Analysts query here
↓
Cloud SQL MySQL ← Hive metastore (metadata persists)

**Key features:**
- ACID transactions on object storage
- Time travel (query historical versions)
- Schema evolution (add columns without rewrite)
- Persistent metadata (delete cluster, metadata survives)
- Dual query interface (Spark + BigQuery)

## What You Get

### Delta Tables (16 total)
- **Bronze (3):** transactions, job_control, quarantine
- **Silver (3):** transactions, job_control, quarantine  
- **Gold (7):** fact_transactions, 5 dimensions, job_control
- **Archives (3):** job_control archives (long-term retention)

### BigQuery External Tables (13 total)
All Delta tables exposed via BigLake for SQL querying.

### Features
- ✅ ACID transactions (UPDATE, DELETE, MERGE)
- ✅ Time travel (VERSION AS OF)
- ✅ Schema evolution (mergeSchema option)
- ✅ Soft deletes (is_deleted flag)
- ✅ SCD Type 2 (dim_customers, dim_merchants)
- ✅ Data quality framework (quarantine tables)
- ✅ Watermark tracking (incremental processing)

## Cost Breakdown

### Running 24/7
- Cloud SQL: $50/month
- Dataproc (single-node): $292/month
- GCS: $2/month
- KMS: $1/month
- **Total: ~$345/month**

### Optimized (delete cluster when not using)
- Cloud SQL: $50/month (always on, has metadata)
- Dataproc: $0.40/hour × actual usage
- GCS: $2/month
- KMS: $1/month
- **Total: ~$53/month + usage**

**Example:** 2 hours/day usage = ~$78/month total

**Delete cluster command:**
```bash
gcloud dataproc clusters delete dataproc-delta-cluster-final --region=us-central1 --quiet
```

Metadata survives in Cloud SQL. Recreate cluster anytime with same command.

## Common Issues & Fixes

### Issue: "Access denied for user 'hive'@'localhost'"
**Fix:** Cloud SQL root password must be empty. Delete instance, recreate without `--root-password` flag.

### Issue: "could not parse resource []" (KMS error)
**Fix:** Grant decrypt permission:
```bash
gcloud kms keys add-iam-policy-binding hive-password-key \
  --location=us-central1 --keyring=dataproc-keys \
  --member=serviceAccount:PROJECT-NUMBER-compute@developer.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyDecrypter
```

### Issue: Dataproc version incompatible
**Fix:** Use `--image-version 2.2-debian12` (2.0 no Delta, 2.3 broken init script)

### Issue: BigQuery "Dataset not found in location US"
**Fix:** Set BigQuery location to `us-central1` in Query settings

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for complete list.

## Documentation

- [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - All errors encountered + solutions
- [SCHEMA_DOCUMENTATION.md](docs/SCHEMA_DOCUMENTATION.md) - Table schemas, business context
- [Blog Post](LINK-TO-BLOG) - Complete tutorial with real issues

## Project Structure
```
delta-lake-gcp-setup/
├── README.md
├── LICENSE
├── .gitignore
├── configs/
│   └── environment.sh.template
├── scripts/
│   ├── 01-create-cloud-sql.sh
│   ├── 02-create-kms.sh
│   ├── 03-create-buckets.sh
│   ├── 04-create-dataproc.sh
│   └── 05-create-bigquery-external.sh
├── jobs/
│   ├── bronze/
│   │   ├── bronze_transactions_create.py
│   │   ├── bronze_watermark_create.py
│   │   └── bronze_quarantine_create.py
│   ├── silver/
│   │   ├── silver_transactions_create.py
│   │   ├── silver_all_tables_create.py
│   │   ├── silver_quarantine_create.py
│   │   ├── silver_watermark_create.py
│   │   └── validate_bronze_silver.py
│   ├── gold/
│   │   ├── dim/
│   │   │   └── gold_dimensions_create.py
│   │   ├── fact/
│   │   │   └── gold_fact_transactions_create.py
│   │   └── gold_watermark_create.py
│   └── archive/
│       ├── create_archive_tables.py
│       ├── cleanup_archive_test.py
│       └── test_archive_workflow.py
├── bigquery_external_tables/
│   └── bigquery_external_tables_create.sql
├── testing/
│   ├── test_bronze_insert.py
│   ├── test_late_arrival.py
│   ├── test_quarantine.py
│   ├── test_scd_type2.py
│   ├── test_soft_delete.py
│   └── cleanup_test_data.py
├── testing_data/
│   ├── bronze_data_test_BQ.sql
│   └── test files...
├── documentation/
│   ├── generate_schema_docs.py
│   ├── validate_complete_setup.py
│   └── verify_bigquery_tables.sh
└── validate_all_tables.py

```
## Next Steps

After setup completes:

1. **Test Delta features:**
```python
   # SSH to cluster
   gcloud compute ssh dataproc-delta-cluster-final-m --zone=us-central1-b
   
   # Test ACID transaction
   pyspark
   >>> spark.sql("UPDATE bronze.transactions SET amount = amount * 2 WHERE id='TEST001'")
```

2. **Query from BigQuery:**
```sql
   SELECT COUNT(*) FROM bronze_dataset.transactions;
```

3. **Implement pipeline (Blog 3a):**
   - Bronze layer ingestion
   - Silver layer transformations
   - Gold layer star schema
   - Airflow orchestration

## Related Projects

- **Blog 1 & 2:** BigQuery-native pipeline - [payment-gateway-bigquery](LINK-TO-REPO)
- **Blog 3a-3e:** Delta Lake implementation - [delta-lake-gcp-implementation](LINK-TO-REPO)

## Contributing

Found a bug? Have a suggestion?
1. Open an issue
2. Submit a PR
3. Star the repo if it helped you

## License

MIT License - see [LICENSE](LICENSE)

## Acknowledgments

Built after 6 hours of debugging. You're welcome.

If this saved you time, ⭐ star the repo.

## Support

- **Issues:** [GitHub Issues](https://github.com/yourusername/delta-lake-gcp-setup/issues)
- **Blog:** [Link to blog post](LINK-TO-BLOG)
- **Discussions:** [Delta Lake Slack](https://delta.io/slack)