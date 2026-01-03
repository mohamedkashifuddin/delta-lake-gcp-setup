# Delta Lake on GCP Setup

Production-ready Delta Lake setup on Google Cloud Platform using Dataproc and Cloud SQL as external Hive metastore.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![GCP](https://img.shields.io/badge/cloud-GCP-blue)](https://cloud.google.com)
[![Delta Lake](https://img.shields.io/badge/delta-2.0-orange)](https://delta.io)
[![Version](https://img.shields.io/badge/version-1.0-green)](https://github.com/mohamedkashifuddin/delta-lake-gcp-setup/releases)

## ⚠️ Important Disclaimer

**This repository provides a near-production template for Delta Lake on GCP.** 

Before deploying to Production, UAT, Dev, or Sandbox environments:
- ✅ Review and modify configurations according to your organization's policies
- ✅ Adjust security settings, IAM roles, and network configurations
- ✅ Validate cost implications and resource sizing for your workload
- ✅ Consult with your DevOps, Security, and Data Engineering teams
- ✅ Different environments (Dev/UAT/Prod) may require different configurations
- ✅ Test thoroughly in non-production environments first

**This template serves as a starting point - customize it to meet your specific organizational requirements and compliance standards.**

## What This Repo Does

Automates the setup of:
- ✅ Cloud SQL MySQL as external Hive metastore (metadata persists across clusters)
- ✅ Dataproc cluster with Delta Lake 2.0 support
- ✅ KMS-encrypted password management
- ✅ Medallion architecture on GCS (Bronze/Silver/Gold)
- ✅ BigQuery external tables via BigLake (zero-copy querying)
- ✅ Automated pipeline execution script

**Why this exists:** Official docs make it look easy. It's not. This repo documents the real issues and provides working scripts.

## Quick Start (30 minutes)

### Prerequisites

- GCP project with billing enabled
- gcloud CLI installed and authenticated
- bq CLI installed (comes with gcloud)
- $300 free credits or budget for ~$53/month base cost

### Setup

```bash
# 1. Clone repo
git clone https://github.com/mohamedkashifuddin/delta-lake-gcp-setup.git
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
# Wait for cluster creation, then create Delta tables
./scripts/05-create-bigquery-external.sh   # 5 min  - BigQuery tables

# Total: ~25 minutes + table creation time
```

## Pipeline Execution Options

### Option 1: Automated Sequential Pipeline (Recommended)

The `run_delta_pipeline.sh` script executes all jobs sequentially and handles errors automatically.

**Before running, configure these variables in the script:**

```bash
# Edit run_delta_pipeline.sh
CLUSTER_NAME="dataproc-delta-cluster-final"  # Your cluster name
REGION="us-central1"                          # Your cluster region
```

**Execute the complete pipeline:**

```bash
# Make script executable
chmod +x run_delta_pipeline.sh

# Run the automated pipeline
./run_delta_pipeline.sh
```

**What the script does:**
1. Creates Bronze layer tables (transactions, quarantine, watermark)
2. Creates Silver layer tables with validation
3. Creates Gold layer (fact and dimension tables)
4. Archives testing data
5. Validates all tables
6. Verifies BigQuery external tables

All logs are saved in the `logs/` directory for troubleshooting.

### Option 2: Manual Job Submission

Submit individual jobs manually for testing or debugging:

```bash
# Bronze layer
gcloud dataproc jobs submit pyspark jobs/bronze/bronze_transactions_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Silver layer
gcloud dataproc jobs submit pyspark jobs/silver/silver_transactions_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Gold layer - Fact tables
gcloud dataproc jobs submit pyspark jobs/gold/fact/gold_fact_transactions_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Gold layer - Dimensions
gcloud dataproc jobs submit pyspark jobs/gold/dim/gold_dimensions_create.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Archives
gcloud dataproc jobs submit pyspark jobs/archive/create_archive_tables.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1
```

### Validation and Documentation Scripts

After pipeline execution, run these utility scripts:

```bash
# Validate all Delta tables (schemas, row counts, data quality)
gcloud dataproc jobs submit pyspark validate_all_tables.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1

# Generate detailed schema documentation for all tables
gcloud dataproc jobs submit pyspark describe_all_tables.py \
  --cluster=dataproc-delta-cluster-final --region=us-central1
```

**Note:** Replace `dataproc-delta-cluster-final` with your actual cluster name and `us-central1` with your region.

## Architecture

```
Raw Data (GCS)
    ↓
Bronze Layer (Delta on GCS) → Silver Layer (Delta on GCS) → Gold Layer (Delta on GCS)
    ↓                              ↓                              ↓
BigQuery (via BigLake) ← Analysts query here
    ↓
Cloud SQL MySQL ← Hive metastore (metadata persists)
```

**Key features:**
- ACID transactions on object storage
- Time travel (query historical versions)
- Schema evolution (add columns without rewrite)
- Persistent metadata (delete cluster, metadata survives)
- Dual query interface (Spark + BigQuery)

## What You Get

### Delta Tables (20+ total)

- **Bronze (4):** transactions, transactions_staging, quarantine, watermark
- **Silver (4):** transactions, transactions_staging, quarantine, watermark
- **Gold (9):** fact_transactions, fact_transactions_staging, dim_customer, dim_customer_staging, dim_merchant, dim_merchant_staging, + 3 other dimensions, watermark
- **Archives (3):** testing data archives (long-term retention)

**Note:** Staging tables enable blue-green deployment patterns and safer production updates.

### BigQuery External Tables (13+ total)

All main Delta tables (non-staging) exposed via BigLake for SQL querying.

### Features

- ✅ ACID transactions (UPDATE, DELETE, MERGE)
- ✅ Time travel (VERSION AS OF)
- ✅ Schema evolution (mergeSchema option)
- ✅ Soft deletes (is_deleted flag)
- ✅ SCD Type 2 (dim_customers, dim_merchants)
- ✅ Data quality framework (quarantine tables)
- ✅ Watermark tracking (incremental processing)
- ✅ Automated pipeline execution with error handling

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

Metadata survives in Cloud SQL. Recreate cluster anytime with the same command.

## Common Issues & Fixes

### Issue: "Access denied for user 'hive'@'localhost'"

**Fix:** Cloud SQL root password must be empty. Delete instance, recreate without --root-password flag.

### Issue: "could not parse resource []" (KMS error)

**Fix:** Grant decrypt permission:

```bash
gcloud kms keys add-iam-policy-binding hive-password-key \
  --location=us-central1 --keyring=dataproc-keys \
  --member=serviceAccount:PROJECT-NUMBER-compute@developer.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyDecrypter
```

### Issue: Dataproc version incompatible

**Fix:** Use --image-version 2.2-debian12 (2.0 no Delta, 2.3 broken init script)

### Issue: BigQuery "Dataset not found in location US"

**Fix:** Set BigQuery location to us-central1 in Query settings

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for complete list.

## Documentation

- [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - All errors encountered + solutions
- [SCHEMA_DOCUMENTATION.md](docs/SCHEMA_DOCUMENTATION.md) - Table schemas, business context

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
│   │   ├── bronze_transactions_staging.py
│   │   ├── bronze_watermark_create.py
│   │   └── bronze_quarantine_create.py
│   ├── silver/
│   │   ├── silver_transactions_create.py
│   │   ├── silver_transactions_staging.py
│   │   ├── silver_all_tables_create.py
│   │   ├── silver_quarantine_create.py
│   │   ├── silver_watermark_create.py
│   │   └── validate_bronze_silver.py
│   ├── gold/
│   │   ├── dim/
│   │   │   ├── gold_dimensions_create.py
│   │   │   ├── gold_dim_customer_staging.py
│   │   │   └── gold_dim_merchant_staging.py
│   │   ├── fact/
│   │   │   ├── gold_fact_transactions_create.py
│   │   │   └── gold_fact_transactions_staging.py
│   │   └── gold_watermark_create.py
│   └── archive/
│       └── create_archive_tables.py
├── bigquery_external_tables/
│   └── bigquery_external_tables_create.sql
├── testing/
│   └── [test scripts...]
├── testing_data/
│   └── [test data files...]
├── documentation/
│   ├── generate_schema_docs.py
│   ├── validate_complete_setup.py
│   └── verify_bigquery_tables.sh
├── run_delta_pipeline.sh          # Automated pipeline executor
├── validate_all_tables.py          # Table validation script
├── describe_all_tables.py          # Schema documentation generator
├── check_delta_tables.py           # Delta table health checker
└── logs/                           # Pipeline execution logs
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

3. **Implement pipeline orchestration:**
   - Schedule `run_delta_pipeline.sh` via Cloud Scheduler
   - Integrate with Apache Airflow for complex workflows
   - Set up monitoring and alerting

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

- **Issues:** [GitHub Issues](https://github.com/mohamedkashifuddin/delta-lake-gcp-setup/issues)
- **Discussions:** [Delta Lake Slack](https://delta.io/slack)

---

## 👤 Author

**Mohamed Kashifuddin**

Data Engineer | Delta Lake Enthusiast | Cloud Architecture

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohamedkashifuddin/)
[![Medium](https://img.shields.io/badge/Medium-12100E?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@mohamed_kashifuddin)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/mohamedkashifuddin)
[![Portfolio](https://img.shields.io/badge/Portfolio-FF7139?style=for-the-badge&logo=Firefox&logoColor=white)](https://mohamedkashifuddin.com)

📧 Email: mohamedkashifuddin24@gmail.com

---

**⭐ If this project helped you, please star the repository!**
