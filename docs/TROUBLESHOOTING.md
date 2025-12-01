# Troubleshooting Guide

This document covers every issue encountered during Delta Lake on GCP setup.

---

## Issue #1: Access denied for user 'hive'@'localhost'

### Symptoms
Caused by: java.sql.SQLException: Access denied for user 'hive'@'localhost' (using password: YES)
Hive metastore fails to start on Dataproc cluster.

### Root Cause
Cloud SQL was created with a root password. The cloud-sql-proxy initialization script expects **empty root password** to auto-create the hive user and database.

### Why This Happens
The init script logic:
1. Connects to MySQL as root with no password
2. Creates `hive` user with KMS-encrypted password
3. Creates `hive_metastore` database
4. Runs schema initialization

If root has a password, step 1 fails silently, hive user never gets created.

### Solution

**1. Delete the Cloud SQL instance:**
```bash
gcloud sql instances delete hive-metastore-mysql --quiet
```

**2. Recreate WITHOUT root password:**
```bash
gcloud sql instances create hive-metastore-mysql \
  --database-version=MYSQL_8_0 \
  --tier=db-n1-standard-1 \
  --region=us-central1 \
  --storage-type=SSD \
  --storage-size=10GB
# Notice: NO --root-password flag
```

**3. Recreate Dataproc cluster** (init script will now work)

### Time Lost
3 hours of debugging

---

## Issue #2: KMS Decrypt Error

### Symptoms
ERROR: (gcloud.kms.decrypt) could not parse resource []

DB_HIVE_PASSWORD=
Init script fails to decrypt hive password.

### Root Cause
Dataproc service account doesn't have permission to decrypt KMS key.

### Solution

Get your project number:
```bash
PROJECT_NUMBER=$(gcloud projects describe YOUR-PROJECT-ID --format="value(projectNumber)")
```

Grant decrypt permission:
```bash
gcloud kms keys add-iam-policy-binding hive-password-key \
  --location=us-central1 \
  --keyring=dataproc-keys \
  --member=serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyDecrypter
```

### Prevention
The `02-create-kms.sh` script in this repo handles this automatically.

### Time Lost
1 hour

---

## Issue #3: Dataproc 2.3 Initialization Failure

### Symptoms
Cluster creation fails during initialization with cloud-sql-proxy errors.

### Root Cause
Dataproc 2.3 has breaking changes in how cloud-sql-proxy binary is invoked. The initialization script isn't compatible.

### Solution
Use Dataproc 2.2:
```bash
--image-version 2.2-debian12
```

### Version Compatibility Matrix

| Version | Delta Support | Cloud SQL Proxy | Verdict |
|---------|---------------|-----------------|---------|
| 2.0     | ❌ No         | ✅ Yes          | No Delta component |
| 2.1     | ❌ No         | ✅ Yes          | No Delta component |
| **2.2** | ✅ Yes        | ✅ Yes          | **✅ Works** |
| 2.3     | ✅ Yes        | ✅ Yes           | **✅ Works**  |

### Time Lost
2 hours

---

## Issue #4: Stale Metastore Entries

### Symptoms
AnalysisException: Table bronze.test_transactions already exists
But `DROP TABLE` says table doesn't exist, or error persists.

### Root Cause
Previous failed table creation left orphaned metadata in Cloud SQL.

### Solution

Connect to Cloud SQL:
```bash
gcloud sql connect hive-metastore-mysql --user=root
```

Manual cleanup:
```sql
USE hive_metastore;

-- Find the stale table
SELECT TBL_ID, SD_ID FROM TBLS WHERE TBL_NAME='test_transactions';
-- Example output: TBL_ID=2, SD_ID=2

-- Delete in order (foreign key constraints)
DELETE FROM TBL_PRIVS WHERE TBL_ID=2;
DELETE FROM TABLE_PARAMS WHERE TBL_ID=2;
DELETE FROM PARTITIONS WHERE TBL_ID=2;
DELETE FROM PARTITION_KEYS WHERE TBL_ID=2;
DELETE FROM TAB_COL_STATS WHERE TBL_ID=2;

-- Get CD_ID
SELECT CD_ID FROM SDS WHERE SD_ID=2;
-- Example output: CD_ID=2

DELETE FROM COLUMNS_V2 WHERE CD_ID=2;
DELETE FROM SDS WHERE SD_ID=2;
DELETE FROM TBLS WHERE TBL_ID=2;
```

### Prevention
Use transactions in production pipelines. External metastore allows manual fixes (unlike in-cluster metastore).

### Time Lost
30 minutes

---

## Issue #5: Disk Quota Exceeded

### Symptoms
INVALID_ARGUMENT: Insufficient 'DISKS_TOTAL_GB' quota. Requested 3000.0, available 2048.0
### Root Cause
Multiple failed cluster attempts left orphaned disks. Free tier has limited quota.

### Solution

List all clusters:
```bash
gcloud dataproc clusters list --region=us-central1
```

Delete failed clusters:
```bash
gcloud dataproc clusters delete CLUSTER-NAME --region=us-central1 --quiet
```

List orphaned disks:
```bash
gcloud compute disks list --filter="zone:us-central1"
```

Delete if needed (usually auto-deleted with cluster):
```bash
gcloud compute disks delete DISK-NAME --zone=us-central1
```

### Prevention
Clean up failed resources immediately in free tier.

---

## Issue #6: Zone Mismatch

### Symptoms
ERROR: Instance not found in zone us-central1-a
When trying to SSH to cluster.

### Root Cause
Cluster created in `us-central1-b` (auto-selected), but SSH command used `us-central1-a`.

### Solution

Check actual zone:
```bash
gcloud dataproc clusters describe dataproc-delta-cluster-final \
  --region=us-central1 \
  --format="value(config.gceClusterConfig.zoneUri)"
```

SSH with correct zone:
```bash
gcloud compute ssh dataproc-delta-cluster-final-m --zone=us-central1-b
```

### Prevention
Explicitly set `--zone` in cluster creation (already done in `04-create-dataproc.sh`).

---

## Issue #7: BigQuery Location Mismatch

### Symptoms
Not found: Dataset bronze_dataset was not found in location US

External tables exist but aren't visible in BigQuery UI.

### Root Cause
Delta tables created in `us-central1`, but BigQuery UI defaults to `US` (multi-region).

### Solution

In BigQuery UI:
1. Click 3 dots next to project name
2. Select "Query settings"
3. Under "Processing location" → Choose `us-central1`
4. Click "Save"
5. Refresh page

Now datasets appear.

### Prevention
Create all resources in same region. Document location in README.

---

## Debugging Tips

### Check Init Script Logs
```bash
# Find staging bucket
gsutil ls gs://dataproc-staging-*

# View init script output
gsutil cat gs://dataproc-staging-XXXX/google-cloud-dataproc-metainfo/CLUSTER-UUID/CLUSTER-NAME-m/dataproc-initialization-script-0_output
```

### Check Hive Metastore Logs
```bash
# SSH to master
gcloud compute ssh dataproc-delta-cluster-final-m --zone=us-central1-b

# View logs
sudo cat /var/log/hive/hive-metastore.log | grep -i error
```

### Verify Cloud SQL Schema
```bash
gcloud sql connect hive-metastore-mysql --user=root
```
```sql
USE hive_metastore;
SELECT * FROM VERSION;  -- Should show 3.1.0
SHOW TABLES;            -- Should show 74 tables
```

### Check Delta Table
```bash
gsutil ls -r gs://your-bucket/bronze/transactions/
```

Should show:
- `_delta_log/` directory
- Parquet data files (if not empty)

---

## When All Else Fails

1. **Delete everything and start over:**
```bash
   # Delete cluster
   gcloud dataproc clusters delete dataproc-delta-cluster-final --region=us-central1 --quiet
   
   # Delete Cloud SQL
   gcloud sql instances delete hive-metastore-mysql --quiet
   
   # Delete KMS key (schedules deletion, can't be instant)
   gcloud kms keys versions destroy 1 --key=hive-password-key --keyring=dataproc-keys --location=us-central1
   
   # Delete GCS bucket
   gsutil -m rm -r gs://your-bucket/
```

2. **Re-run setup scripts from beginning**

3. **Open GitHub issue with:**
   - Error message (full text)
   - Commands run
   - Init script logs
   - Hive metastore logs

---

## Additional Resources

- [Dataproc Troubleshooting](https://cloud.google.com/dataproc/docs/support/troubleshooting)
- [Delta Lake Troubleshooting](https://docs.delta.io/latest/delta-faq.html)
- [Cloud SQL Debugging](https://cloud.google.com/sql/docs/mysql/diagnose-issues)