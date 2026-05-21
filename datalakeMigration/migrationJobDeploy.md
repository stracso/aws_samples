# Iceberg Migration Glue Job — Deployment Guide

## Prerequisites

- AWS CLI v2 installed and configured
- An IAM role for Glue with permissions for:
  - `glue:*` (catalog operations)
  - `s3:*` on both the shared and dedicated buckets
  - `logs:*` for CloudWatch logging
- The script uploaded to an S3 bucket accessible by the Glue role
- Glue 5.0 runtime (supports Iceberg natively via `--datalake-formats iceberg`)

---

## 1. Upload the Script to S3

```bash
SCRIPT_BUCKET="jpmc-iceberg-warehouse-038676220235-us-west-2"
aws s3 cp iceberg_migration_job.py s3://${SCRIPT_BUCKET}/glue-scripts/iceberg_migration_job.py
```

---

## 2. Create the Glue Job

```bash
ROLE_ARN="arn:aws:iam::038676220235:role/GlueIcebergRole"
SCRIPT_BUCKET="jpmc-iceberg-warehouse-038676220235-us-west-2"

aws glue create-job \
  --name "iceberg-migration-job" \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${SCRIPT_BUCKET}"'/glue-scripts/iceberg_migration_job.py",
    "PythonVersion": "3"
  }' \
  --glue-version "5.0" \
  --worker-type "G.1X" \
  --number-of-workers 2 \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.defaultCatalog=glue_catalog",
    "--enable-glue-datacatalog": "true"
  }'
```

> Adjust `--number-of-workers` based on table sizes. For large tables (100M+ rows), consider `G.2X` workers or increasing the count.

---

## 3. Run the Job

### Basic invocation

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job" \
  --arguments '{
    "--GLUE_ENDPOINT": "https://glue.us-west-2.amazonaws.com",
    "--REGION": "us-west-2",
    "--DATABASE": "jpmc_demo_warehouse",
    "--TABLES": "main_rd_table,orders_pftest_small_datepart",
    "--SHARED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-whv2-038676220235-us-west-2",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "false",
    "--DRYRUN": "true"
  }'
```

### Dry-run mode (no mutations, logs what would happen)

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job" \
  --arguments '{
    "--GLUE_ENDPOINT": "https://glue.us-east-1.amazonaws.com",
    "--DATABASE": "jpmc_demo_warehouse",
    "--TABLES": "main_rd_table,main_sample_table",
    "--SHARED_BUCKET": "jpmc-shared-datalake-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-whv2-038676220235-us-west-2",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "true",
    "--DRYRUN": "true"
  }'
```

### Skip cleanup (migrate without expiring snapshots)

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job" \
  --arguments '{
    "--GLUE_ENDPOINT": "https://glue.us-east-1.amazonaws.com",
    "--DATABASE": "jpmc_demo_warehouse",
    "--TABLES": "main_rd_table,main_sample_table",
    "--SHARED_BUCKET": "jpmc-shared-datalake-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-whv2-038676220235-us-west-2",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "true",
    "--DRYRUN": "false"
  }'
```

---

## 4. Monitor the Job Run

```bash
# Get the run ID from the start-job-run output, then:
RUN_ID="jr_xxxxxxxxxxxxxxxx"

aws glue get-job-run \
  --job-name "iceberg-migration-job" \
  --run-id "${RUN_ID}"
```

Check CloudWatch Logs at:
```
/aws-glue/jobs/output   — stdout/logger output
/aws-glue/jobs/error    — stderr/exceptions
```

---

## 5. Update an Existing Job

If you need to update the script or default arguments:

```bash
aws glue update-job \
  --job-name "iceberg-migration-job" \
  --job-update '{
    "Command": {
      "Name": "glueetl",
      "ScriptLocation": "s3://'"${SCRIPT_BUCKET}"'/scripts/iceberg_migration_job.py",
      "PythonVersion": "3"
    },
    "NumberOfWorkers": 8,
    "WorkerType": "G.2X"
  }'
```

---

## 6. Delete the Job (when no longer needed)

```bash
aws glue delete-job --job-name "iceberg-migration-job"
```

---

## Job Parameters Reference

| Parameter | Required | Description |
|-----------|----------|-------------|
| `GLUE_ENDPOINT` | Yes | Glue service endpoint URL (e.g. `https://glue.us-east-1.amazonaws.com`) |
| `DATABASE` | Yes | Glue Catalog database name |
| `TABLES` | Yes | Comma-separated list of Iceberg table names to migrate |
| `SHARED_BUCKET` | Yes | Source S3 bucket (shared) |
| `SHARED_PREFIX` | Yes | S3 prefix in the shared bucket |
| `DEDICATED_BUCKET` | Yes | Target S3 bucket (dedicated) |
| `DEDICATED_PREFIX` | Yes | S3 prefix in the dedicated bucket |
| `SKIP_CLEANUP` | Yes | `"true"` to skip snapshot expiration and orphan file removal |
| `DRYRUN` | Yes | `"true"` to log actions without executing mutations |

---

## Migration Pipeline Steps

1. **Resolve metadata** — Reads the latest `metadata_location` from each table's Glue Catalog entry
2. **Register tables** — Drops and re-registers each table pointing to the dedicated bucket metadata
3. **Rewrite data files** — Rewrites Parquet/data files so internal references use the new paths
4. **Update table location** — Patches the Glue Catalog `StorageDescriptor.Location` and `metadata_location` from shared to dedicated URI
5. **Cleanup** — Expires old snapshots and removes orphan files (skippable)
6. **Validate** — Logs row counts per table as a sanity check

---

## Recommended Workflow

1. Run with `--DRYRUN true` first to verify table resolution and path mapping
2. Run with `--SKIP_CLEANUP true` to migrate without removing old data (allows rollback)
3. Validate data in the dedicated bucket
4. Run again with `--SKIP_CLEANUP false` to clean up old snapshots and orphan files
