# Iceberg Cross-Region Migration Glue Job — Deployment Guide

## Overview

This guide covers deploying and running `iceberg_migration_job_xr.py`, which migrates Iceberg tables **across AWS regions** — reading from a source Glue Catalog and S3 bucket in one region (e.g., us-west-2) and writing to a destination Glue Catalog and S3 bucket in another region (e.g., us-east-1).

---

## Prerequisites

- AWS CLI v2 installed and configured
- An IAM role for Glue with **cross-region** permissions:
  - `glue:GetTable`, `glue:GetTables`, `glue:GetDatabase` on the **source** region catalog
  - `glue:*` on the **destination** region catalog (create/update/delete tables and databases)
  - `s3:GetObject`, `s3:ListBucket` on the **source** bucket (us-west-2)
  - `s3:*` on the **destination** bucket (us-east-1)
  - `logs:*` for CloudWatch logging
- The script uploaded to an S3 bucket in the **destination** region (where the Glue job runs)
- Glue 5.0 runtime (supports Iceberg natively via `--datalake-formats iceberg`)
- **Source data (including `metadata/` folder) already copied to the destination S3 bucket** before running the job (use `s3_copy_job.py` or S3 Batch Replication)

> **Important:** The `register_table` procedure points the catalog at a metadata.json file in the destination bucket. If that file hasn't been copied yet, registration will silently fail or produce a broken table entry.

---

## 1. Upload the Script to S3 (Destination Region)

```bash
# jpmc-iceberg-warehouse-038676220235-us-east-1
SCRIPT_BUCKET="jpmc-iceberg-warehouse-038676220235-us-east-1"

aws s3 cp iceberg_migration_job_xr.py \
  s3://${SCRIPT_BUCKET}/glue-scripts/iceberg_migration_job_xr.py \
  --region us-east-1
```

---

## 2. Create the Glue Job (in Destination Region)

The job should be created in the **destination region** (us-east-1) since Spark catalog operations target that region's Glue endpoint.

```bash
ROLE_ARN="arn:aws:iam::038676220235:role/GlueIcebergRole"
SCRIPT_BUCKET="jpmc-iceberg-warehouse-038676220235-us-east-1"

aws glue create-job \
  --name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${SCRIPT_BUCKET}"'/glue-scripts/iceberg_migration_job_xr.py",
    "PythonVersion": "3"
  }' \
  --glue-version "5.0" \
  --worker-type "G.1X" \
  --number-of-workers 2 \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }'
```

> **Do NOT use `--enable-glue-datacatalog`** in the default arguments. This flag injects Hive metastore bindings that override your explicit `glue.region` and `glue.endpoint` Spark catalog config, causing `register_table` and other catalog operations to target the wrong region. The script configures the Iceberg catalog directly via SparkSession.
>
> Adjust `--number-of-workers` based on table sizes. For cross-region transfers with large tables (100M+ rows), consider `G.2X` workers or increasing the count.

---

## 3. Run the Job

### Dry-run (recommended first step)

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --arguments '{
    "--SOURCE_REGION": "us-west-2",
    "--DEST_REGION": "us-east-1",
    "--DATABASE": "jpmc_demo_warehouse",
    "--TABLES": "events,main_concur_table,main_sample_table_perf_20260403,main_sample_table_v2,orders,orders_pftest_small_datepart",
    "--SHARED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-east-1",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "true",
    "--DRYRUN": "true"
  }'
```

### Full migration (specific tables, skip cleanup)

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --arguments '{
    "--SOURCE_REGION": "us-west-2",
    "--DEST_REGION": "us-east-1",
    "--DATABASE": "jpmc_demo_warehouse",
    "--TABLES": "events,main_concur_table,main_sample_table_perf_20260403,main_sample_table_v2,orders,orders_pftest_small_datepart",
    "--SHARED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-east-1",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "true",
    "--DRYRUN": "false"
  }'
```

### Full migration (all tables in database)

Omit `--TABLES` to migrate all tables in the database:

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --arguments '{
    "--SOURCE_REGION": "us-west-2",
    "--DEST_REGION": "us-east-1",
    "--DATABASE": "jpmc_demo_warehouse",
    "--SHARED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-east-1",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "false",
    "--DRYRUN": "false"
  }'
```

### With custom Glue endpoints (e.g., VPC endpoints)

```bash
aws glue start-job-run \
  --job-name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --arguments '{
    "--SOURCE_REGION": "us-west-2",
    "--DEST_REGION": "us-east-1",
    "--SOURCE_GLUE_ENDPOINT": "https://glue.us-west-2.amazonaws.com",
    "--DEST_GLUE_ENDPOINT": "https://glue.us-east-1.amazonaws.com",
    "--DATABASE": "jpmc_demo_warehouse",
    "--TABLES": "events,orders",
    "--SHARED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-west-2",
    "--SHARED_PREFIX": "warehouse",
    "--DEDICATED_BUCKET": "jpmc-iceberg-warehouse-038676220235-us-east-1",
    "--DEDICATED_PREFIX": "warehouse",
    "--SKIP_CLEANUP": "false",
    "--DRYRUN": "false"
  }'
```

---

## 4. Monitor the Job Run

```bash
# Get the run ID from the start-job-run output, then:
RUN_ID="jr_xxxxxxxxxxxxxxxx"

aws glue get-job-run \
  --job-name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --run-id "${RUN_ID}"
```

Check CloudWatch Logs in **us-east-1** at:
```
/aws-glue/jobs/output   — stdout/logger output
/aws-glue/jobs/error    — stderr/exceptions
```

---

## 5. Update an Existing Job

```bash
SCRIPT_BUCKET="jpmc-iceberg-warehouse-038676220235-us-east-1"

aws glue update-job \
  --job-name "iceberg-migration-job-xr" \
  --region us-east-1 \
  --job-update '{
    "Command": {
      "Name": "glueetl",
      "ScriptLocation": "s3://'"${SCRIPT_BUCKET}"'/glue-scripts/iceberg_migration_job_xr.py",
      "PythonVersion": "3"
    },
    "NumberOfWorkers": 8,
    "WorkerType": "G.2X"
  }'
```

---

## 6. Delete the Job (when no longer needed)

```bash
aws glue delete-job --job-name "iceberg-migration-job-xr" --region us-east-1
```

---

## Job Parameters Reference

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `SOURCE_REGION` | Yes | `us-west-2` | AWS region of the source Glue Catalog and S3 bucket |
| `DEST_REGION` | Yes | `us-east-1` | AWS region of the destination Glue Catalog and S3 bucket |
| `SOURCE_GLUE_ENDPOINT` | No | Auto-derived | Source Glue endpoint URL (useful for VPC endpoints) |
| `DEST_GLUE_ENDPOINT` | No | Auto-derived | Destination Glue endpoint URL (useful for VPC endpoints) |
| `DATABASE` | Yes | — | Glue Catalog database name (same name used in both regions) |
| `TABLES` | No | All tables | Comma-separated list of Iceberg table names to migrate |
| `SHARED_BUCKET` | Yes | — | Source S3 bucket name (without `s3://` prefix) |
| `SHARED_PREFIX` | Yes | — | S3 prefix in the source bucket |
| `DEDICATED_BUCKET` | Yes | — | Destination S3 bucket name (without `s3://` prefix) |
| `DEDICATED_PREFIX` | Yes | — | S3 prefix in the destination bucket |
| `SKIP_CLEANUP` | Yes | — | `"true"` to skip snapshot expiration and orphan file removal |
| `DRYRUN` | Yes | — | `"true"` to log actions without executing mutations |

---

## Migration Pipeline Steps

1. **Ensure database** — Creates the database in the destination Glue Catalog if it doesn't exist
2. **Resolve metadata** — Reads the latest `metadata_location` from each table in the **source** Glue Catalog
3. **Register tables** — Drops (if exists) and re-registers each table in the **destination** Glue Catalog pointing to the new S3 location
4. **Rewrite data files** — Rewrites Parquet/data files so internal references use the destination bucket paths
5. **Update table location** — Patches the destination Glue Catalog `StorageDescriptor.Location` and `metadata_location`
6. **Cleanup** — Expires old snapshots and removes orphan files (skippable)
7. **Validate** — Logs row counts per table from the destination catalog as a sanity check

---

## IAM Role Requirements

The Glue execution role needs cross-region access. Example policy additions:

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetDatabase",
    "glue:GetTable",
    "glue:GetTables"
  ],
  "Resource": [
    "arn:aws:glue:us-west-2:038676220235:catalog",
    "arn:aws:glue:us-west-2:038676220235:database/jpmc_demo_warehouse",
    "arn:aws:glue:us-west-2:038676220235:table/jpmc_demo_warehouse/*"
  ]
},
{
  "Effect": "Allow",
  "Action": "glue:*",
  "Resource": [
    "arn:aws:glue:us-east-1:038676220235:catalog",
    "arn:aws:glue:us-east-1:038676220235:database/jpmc_demo_warehouse",
    "arn:aws:glue:us-east-1:038676220235:table/jpmc_demo_warehouse/*"
  ]
},
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::jpmc-iceberg-warehouse-038676220235-us-west-2",
    "arn:aws:s3:::jpmc-iceberg-warehouse-038676220235-us-west-2/*"
  ]
},
{
  "Effect": "Allow",
  "Action": "s3:*",
  "Resource": [
    "arn:aws:s3:::jpmc-iceberg-warehouse-038676220235-us-east-1",
    "arn:aws:s3:::jpmc-iceberg-warehouse-038676220235-us-east-1/*"
  ]
}
```

---

## Recommended Workflow

1. **Copy data first** — Use S3 Batch Replication or `s3_copy_job.py` to replicate table data from source to destination bucket
2. **Dry-run** — Run with `--DRYRUN true` to verify table resolution, path mapping, and database creation
3. **Migrate without cleanup** — Run with `--SKIP_CLEANUP true` and `--DRYRUN false` to register tables (allows rollback)
4. **Validate** — Check row counts in the logs, query tables in Athena from the destination region
5. **Cleanup** — Run again with `--SKIP_CLEANUP false` to expire old snapshots and remove orphan files
6. **Decommission source** — Once validated, remove or archive the source tables

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `AccessDeniedException` on source Glue | IAM role missing cross-region Glue read permissions | Add source region Glue ARNs to the role policy |
| `EntityNotFoundException` for database | Database doesn't exist in destination | The job creates it automatically; check logs for creation errors |
| `metadata_location not found` | Table is not a registered Iceberg table | Verify source tables are Iceberg format, not Hive |
| Slow `rewrite_data_files` | Cross-region S3 reads | Ensure data was pre-copied to destination bucket before running |
| `NoSuchBucket` on dedicated bucket | Destination bucket doesn't exist or wrong region | Create the bucket in the destination region first |
| Table not visible in destination Glue catalog | `--enable-glue-datacatalog` overriding explicit catalog config | Remove `--enable-glue-datacatalog` from job default arguments; the script configures the catalog directly |
| Table registered but broken/empty | Metadata file not yet copied to destination bucket | Ensure S3 copy (including `/metadata/` folder) completes before running the migration job |
| `rewrite_data_files` fails cross-region | S3FileIO can't read from a bucket in a different region | The script sets `s3.cross-region-access-enabled=true`; verify Glue 5.0 has Iceberg ≥ 1.4.0 (it does) |
