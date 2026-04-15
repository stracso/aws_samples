# Deploy: Iceberg Synthetic Data Pipeline — Glue Jobs

This guide deploys and runs two Glue jobs that together generate synthetic data
and persist it into an Iceberg table:

1. **synthetic-data-generator** — reads a source Iceberg table's schema, creates
   a target table with a custom partition spec, generates synthetic rows, and
   writes them to a staging Parquet dataset on S3.
2. **iceberg-persist-synthetic-data** — reads the staged Parquet and inserts it
   into the target Iceberg table in batches.

---

## Prerequisites

- AWS CLI configured with appropriate credentials
- An S3 bucket for Glue script storage, the Iceberg warehouse, and staging data
- An IAM role for Glue with permissions for S3, Glue Catalog, and CloudWatch Logs

## Shared Variables

Set these once and reuse throughout:

```bash
S3_BUCKET="your-bucket-name"
ROLE_ARN="arn:aws:iam::123456789012:role/your-glue-role"
STAGING_PATH="s3://${S3_BUCKET}/staging"
```

---

## 1. Upload Scripts to S3

```bash
aws s3 cp perfTesting/synthetic_data_generator.py \
  s3://${S3_BUCKET}/glue-scripts/synthetic_data_generator.py

aws s3 cp perfTesting/iceberg_persist_synthetic_data.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_persist_synthetic_data.py
```

---

## 2. Create the Glue Jobs

### 2a. synthetic-data-generator

```bash
aws glue create-job \
  --name synthetic-data-generator \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/synthetic_data_generator.py",
    "PythonVersion": "3"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type "G.1X" \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--enable-metrics": "true"
  }'
```

### 2b. iceberg-persist-synthetic-data

```bash
aws glue create-job \
  --name iceberg-persist-synthetic-data \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/iceberg_persist_synthetic_data.py",
    "PythonVersion": "3"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type "G.1X" \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--enable-metrics": "true"
  }'
```

### Worker Sizing Guide

Adjust `--number-of-workers` and `--worker-type` based on your `TARGET_ROWS`:

| Target Rows | Workers | Worker Type |
|---|---|---|
| < 1M | 5 | G.1X |
| 1M – 100M | 10 | G.1X |
| 100M – 1B | 20 | G.2X |
| > 1B | 40+ | G.2X |

Use the same sizing for both jobs.

---

## 3. Run the Pipeline

### Step 1 — Generate synthetic data

```bash
aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "perf_test_db",
    "--SOURCE_TABLE": "my_source_table",
    "--TARGET_TABLE": "my_target_table",
    "--TARGET_PARTITION": "month(event_ts), bucket(8, user_id)",
    "--TARGET_ROWS": "10000000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'
```

Wait for this job to complete before starting Step 2 — the persist job reads
the staging Parquet written by the generator.

### Step 2 — Persist into Iceberg

```bash
aws glue start-job-run \
  --job-name iceberg-persist-synthetic-data \
  --arguments '{
    "--DATABASE_NAME": "perf_test_db",
    "--TARGET_TABLE": "my_target_table",
    "--TARGET_ROWS": "10000000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'
```

Both commands return a `JobRunId`. Use it to monitor progress (see below).

---

## 4. Monitor Runs

```bash
JOB_RUN_ID="jr_xxxxxxxxxxxxx"
JOB_NAME="synthetic-data-generator"  # or "iceberg-persist-synthetic-data"

aws glue get-job-run \
  --job-name "${JOB_NAME}" \
  --run-id "${JOB_RUN_ID}" \
  --query 'JobRun.{Status:JobRunState,Started:StartedOn,Duration:ExecutionTime,Error:ErrorMessage}'
```

Stream logs via CloudWatch:

```bash
# Driver logs
aws logs tail /aws-glue/jobs/output --follow

# Error logs
aws logs tail /aws-glue/jobs/error --follow
```

---

## 5. Update Scripts After Changes

Re-upload the scripts — Glue picks up the latest version on the next run:

```bash
aws s3 cp perfTesting/synthetic_data_generator.py \
  s3://${S3_BUCKET}/glue-scripts/synthetic_data_generator.py

aws s3 cp perfTesting/iceberg_persist_synthetic_data.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_persist_synthetic_data.py
```

---

## 6. Cleanup

```bash
# Delete both jobs
aws glue delete-job --job-name synthetic-data-generator
aws glue delete-job --job-name iceberg-persist-synthetic-data

# Remove scripts from S3
aws s3 rm s3://${S3_BUCKET}/glue-scripts/synthetic_data_generator.py
aws s3 rm s3://${S3_BUCKET}/glue-scripts/iceberg_persist_synthetic_data.py

# Remove staging data (if not already cleaned by the persist job)
aws s3 rm s3://${STAGING_PATH}/ --recursive
```

---

## Notes

- The `--datalake-formats iceberg` default argument tells Glue 4.0 to bundle
  the Iceberg runtime JARs automatically — no need to supply them manually.
- The partition spec in `--TARGET_PARTITION` supports any valid Iceberg
  partition transform: `identity`, `bucket(N, col)`, `truncate(N, col)`,
  `year()`, `month()`, `day()`, `hour()`, or comma-separated composites.
- The generator writes staging Parquet to `<STAGING_PATH>/<TARGET_TABLE>_parquet_staging`.
  The persist job reads from the same derived path, so `STAGING_PATH` must match
  between the two runs.
- The persist job automatically cleans up the staging Parquet after a successful
  insert. The S3 cleanup step above is only needed if the persist job failed or
  was skipped.
- If the target table already exists, the generator will drop and recreate it.
- The persist job sets `write.distribution-mode = hash` on the target table
  before inserting for better write performance.
