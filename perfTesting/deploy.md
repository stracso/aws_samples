# Deploy: Synthetic Data Generator & Iceberg Persist Glue Jobs

This pipeline uses two Glue jobs:

1. `synthetic-data-generator` — generates synthetic data and writes it as a staging Parquet dataset, and creates the target Iceberg table.
2. `iceberg-persist-synthetic-data` — reads the staged Parquet and inserts into the Iceberg table.

## Prerequisites

- AWS CLI configured with appropriate credentials
- An S3 bucket for Glue script storage and the Iceberg warehouse
- An S3 bucket (or path) for staging Parquet data
- An IAM role for Glue with permissions for S3, Glue Catalog, and CloudWatch Logs

## 1. Upload the scripts to S3

```bash
S3_BUCKET="your-bucket-name"

aws s3 cp perfTesting/synthetic_data_generator.py \
  s3://${S3_BUCKET}/glue-scripts/synthetic_data_generator.py

aws s3 cp perfTesting/iceberg_persist_synthetic_data.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_persist_synthetic_data.py
```

## 2. Create the Glue jobs

### 2a. Synthetic Data Generator

```bash
ROLE_ARN="arn:aws:iam::123456789012:role/your-glue-role"
S3_BUCKET="your-bucket-name"

aws glue create-job \
  --name synthetic-data-generator \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/synthetic_data_generator.py",
    "PythonVersion": "3"
  }' \
  --glue-version "5.0" \
  --number-of-workers 10 \
  --worker-type "G.1X" \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://'"${S3_BUCKET}"'/warehouse --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--write-shuffle-files-to-s3": "true",
    "--TempDir": "s3://custom_shuffle_bucket",
    "--enable-metrics": "true"
  }'
```

### 2b. Iceberg Persist Synthetic Data

```bash
aws glue create-job \
  --name iceberg-persist-synthetic-data \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/iceberg_persist_synthetic_data.py",
    "PythonVersion": "3"
  }' \
  --glue-version "5.0" \
  --number-of-workers 10 \
  --worker-type "G.1X" \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://'"${S3_BUCKET}"'/warehouse --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--write-shuffle-files-to-s3": "true",
    "--TempDir": "s3://custom_shuffle_bucket",
    "--enable-metrics": "true"
  }'
```

Adjust `--number-of-workers` and `--worker-type` based on your `TARGET_ROWS`:

| Target Rows | Workers | Worker Type |
|---|---|---|
| < 1M | 5 | G.1X |
| 1M – 100M | 10 | G.1X |
| 100M – 1B | 20 | G.2X |
| > 1B | 40+ | G.2X |

## 3. Run the jobs

### 3a. Run the Synthetic Data Generator first

```bash
aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "perf_test_db",
    "--SOURCE_TABLE": "my_source_table",
    "--TARGET_TABLE": "my_target_table",
    "--TARGET_PARTITION": "month(event_ts), bucket(8, user_id)",
    "--TARGET_ROWS": "10000000",
    "--STAGING_PATH": "s3://your-staging-bucket/staging"
  }'
```

Wait for this job to complete before running the next one.

### 3b. Run the Iceberg Persist job

```bash
aws glue start-job-run \
  --job-name iceberg-persist-synthetic-data \
  --arguments '{
    "--DATABASE_NAME": "perf_test_db",
    "--TARGET_TABLE": "my_target_table",
    "--TARGET_ROWS": "10000000",
    "--STAGING_PATH": "s3://your-staging-bucket/staging"
  }'
```

Both jobs return a `JobRunId`. Use it to monitor progress.

## 4. Monitor the runs

```bash
JOB_RUN_ID="jr_xxxxxxxxxxxxx"

# Check synthetic data generator status
aws glue get-job-run \
  --job-name synthetic-data-generator \
  --run-id "${JOB_RUN_ID}" \
  --query 'JobRun.{Status:JobRunState,Started:StartedOn,Duration:ExecutionTime,Error:ErrorMessage}'

# Check iceberg persist status
aws glue get-job-run \
  --job-name iceberg-persist-synthetic-data \
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

## 5. Update the scripts after changes

```bash
# Re-upload — Glue picks up the latest version on next run
aws s3 cp perfTesting/synthetic_data_generator.py \
  s3://${S3_BUCKET}/glue-scripts/synthetic_data_generator.py

aws s3 cp perfTesting/iceberg_persist_synthetic_data.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_persist_synthetic_data.py
```

## 6. Cleanup

```bash
# Delete the jobs
aws glue delete-job --job-name synthetic-data-generator
aws glue delete-job --job-name iceberg-persist-synthetic-data

# Remove the scripts from S3
aws s3 rm s3://${S3_BUCKET}/glue-scripts/synthetic_data_generator.py
aws s3 rm s3://${S3_BUCKET}/glue-scripts/iceberg_persist_synthetic_data.py
```

## Notes

- The `--datalake-formats iceberg` argument tells Glue 5.0 to bundle the Iceberg runtime JARs automatically.
- The partition spec in `--TARGET_PARTITION` supports any valid Iceberg partition transform: `identity`, `bucket(N, col)`, `truncate(N, col)`, `year()`, `month()`, `day()`, `hour()`, or comma-separated composites.
- If the target table already exists it will be dropped and recreated by the synthetic data generator job.
- The `STAGING_PATH` must be the same for both jobs. The actual Parquet data is written under `<STAGING_PATH>/<TARGET_TABLE>_parquet_staging/`.
- The persist job cleans up the staging Parquet data after successful insertion.
