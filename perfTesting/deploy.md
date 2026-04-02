# Deploy: Iceberg Synthetic Data Generator Glue Job

## Prerequisites

- AWS CLI configured with appropriate credentials
- An S3 bucket for Glue script storage and the Iceberg warehouse
- An IAM role for Glue with permissions for S3, Glue Catalog, and CloudWatch Logs

## 1. Upload the script to S3

```bash
S3_BUCKET="your-bucket-name"
aws s3 cp perfTesting/iceberg_synthetic_data_generator.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_synthetic_data_generator.py
```

## 2. Create the Glue job

```bash
ROLE_ARN="arn:aws:iam::123456789012:role/your-glue-role"
S3_BUCKET="your-bucket-name"

aws glue create-job \
  --name iceberg-synthetic-data-generator \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/iceberg_synthetic_data_generator.py",
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

Adjust `--number-of-workers` and `--worker-type` based on your `TARGET_ROWS`:

| Target Rows | Workers | Worker Type |
|---|---|---|
| < 1M | 5 | G.1X |
| 1M – 100M | 10 | G.1X |
| 100M – 1B | 20 | G.2X |
| > 1B | 40+ | G.2X |

## 3. Run the job

```bash
aws glue start-job-run \
  --job-name iceberg-synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "perf_test_db",
    "--SOURCE_TABLE": "my_source_table",
    "--TARGET_TABLE": "my_target_table",
    "--TARGET_PARTITION": "month(event_ts), bucket(8, user_id)",
    "--TARGET_ROWS": "10000000"
  }'
```

This returns a `JobRunId`. Use it to monitor progress.

## 4. Monitor the run

```bash
JOB_RUN_ID="jr_xxxxxxxxxxxxx"

# Check status
aws glue get-job-run \
  --job-name iceberg-synthetic-data-generator \
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

## 5. Update the script after changes

```bash
# Re-upload the script — Glue picks up the latest version on next run
aws s3 cp perfTesting/iceberg_synthetic_data_generator.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_synthetic_data_generator.py
```

## 6. Cleanup

```bash
# Delete the job
aws glue delete-job --job-name iceberg-synthetic-data-generator

# Remove the script from S3
aws s3 rm s3://${S3_BUCKET}/glue-scripts/iceberg_synthetic_data_generator.py
```

## Notes

- The `--datalake-formats iceberg` argument in default-arguments tells Glue 4.0 to bundle the Iceberg runtime JARs automatically — no need to supply them manually.
- The partition spec in `--TARGET_PARTITION` supports any valid Iceberg partition transform: `identity`, `bucket(N, col)`, `truncate(N, col)`, `year()`, `month()`, `day()`, `hour()`, or comma-separated composites.
- If the target table already exists it will be dropped and recreated.
