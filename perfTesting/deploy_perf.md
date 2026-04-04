# Deploy: Iceberg Partition Performance Test Glue Job

## Prerequisites

- AWS CLI configured with appropriate credentials
- An S3 bucket for Glue script storage, Iceberg warehouse, and results output
- An IAM role for Glue with permissions for S3, Glue Catalog, and CloudWatch Logs

## 1. Upload the script to S3

```bash
S3_BUCKET="your-bucket-name"
aws s3 cp perfTesting/iceberg_partition_perf_test.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_partition_perf_test.py
```

## 2. Create the Glue job

```bash
ROLE_ARN="arn:aws:iam::123456789012:role/your-glue-role"
S3_BUCKET="your-bucket-name"

aws glue create-job \
  --name iceberg-partition-perf-test \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/iceberg_partition_perf_test.py",
    "PythonVersion": "3"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type "G.1X" \
  --default-arguments '{
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
    "--enable-metrics": "true"
  }'
```

Scale workers based on the dataset size you plan to test:

| DATASET_SIZE | Rows | Workers | Worker Type |
|---|---|---|---|
| small | 1,000,000 | 5 | G.1X |
| medium | 100,000,000 | 20 | G.2X |
| large | 1,000,000,000 | 40+ | G.2X |

## 3. Run the job

```bash
S3_BUCKET="your-bucket-name"

aws glue start-job-run \
  --job-name iceberg-partition-perf-test \
  --arguments '{
    "--DATABASE_NAME": "perf_test_db",
    "--S3_BUCKET": "'"${S3_BUCKET}"'",
    "--DATASET_SIZE": "small",
    "--TEST_SCOPE": "all",
    "--RESULTS_PATH": "s3://'"${S3_BUCKET}"'/perf_results/",
    "--WAREHOUSE_PATH": "s3://'"${S3_BUCKET}"'/iceberg_warehouse/"
  }'
```

### Job parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `--DATABASE_NAME` | No | `perf_test_db` | Glue catalog database name |
| `--S3_BUCKET` | Yes | — | S3 bucket for table storage |
| `--DATASET_SIZE` | No | `small` | `small` / `medium` / `large` |
| `--TEST_SCOPE` | No | `all` | `all` / `insert` / `query` |
| `--RESULTS_PATH` | No | `s3://<bucket>/perf_results/` | S3 path for results output |
| `--WAREHOUSE_PATH` | No | `s3://<bucket>/iceberg_warehouse/` | S3 warehouse path |

### Test scope options

- `all` — runs both insert and query benchmarks across all 8 partition strategies
- `insert` — runs only insert tests (I-1 through I-5: bulk, incremental, out-of-order, upsert, concurrent)
- `query` — runs only query tests (Q-1 through Q-7: point lookup, time range, partition-aligned, cross-partition, aggregation, join, full scan)

## 4. Monitor the run

```bash
JOB_RUN_ID="jr_xxxxxxxxxxxxx"

# Check status
aws glue get-job-run \
  --job-name iceberg-partition-perf-test \
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
# Re-upload — Glue picks up the latest version on next run
aws s3 cp perfTesting/iceberg_partition_perf_test.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_partition_perf_test.py
```

## 6. Cleanup

```bash
# Delete the job
aws glue delete-job --job-name iceberg-partition-perf-test

# Remove the script from S3
aws s3 rm s3://${S3_BUCKET}/glue-scripts/iceberg_partition_perf_test.py
```

## Notes

- The `--datalake-formats iceberg` default argument tells Glue 4.0 to bundle the Iceberg runtime JARs automatically.
- The job tests 8 partition strategies: unpartitioned, identity (date/region), bucket, truncate, month, day, and composite.
- Results are written as JSON to the `RESULTS_PATH` under `<dataset_size>/<timestamp>/`.
- For `query`-only runs, if a table doesn't exist yet the job will create it and insert data before running queries.
- Large dataset runs can take several hours. Consider starting with `small` to validate the setup.
