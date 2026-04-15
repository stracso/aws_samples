# Deploy: Iceberg Partition Performance Test v2 — Glue Job

## Prerequisites

- AWS CLI configured with appropriate credentials
- An S3 bucket for Glue script storage and results output
- An IAM role for Glue with permissions for S3, Glue Catalog, and CloudWatch Logs
- Pre-existing Iceberg tables for each partition strategy you want to test
- For INSERT tests: a pre-existing Iceberg table containing the source/input data

## 1. Upload the script to S3

```bash
S3_BUCKET="your-bucket-name"

aws s3 cp perfTesting/iceberg_partition_perf_test_v2.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_partition_perf_test_v2.py
```

## 2. Create the Glue job

```bash
ROLE_ARN="arn:aws:iam::123456789012:role/your-glue-role"
S3_BUCKET="your-bucket-name"

aws glue create-job \
  --name iceberg-partition-perf-test-v2 \
  --role "${ROLE_ARN}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/glue-scripts/iceberg_partition_perf_test_v2.py",
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

Adjust `--number-of-workers` and `--worker-type` based on your dataset size:

| Dataset Size | Workers | Worker Type |
|---|---|---|
| < 1M rows | 5 | G.1X |
| 1M – 100M | 10 | G.1X |
| 100M – 1B | 20 | G.2X |
| > 1B | 40+ | G.2X |

## 3. Run INSERT tests

Provide `--TEST_SCOPE insert`, the `--INPUT_TABLE` with source data, and one or more `--TABLE_<STRATEGY>` parameters for the target tables.

```bash
aws glue start-job-run \
  --job-name iceberg-partition-perf-test-v2 \
  --arguments '{
    "--TEST_SCOPE": "insert",
    "--DATABASE_NAME": "perf_test_db",
    "--INPUT_TABLE": "glue_catalog.perf_test_db.source_data",
    "--TABLE_UNPARTITIONED": "glue_catalog.perf_test_db.perf_unpart",
    "--TABLE_IDENTITY": "glue_catalog.perf_test_db.perf_identity",
    "--PARTITION_COL_IDENTITY": "created_date",
    "--TABLE_BUCKET": "glue_catalog.perf_test_db.perf_bucket",
    "--PARTITION_COL_BUCKET": "bucket(16, id)",
    "--TABLE_TIME_BASED": "glue_catalog.perf_test_db.perf_monthly",
    "--PARTITION_COL_TIME_BASED": "month(event_ts)",
    "--TABLE_COMPOSITE": "glue_catalog.perf_test_db.perf_composite",
    "--PARTITION_COL_COMPOSITE": "month(event_ts), bucket(8, user_id)",
    "--RESULTS_PATH": "s3://'"${S3_BUCKET}"'/perf_results/"
  }'
```

> Each insert test case (I-1 through I-5) clears the target table before running, so the table must already exist with the correct schema and partition spec.

## 4. Run QUERY tests

Provide `--TEST_SCOPE query` and the same table parameters. No `--INPUT_TABLE` is needed — the tables must already contain data (e.g. from a prior insert run).

```bash
aws glue start-job-run \
  --job-name iceberg-partition-perf-test-v2 \
  --arguments '{
    "--TEST_SCOPE": "query",
    "--DATABASE_NAME": "perf_test_db",
    "--TABLE_UNPARTITIONED": "glue_catalog.perf_test_db.perf_unpart",
    "--TABLE_IDENTITY": "glue_catalog.perf_test_db.perf_identity",
    "--PARTITION_COL_IDENTITY": "created_date",
    "--TABLE_BUCKET": "glue_catalog.perf_test_db.perf_bucket",
    "--PARTITION_COL_BUCKET": "bucket(16, id)",
    "--TABLE_TIME_BASED": "glue_catalog.perf_test_db.perf_monthly",
    "--PARTITION_COL_TIME_BASED": "month(event_ts)",
    "--TABLE_COMPOSITE": "glue_catalog.perf_test_db.perf_composite",
    "--PARTITION_COL_COMPOSITE": "month(event_ts), bucket(8, user_id)",
    "--RESULTS_PATH": "s3://'"${S3_BUCKET}"'/perf_results/"
  }'
```

### Optional query filter overrides

You can override the auto-derived filters for Q-1, Q-2, and Q-4:

```bash
    "--QUERY_PARTITION_FILTER": "created_date = DATE '2022-07-15'",
    "--QUERY_RANGE_FILTER": "event_ts BETWEEN TIMESTAMP '2022-06-01 00:00:00' AND TIMESTAMP '2022-06-30 23:59:59'",
    "--QUERY_AGG_COLUMN": "region"
```

If omitted, the script derives sensible defaults from each strategy's partition expression.

## 5. Monitor the run

```bash
JOB_RUN_ID="jr_xxxxxxxxxxxxx"

# Check status
aws glue get-job-run \
  --job-name iceberg-partition-perf-test-v2 \
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

## 6. Update the script after changes

```bash
# Re-upload — Glue picks up the latest version on next run
aws s3 cp perfTesting/iceberg_partition_perf_test_v2.py \
  s3://${S3_BUCKET}/glue-scripts/iceberg_partition_perf_test_v2.py
```

## 7. Cleanup

```bash
# Delete the job
aws glue delete-job --job-name iceberg-partition-perf-test-v2

# Remove the script from S3
aws s3 rm s3://${S3_BUCKET}/glue-scripts/iceberg_partition_perf_test_v2.py
```

## Parameter Reference

| Parameter | Required | Scope | Description |
|---|---|---|---|
| `--TEST_SCOPE` | Yes | Both | `insert` or `query` |
| `--DATABASE_NAME` | No | Both | Glue catalog database (default: `perf_test_db`) |
| `--RESULTS_PATH` | Yes | Both | S3 path for JSON results output |
| `--INPUT_TABLE` | Yes (insert) | Insert | Fully-qualified Iceberg table with source data |
| `--TABLE_UNPARTITIONED` | No* | Both | Target table for unpartitioned strategy |
| `--TABLE_IDENTITY` | No* | Both | Target table for identity partition strategy |
| `--TABLE_BUCKET` | No* | Both | Target table for bucket partition strategy |
| `--TABLE_TRUNCATE` | No* | Both | Target table for truncate partition strategy |
| `--TABLE_TIME_BASED` | No* | Both | Target table for time-based partition strategy |
| `--TABLE_COMPOSITE` | No* | Both | Target table for composite partition strategy |
| `--PARTITION_COL_IDENTITY` | No | Both | Partition expression (default: `created_date`) |
| `--PARTITION_COL_BUCKET` | No | Both | Partition expression (default: `bucket(16, id)`) |
| `--PARTITION_COL_TRUNCATE` | No | Both | Partition expression (default: `truncate(10, id)`) |
| `--PARTITION_COL_TIME_BASED` | No | Both | Partition expression (default: `month(event_ts)`) |
| `--PARTITION_COL_COMPOSITE` | No | Both | Partition expression (default: `month(event_ts), bucket(8, user_id)`) |
| `--QUERY_PARTITION_FILTER` | No | Query | Override for Q-1 point lookup WHERE clause |
| `--QUERY_RANGE_FILTER` | No | Query | Override for Q-2 range scan WHERE clause |
| `--QUERY_AGG_COLUMN` | No | Query | Override for Q-4 GROUP BY column |

\* At least one `--TABLE_<STRATEGY>` must be provided. Only strategies with a table name will be tested.

## Notes

- The `--datalake-formats iceberg` default argument tells Glue 4.0 to bundle Iceberg runtime JARs automatically.
- All target tables must be pre-created with the correct schema and partition spec before running the job.
- For INSERT tests, each test case (I-1 through I-5) clears the target table via `DELETE FROM` before executing, so results are isolated per test case.
- For QUERY tests, the script optionally runs `rewrite_data_files` compaction before querying for a fair comparison.
- Results are written as JSON to `<RESULTS_PATH>/<insert|query>/<timestamp>/`.
