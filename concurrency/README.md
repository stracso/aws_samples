# Iceberg Concurrency Testing — AWS Glue

Test concurrent data loading into Apache Iceberg tables using AWS Glue jobs.

## Overview

| File | Purpose |
|------|---------|
| `glue_data_loader.py` | Glue job script — single data loader (generate → stage → validate → upsert) |
| `run_concurrency_test.py` | Orchestrator — launches N loaders concurrently and monitors results |
| `glue-role-trust-policy.json` | IAM trust policy for the Glue execution role |
| `glue-role-policy.json` | IAM permissions policy (S3, Glue Catalog, CloudWatch) |
| `iceberg_concurrency_strategy.md` | Full strategy document with architecture details |

## Prerequisites

- AWS CLI v2 configured with appropriate credentials
- Python 3.9+ with `boto3` installed (for the orchestrator)
- An S3 bucket for the Iceberg warehouse and Glue scripts

## Setup Variables

Set these once for all commands below:

```bash
export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export S3_BUCKET="your-iceberg-warehouse-bucket"
export SCRIPTS_PREFIX="glue-scripts/concurrency"
export WAREHOUSE_PATH="s3://${S3_BUCKET}/warehouse/"
export DATABASE="concurrency_test_db"
export MAIN_TABLE="main_table"
export GLUE_ROLE_NAME="GlueIcebergConcurrencyRole"
export GLUE_JOB_NAME="iceberg-concurrency-loader"
```

---

## Step 1: Create the S3 Bucket (if needed)

```bash
aws s3 mb s3://${S3_BUCKET} --region ${AWS_REGION}
```

## Step 2: Upload the Glue Script

```bash
aws s3 cp glue_data_loader.py s3://${S3_BUCKET}/${SCRIPTS_PREFIX}/glue_data_loader.py
```

## Step 3: Create the IAM Role

```bash
# Create the role
aws iam create-role \
  --role-name ${GLUE_ROLE_NAME} \
  --assume-role-policy-document file://glue-role-trust-policy.json \
  --description "Role for Iceberg concurrency test Glue jobs"

# Attach the custom policy (edit glue-role-policy.json first — replace YOUR_BUCKET_NAME)
sed -i '' "s/YOUR_BUCKET_NAME/${S3_BUCKET}/g" glue-role-policy.json

aws iam put-role-policy \
  --role-name ${GLUE_ROLE_NAME} \
  --policy-name GlueIcebergConcurrencyPolicy \
  --policy-document file://glue-role-policy.json

# Attach the AWS managed Glue service policy
aws iam attach-role-policy \
  --role-name ${GLUE_ROLE_NAME} \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Wait for IAM propagation
echo "Waiting 10s for IAM propagation..."
sleep 10
```

## Step 4: Create the Glue Job

```bash
aws glue create-job \
  --name ${GLUE_JOB_NAME} \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE_NAME}" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'"${S3_BUCKET}"'/'"${SCRIPTS_PREFIX}"'/glue_data_loader.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://'"${S3_BUCKET}"'/warehouse/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--loader_id": "default",
    "--source_system": "default",
    "--database": "'"${DATABASE}"'",
    "--main_table": "'"${MAIN_TABLE}"'",
    "--warehouse_path": "s3://'"${S3_BUCKET}"'/warehouse/",
    "--num_rows": "10000",
    "--overlap_mode": "isolated",
    "--commit_retries": "5"
  }' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --region ${AWS_REGION}
```

> **Note:** `--datalake-formats iceberg` tells Glue 4.0 to include the Iceberg libraries automatically.

---

## Step 5: Run Concurrency Tests

### Test A: Isolated Partitions (No Conflicts Expected)

Each loader writes to its own partition — validates that parallel writes succeed without retries.

```bash
python run_concurrency_test.py \
  --job-name ${GLUE_JOB_NAME} \
  --database ${DATABASE} \
  --main-table ${MAIN_TABLE} \
  --warehouse-path ${WAREHOUSE_PATH} \
  --num-loaders 3 \
  --num-rows 10000 \
  --test-mode isolated
```

**Expected outcome:** All 3 loaders succeed on first attempt, no retries.

### Test B: Overlapping Partitions (Conflicts Expected)

All loaders write to the same partition — forces commit conflicts and tests retry logic.

```bash
python run_concurrency_test.py \
  --job-name ${GLUE_JOB_NAME} \
  --database ${DATABASE} \
  --main-table ${MAIN_TABLE} \
  --warehouse-path ${WAREHOUSE_PATH} \
  --num-loaders 3 \
  --num-rows 5000 \
  --test-mode overlapping \
  --commit-retries 8
```

**Expected outcome:** Some loaders retry 1-3 times, all eventually succeed.

### Test C: Mixed Mode

Half the loaders are isolated, half overlap — simulates a realistic mixed workload.

```bash
python run_concurrency_test.py \
  --job-name ${GLUE_JOB_NAME} \
  --database ${DATABASE} \
  --main-table ${MAIN_TABLE} \
  --warehouse-path ${WAREHOUSE_PATH} \
  --num-loaders 6 \
  --num-rows 10000 \
  --test-mode mixed
```

**Expected outcome:** Isolated loaders succeed immediately; overlapping loaders may retry.

### Test D: High Concurrency Stress Test

Push the system with 10+ concurrent loaders to find the breaking point.

```bash
python run_concurrency_test.py \
  --job-name ${GLUE_JOB_NAME} \
  --database ${DATABASE} \
  --main-table ${MAIN_TABLE} \
  --warehouse-path ${WAREHOUSE_PATH} \
  --num-loaders 10 \
  --num-rows 5000 \
  --test-mode overlapping \
  --commit-retries 10
```

---

## Step 6: Validate Data Integrity (Post-Test)

After tests complete, query the table via Athena or a Spark session:

```sql
-- Total row count
SELECT count(*) FROM concurrency_test_db.main_table;

-- Rows per source system
SELECT source_system, count(*) as cnt
FROM concurrency_test_db.main_table
GROUP BY source_system
ORDER BY source_system;

-- Check for duplicate primary keys
SELECT id, source_system, count(*) as cnt
FROM concurrency_test_db.main_table
GROUP BY id, source_system
HAVING cnt > 1;

-- View commit history (snapshots)
SELECT snapshot_id, committed_at, operation, summary
FROM concurrency_test_db.main_table.snapshots
ORDER BY committed_at DESC;

-- Check for orphan files
SELECT * FROM concurrency_test_db.main_table.all_data_files
WHERE content = 0;
```

---

## Step 7: Run a Single Loader Manually (Debugging)

To test a single loader in isolation:

```bash
aws glue start-job-run \
  --job-name ${GLUE_JOB_NAME} \
  --arguments '{
    "--loader_id": "manual_test_01",
    "--source_system": "manual_system",
    "--database": "'"${DATABASE}"'",
    "--main_table": "'"${MAIN_TABLE}"'",
    "--warehouse_path": "'"${WAREHOUSE_PATH}"'",
    "--num_rows": "1000",
    "--overlap_mode": "isolated",
    "--commit_retries": "5"
  }'
```

Monitor the run:

```bash
# Get the run ID from the start-job-run output, then:
RUN_ID="jr_xxxxx"

aws glue get-job-run \
  --job-name ${GLUE_JOB_NAME} \
  --run-id ${RUN_ID} \
  --query 'JobRun.{Status:JobRunState,Duration:ExecutionTime,Error:ErrorMessage}'
```

View logs:

```bash
# Glue job logs are in CloudWatch
aws logs get-log-events \
  --log-group-name "/aws-glue/jobs/output" \
  --log-stream-name "${RUN_ID}" \
  --limit 50
```

---

## Step 8: Cleanup

### Reset the test table (drop and recreate)

```bash
# Run a quick Glue job that just drops the table
aws glue start-job-run \
  --job-name ${GLUE_JOB_NAME} \
  --arguments '{
    "--loader_id": "cleanup",
    "--source_system": "cleanup",
    "--database": "'"${DATABASE}"'",
    "--main_table": "'"${MAIN_TABLE}"'",
    "--warehouse_path": "'"${WAREHOUSE_PATH}"'",
    "--num_rows": "1",
    "--overlap_mode": "isolated",
    "--commit_retries": "1"
  }'
```

Or drop via Athena/Spark:

```sql
DROP TABLE IF EXISTS concurrency_test_db.main_table;
DROP DATABASE IF EXISTS concurrency_test_db;
```

### Delete all AWS resources

```bash
# Delete the Glue job
aws glue delete-job --job-name ${GLUE_JOB_NAME}

# Delete the IAM role
aws iam detach-role-policy \
  --role-name ${GLUE_ROLE_NAME} \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam delete-role-policy \
  --role-name ${GLUE_ROLE_NAME} \
  --policy-name GlueIcebergConcurrencyPolicy

aws iam delete-role --role-name ${GLUE_ROLE_NAME}

# Delete scripts from S3
aws s3 rm s3://${S3_BUCKET}/${SCRIPTS_PREFIX}/ --recursive

# Delete warehouse data (CAUTION: removes all test data)
aws s3 rm s3://${S3_BUCKET}/warehouse/${DATABASE}/ --recursive
```

---

## Architecture Diagram

```
                    run_concurrency_test.py (local)
                    ┌──────────────────────────┐
                    │  Orchestrator            │
                    │  • Generates configs     │
                    │  • Launches N Glue jobs  │
                    │  • Polls status          │
                    │  • Reports results       │
                    └────────────┬─────────────┘
                                 │ start_job_run() × N
                                 ▼
              ┌──────────────────────────────────────┐
              │         AWS Glue (concurrent runs)    │
              ├──────────┬──────────┬────────────────┤
              │ Loader 0 │ Loader 1 │ ... Loader N-1 │
              │          │          │                │
              │ generate │ generate │ generate       │
              │ → stage  │ → stage  │ → stage        │
              │ validate │ validate │ validate       │
              │ MERGE ↓  │ MERGE ↓  │ MERGE ↓        │
              └────┬─────┴────┬─────┴────┬───────────┘
                   │          │          │
                   ▼          ▼          ▼
              ┌──────────────────────────────────────┐
              │    Iceberg Main Table (S3 + Glue)    │
              │    ┌─────────┬─────────┬──────────┐  │
              │    │ part=A  │ part=B  │ part=C   │  │
              │    └─────────┴─────────┴──────────┘  │
              └──────────────────────────────────────┘
```

---

## Tuning Tips

| Parameter | Default | When to Increase |
|-----------|---------|-----------------|
| `--num-workers` (Glue job) | 2 | Large datasets (>100K rows) |
| `--commit-retries` | 5 | High concurrency (>5 overlapping loaders) |
| `--num-rows` | 10000 | Stress testing write throughput |
| `--poll-interval` | 30s | Reduce for faster feedback (min ~15s) |

### Glue Job Concurrency Limit

By default, Glue jobs have a max concurrency of 1. Increase it:

```bash
aws glue update-job \
  --job-name ${GLUE_JOB_NAME} \
  --job-update '{
    "ExecutionProperty": {
      "MaxConcurrentRuns": 20
    }
  }'
```

This is **required** before running the concurrency tests with multiple loaders.
