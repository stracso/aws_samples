# Iceberg Datalake Migration: Shared Bucket → Dedicated Bucket

## Overview

Migrate an existing Iceberg datalake from a shared S3 bucket to a dedicated S3 bucket using S3 Batch Operations for the data copy and AWS Glue Catalog updates to re-register tables at the new location.

### Why S3 Batch Operations?

- Handles billions of objects with built-in retry, throttling, and progress tracking.
- Runs server-side (no data through your machine).
- Produces a completion report so you can verify every object was copied.

### Migration Scope

| Component | Source | Destination |
|---|---|---|
| Data files (Parquet) | `s3://shared-bucket/<db>/` | `s3://dedicated-bucket/<db>/` |
| Iceberg metadata | `s3://shared-bucket/<db>/<table>/metadata/` | `s3://dedicated-bucket/<db>/<table>/metadata/` |
| Glue Catalog entries | Points to shared bucket | Re-registered to dedicated bucket |

> **Convention used throughout:** replace `shared-bucket`, `dedicated-bucket`, and `market_data` with your actual values.

---

## Prerequisites

- AWS CLI v2 installed and configured.
- IAM permissions for S3, S3 Batch Operations, Glue, and IAM (to create the Batch Ops role).
- The dedicated bucket already created with appropriate bucket policy, encryption, and versioning settings.
- A manifest bucket (can be the dedicated bucket) to store the S3 Inventory or manifest CSV.

---

## Phase 1 — Preparation

### 1.1 Inventory the Source Data

Generate a full object listing of the Iceberg data to migrate. You have two options:

**Option A — S3 Inventory (recommended for large datalakes)**

Enable S3 Inventory on the shared bucket scoped to the datalake prefix. Inventory delivers a manifest CSV daily or weekly.

```bash
aws s3api put-bucket-inventory-configuration \
  --bucket shared-bucket \
  --id datalake-migration-inventory \
  --inventory-configuration '{
    "Id": "datalake-migration-inventory",
    "IsEnabled": true,
    "Destination": {
      "S3BucketDestination": {
        "Bucket": "arn:aws:s3:::dedicated-bucket",
        "Format": "CSV",
        "Prefix": "inventory/"
      }
    },
    "Schedule": { "Frequency": "Daily" },
    "IncludedObjectVersions": "Current",
    "Filter": { "Prefix": "market_data/" },
    "OptionalFields": ["Size", "LastModifiedDate", "ETag"]
  }'
```

Wait for the first inventory delivery (up to 48 hours for the initial run).

**Option B — Generate a manifest with `aws s3 ls` (smaller datalakes)**

```bash
aws s3 ls s3://shared-bucket/market_data/ --recursive \
  | awk '{print "shared-bucket," $4}' \
  > manifest.csv
```

Upload the manifest:

```bash
aws s3 cp manifest.csv s3://dedicated-bucket/migration/manifest.csv
```

### 1.2 Snapshot Current Table State

Before touching anything, record the current state of every table for rollback validation.

```bash
# List all Iceberg tables in the database
aws glue get-tables \
  --database-name market_data \
  --query 'TableList[].{Name:Name, Location:StorageDescriptor.Location}' \
  --output table
```

Save the full table definitions:

```bash
for TABLE_NAME in $(aws glue get-tables --database-name market_data \
  --query 'TableList[].Name' --output text); do
  aws glue get-table \
    --database-name market_data \
    --name "$TABLE_NAME" \
    --output json > "backup_${TABLE_NAME}.json"
  echo "Backed up: $TABLE_NAME"
done
```

### 1.3 Create the Batch Operations IAM Role

S3 Batch Operations needs a role with read access to the source and write access to the destination.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBatchOpsAssume",
      "Effect": "Allow",
      "Principal": { "Service": "batchoperations.s3.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Save as `batch-ops-trust-policy.json`, then create the role:

```bash
aws iam create-role \
  --role-name S3BatchOpsDatalakeMigration \
  --assume-role-policy-document file://batch-ops-trust-policy.json
```

Attach a policy granting the required permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSource",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetObjectTagging"
      ],
      "Resource": "arn:aws:s3:::shared-bucket/*"
    },
    {
      "Sid": "WriteDestination",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectTagging"
      ],
      "Resource": "arn:aws:s3:::dedicated-bucket/*"
    },
    {
      "Sid": "ReadManifest",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::dedicated-bucket/migration/*"
    },
    {
      "Sid": "WriteReport",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::dedicated-bucket/migration/reports/*"
    }
  ]
}
```

```bash
aws iam put-role-policy \
  --role-name S3BatchOpsDatalakeMigration \
  --policy-name S3BatchOpsMigrationPolicy \
  --policy-document file://batch-ops-migration-policy.json
```

---

## Phase 2 — Copy Data with S3 Batch Operations

### 2.1 Create the Batch Operations Job

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws s3control create-job \
  --account-id "$ACCOUNT_ID" \
  --operation '{
    "S3PutObjectCopy": {
      "TargetResource": "arn:aws:s3:::dedicated-bucket",
      "MetadataDirective": "COPY",
      "StorageClass": "STANDARD"
    }
  }' \
  --manifest '{
    "Spec": {
      "Format": "S3BatchOperations_CSV_20180820",
      "Fields": ["Bucket", "Key"]
    },
    "Location": {
      "ObjectArn": "arn:aws:s3:::dedicated-bucket/migration/manifest.csv",
      "ETag": "<MANIFEST_ETAG>"
    }
  }' \
  --report '{
    "Bucket": "arn:aws:s3:::dedicated-bucket",
    "Prefix": "migration/reports/",
    "Format": "Report_CSV_20180820",
    "Enabled": true,
    "ReportScope": "AllTasks"
  }' \
  --priority 1 \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/S3BatchOpsDatalakeMigration" \
  --confirmation-required \
  --description "Iceberg datalake migration: shared-bucket -> dedicated-bucket"
```

> **Note:** Get the manifest ETag with:
> ```bash
> aws s3api head-object --bucket dedicated-bucket --key migration/manifest.csv --query ETag --output text
> ```

### 2.2 Confirm and Run the Job

The job is created in a `Suspended` state because of `--confirmation-required`. Review the job details, then confirm:

```bash
JOB_ID="<job-id-from-create-job-output>"

# Review the job
aws s3control describe-job \
  --account-id "$ACCOUNT_ID" \
  --job-id "$JOB_ID"

# Confirm to start execution
aws s3control update-job-status \
  --account-id "$ACCOUNT_ID" \
  --job-id "$JOB_ID" \
  --requested-job-status Ready
```

### 2.3 Monitor Progress

```bash
# Poll job status
aws s3control describe-job \
  --account-id "$ACCOUNT_ID" \
  --job-id "$JOB_ID" \
  --query 'Job.{Status:Status, Progress:ProgressSummary}' \
  --output table
```

Wait until `Status` is `Complete`. Check the completion report for any failed objects:

```bash
aws s3 cp s3://dedicated-bucket/migration/reports/ ./reports/ --recursive
# Check for failures
grep -c "failed" reports/*.csv
```

### 2.4 Validate the Copy

Compare object counts and total size between source and destination:

```bash
echo "=== Source ==="
aws s3 ls s3://${SHARERED}/market_data/ --recursive --summarize | tail -2

echo "=== Destination ==="
aws s3 ls s3://dedicated-bucket/market_data/ --recursive --summarize | tail -2
```

For a stronger check, compare ETags on a sample of objects:

```bash
# Spot-check a few files
for KEY in $(aws s3 ls s3://shared-bucket/market_data/ --recursive | head -5 | awk '{print $4}'); do
  SRC_ETAG=$(aws s3api head-object --bucket shared-bucket --key "$KEY" --query ETag --output text)
  DST_ETAG=$(aws s3api head-object --bucket dedicated-bucket --key "$KEY" --query ETag --output text)
  if [ "$SRC_ETAG" = "$DST_ETAG" ]; then
    echo "OK: $KEY"
  else
    echo "MISMATCH: $KEY (src=$SRC_ETAG dst=$DST_ETAG)"
  fi
done
```

---

## Phase 3 — Update Iceberg Metadata Files

Iceberg metadata files (JSON and Avro manifests) contain **absolute S3 paths** to data files. After copying, these paths still reference the old bucket. You need to update them.

### 3.1 Understand the Metadata Structure

Each Iceberg table has a metadata directory:

```
s3://dedicated-bucket/market_data/<table>/metadata/
  ├── v1.metadata.json
  ├── v2.metadata.json       ← latest version
  ├── snap-<id>-<uuid>.avro  ← snapshot manifests
  ├── <uuid>-m0.avro         ← manifest files (contain data file paths)
  └── version-hint.text
```

The key files that contain bucket references:

| File | Contains | Format |
|---|---|---|
| `v*.metadata.json` | `location` field (table root path) | JSON |
| `*-m*.avro` (manifest files) | `data_file.file_path` for every data file | Avro binary |

### 3.2 Option A — Re-register via Spark (Recommended)

The cleanest approach is to use Spark with the Iceberg library to re-register the table. This avoids manually editing Avro manifests.

**Step 1: Drop and re-create the Glue table pointing to the new location.**

Run this in a Spark session (Glue job, EMR, or local Spark with Iceberg + Glue catalog):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://dedicated-bucket/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# For each table, register it from the new metadata location
tables = ["table_a", "table_b", "table_c"]  # replace with your table names
database = "market_data"

for table_name in tables:
    metadata_location = f"s3://dedicated-bucket/{database}/{table_name}/metadata/"

    # Find the latest metadata.json version
    # (you can list the metadata dir and pick the highest v*.metadata.json)

    spark.sql(f"DROP TABLE IF EXISTS glue_catalog.{database}.{table_name}")

    spark.sql(f"""
        CALL glue_catalog.system.register_table(
            table => '{database}.{table_name}',
            metadata_file => 's3://dedicated-bucket/{database}/{table_name}/metadata/<latest>.metadata.json'
        )
    """)
    print(f"Registered: {database}.{table_name}")
```

> **Important:** The `register_table` procedure points the catalog at an existing metadata file. Since the manifest Avro files still contain old bucket paths in `data_file.file_path`, you need to run a full rewrite after registration (see Step 2).

**Step 2: Rewrite data files to update all internal path references.**

```python
for table_name in tables:
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{database}.{table_name}'
        )
    """)
    print(f"Rewritten: {database}.{table_name}")
```

This creates new manifest files with paths pointing to the dedicated bucket.

**Step 3: Clean up old metadata.**

```python
for table_name in tables:
    # Expire old snapshots (keeps only the latest)
    spark.sql(f"""
        CALL glue_catalog.system.expire_snapshots(
            table => '{database}.{table_name}',
            retain_last => 1
        )
    """)

    # Remove orphan files from the old copy
    spark.sql(f"""
        CALL glue_catalog.system.remove_orphan_files(
            table => '{database}.{table_name}'
        )
    """)
    print(f"Cleaned up: {database}.{table_name}")
```

### 3.3 Option B — Patch Metadata JSON + CTAS (Alternative)

If you want to avoid `rewrite_data_files` (which rewrites all Parquet files), you can:

1. Download the latest `metadata.json` for each table.
2. Find-and-replace the old bucket name with the new one.
3. Upload the patched metadata.
4. Register the table with the patched metadata.

```bash
TABLE_NAME="your_table"
DATABASE="market_data"

# Download latest metadata
aws s3 cp \
  "s3://dedicated-bucket/${DATABASE}/${TABLE_NAME}/metadata/v2.metadata.json" \
  ./v2.metadata.json

# Replace bucket references in the JSON
sed -i '' "s|s3://shared-bucket|s3://dedicated-bucket|g" ./v2.metadata.json

# Upload patched metadata
aws s3 cp ./v2.metadata.json \
  "s3://dedicated-bucket/${DATABASE}/${TABLE_NAME}/metadata/v2.metadata.json"
```

> **Caveat:** This only patches the JSON metadata. The Avro manifest files (`*-m*.avro`) also contain absolute paths in binary format. You would need a tool like `avro-tools` or a custom script to patch those. This is why Option A (Spark rewrite) is generally preferred.

**Patching Avro manifests (advanced):**

```python
"""
Script to patch Avro manifest files — replaces old bucket paths with new ones.
Requires: pip install fastavro boto3
"""
import fastavro
import boto3
import io
import re

s3 = boto3.client('s3')
OLD_PREFIX = "s3://shared-bucket"
NEW_PREFIX = "s3://dedicated-bucket"

def patch_manifest(bucket, key):
    """Download an Avro manifest, replace paths, and upload back."""
    response = s3.get_object(Bucket=bucket, Key=key)
    data = io.BytesIO(response['Body'].read())

    reader = fastavro.reader(data)
    schema = reader.writer_schema
    records = []

    for record in reader:
        # Patch data_file path
        if 'data_file' in record and 'file_path' in record['data_file']:
            record['data_file']['file_path'] = record['data_file']['file_path'].replace(
                OLD_PREFIX, NEW_PREFIX
            )
        records.append(record)

    # Write patched Avro back
    output = io.BytesIO()
    fastavro.writer(output, schema, records)
    output.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=output.read())
    print(f"Patched: s3://{bucket}/{key}")


# Usage: list and patch all manifest files for a table
def patch_all_manifests(bucket, database, table_name):
    prefix = f"{database}/{table_name}/metadata/"
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.avro') and '-m' in key.split('/')[-1]:
                patch_manifest(bucket, key)

# Example
patch_all_manifests("dedicated-bucket", "market_data", "your_table")
```

After patching both JSON and Avro files, register the table:

```sql
CALL glue_catalog.system.register_table(
    table => 'market_data.your_table',
    metadata_file => 's3://dedicated-bucket/market_data/your_table/metadata/v2.metadata.json'
);
```

---

## Phase 4 — Re-register Tables in Glue Catalog

### 4.1 Using Spark `register_table` (covered in Phase 3)

If you followed Phase 3 Option A, the tables are already registered. Verify:

```python
for table_name in tables:
    df = spark.sql(f"SELECT count(*) as cnt FROM glue_catalog.{database}.{table_name}")
    df.show()
```

### 4.2 Using AWS CLI (Direct Glue API)

If you patched metadata manually (Option B) and want to update Glue without Spark:

```bash
DATABASE="market_data"
TABLE_NAME="your_table"
NEW_LOCATION="s3://dedicated-bucket/${DATABASE}/${TABLE_NAME}"
METADATA_LOCATION="s3://dedicated-bucket/${DATABASE}/${TABLE_NAME}/metadata/v2.metadata.json"

# Update the table location in Glue
aws glue update-table \
  --database-name "$DATABASE" \
  --table-input "{
    \"Name\": \"${TABLE_NAME}\",
    \"StorageDescriptor\": {
      \"Location\": \"${NEW_LOCATION}\",
      \"InputFormat\": \"org.apache.hadoop.mapred.FileInputFormat\",
      \"OutputFormat\": \"org.apache.hadoop.mapred.FileOutputFormat\",
      \"SerdeInfo\": {
        \"SerializationLibrary\": \"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"
      }
    },
    \"Parameters\": {
      \"table_type\": \"ICEBERG\",
      \"metadata_location\": \"${METADATA_LOCATION}\"
    }
  }"
```

> **Important:** When using `update-table`, you must provide the complete `TableInput` structure. It's safer to fetch the existing definition, modify the location fields, and push it back:

```bash
# Fetch current definition
aws glue get-table --database-name "$DATABASE" --name "$TABLE_NAME" \
  --query 'Table' --output json > table_def.json

# Use jq to patch the locations
jq --arg new_loc "$NEW_LOCATION" --arg meta_loc "$METADATA_LOCATION" '
  del(.DatabaseName, .CreateTime, .UpdateTime, .CreatedBy, .IsRegisteredWithLakeFormation,
      .CatalogId, .VersionId, .IsMultiDialectView, .Status) |
  .StorageDescriptor.Location = $new_loc |
  .Parameters.metadata_location = $meta_loc
' table_def.json > table_input.json

# Update
aws glue update-table \
  --database-name "$DATABASE" \
  --table-input file://table_input.json
```

### 4.3 Batch Script — Update All Tables

```bash
#!/bin/bash
DATABASE="market_data"
OLD_BUCKET="shared-bucket"
NEW_BUCKET="dedicated-bucket"

for TABLE_NAME in $(aws glue get-tables --database-name "$DATABASE" \
  --query 'TableList[].Name' --output text); do

  echo "Processing: $TABLE_NAME"

  # Get current table definition
  aws glue get-table --database-name "$DATABASE" --name "$TABLE_NAME" \
    --query 'Table' --output json > "/tmp/${TABLE_NAME}_def.json"

  # Patch all references from old bucket to new bucket
  jq --arg old "s3://${OLD_BUCKET}" --arg new "s3://${NEW_BUCKET}" '
    del(.DatabaseName, .CreateTime, .UpdateTime, .CreatedBy,
        .IsRegisteredWithLakeFormation, .CatalogId, .VersionId,
        .IsMultiDialectView, .Status) |
    walk(if type == "string" then gsub($old; $new) else . end)
  ' "/tmp/${TABLE_NAME}_def.json" > "/tmp/${TABLE_NAME}_input.json"

  # Update the table
  aws glue update-table \
    --database-name "$DATABASE" \
    --table-input "file:///tmp/${TABLE_NAME}_input.json"

  echo "Updated: $TABLE_NAME"
done
```

---

## Phase 5 — Validation

### 5.1 Verify Glue Catalog Entries

```bash
aws glue get-tables \
  --database-name market_data \
  --query 'TableList[].{Name:Name, Location:StorageDescriptor.Location, MetadataLocation:Parameters.metadata_location}' \
  --output table
```

Confirm all locations point to `s3://dedicated-bucket/...`.

### 5.2 Run Queries Against Migrated Tables

```sql
-- Row count check (compare with pre-migration counts)
SELECT count(*) FROM glue_catalog.market_data.table_a;

-- Sample data check
SELECT * FROM glue_catalog.market_data.table_a LIMIT 10;

-- Metadata inspection
SELECT * FROM glue_catalog.market_data.table_a.snapshots;
SELECT count(*) FROM glue_catalog.market_data.table_a.files;
```

### 5.3 Verify No References to Old Bucket

```bash
# Check metadata files for any remaining old bucket references
for TABLE_NAME in $(aws glue get-tables --database-name market_data \
  --query 'TableList[].Name' --output text); do

  METADATA_LOC=$(aws glue get-table --database-name market_data --name "$TABLE_NAME" \
    --query 'Table.Parameters.metadata_location' --output text)

  aws s3 cp "$METADATA_LOC" /tmp/check_metadata.json
  if grep -q "shared-bucket" /tmp/check_metadata.json; then
    echo "WARNING: $TABLE_NAME metadata still references old bucket!"
  else
    echo "OK: $TABLE_NAME"
  fi
done
```

---

## Phase 6 — Cleanup

### 6.1 Update Downstream Consumers

- Update any Glue jobs, EMR notebooks, Athena workgroups, or application configs that reference the old bucket path.
- Update IAM policies to grant access to the new bucket and (optionally) revoke access to the old bucket's datalake prefix.
- Update any Lake Formation permissions if applicable.

### 6.2 Remove Old Data (After Validation Period)

Keep the old data for a grace period (e.g., 1–2 weeks) as a rollback safety net. Once confident:

```bash
# Only after thorough validation!
aws s3 rm s3://shared-bucket/market_data/ --recursive
```

### 6.3 Clean Up Migration Resources

```bash
# Remove the Batch Ops role
aws iam delete-role-policy \
  --role-name S3BatchOpsDatalakeMigration \
  --policy-name S3BatchOpsMigrationPolicy

aws iam delete-role --role-name S3BatchOpsDatalakeMigration

# Remove inventory configuration
aws s3api delete-bucket-inventory-configuration \
  --bucket shared-bucket \
  --id datalake-migration-inventory

# Remove migration artifacts
aws s3 rm s3://dedicated-bucket/migration/ --recursive
```

---

## Rollback Plan

If something goes wrong after re-registration:

1. **Re-point Glue to old bucket:** Run the batch update script from Phase 4.3 but swap `OLD_BUCKET` and `NEW_BUCKET`.
2. **Verify queries work** against the original data.
3. **Investigate** what went wrong before retrying.

The original data in the shared bucket is untouched until you explicitly delete it in Phase 6.2.

---

## Summary Checklist

| Step | Description | Status |
|---|---|---|
| 1.1 | Generate object inventory / manifest | ☐ |
| 1.2 | Snapshot current Glue table definitions | ☐ |
| 1.3 | Create Batch Ops IAM role | ☐ |
| 2.1 | Create S3 Batch Operations copy job | ☐ |
| 2.2 | Confirm and start the job | ☐ |
| 2.3 | Monitor until completion | ☐ |
| 2.4 | Validate copy (counts, ETags) | ☐ |
| 3.x | Update Iceberg metadata (Option A or B) | ☐ |
| 4.x | Re-register tables in Glue Catalog | ☐ |
| 5.1 | Verify Glue catalog entries | ☐ |
| 5.2 | Run validation queries | ☐ |
| 5.3 | Confirm no old bucket references | ☐ |
| 6.1 | Update downstream consumers | ☐ |
| 6.2 | Remove old data (after grace period) | ☐ |
| 6.3 | Clean up migration resources | ☐ |
