## PHASE 1

### --> Env-Vars: change these as needed, per database
```bash
export SHARED_BUCKET="jpmc-iceberg-warehouse-038676220235-us-west-2"
export DEDICATED_BUCKET="jpmc-iceberg-whv2-038676220235-us-west-2"
export DB_NAME="jpmc_demo_warehouse"
```

### --> Create BOPS role
```bash
aws iam create-role \
  --role-name S3BatchOpsDatalakeMigration \
  --assume-role-policy-document file://bops-trustpolicy.json
```

```bash
aws iam put-role-policy \
  --role-name S3BatchOpsDatalakeMigration \
  --policy-name S3BatchOpsMigrationPolicy \
  --policy-document file://bops-migrationpolicy.json
```


### --> Create Working area. 
```bash
mkdir -p ./workarea/${DB_NAME}
cd workarea
```


### --> Create manifest for BOPS.
```bash
aws s3 ls s3://${SHARED_BUCKET}/warehouse/ --recursive \
  | awk '{print ENVIRON["SHARED_BUCKET"]"," $4}' \
  > ${DB_NAME}/manifest.csv
```

### --> Upload manifest to S3 (dedicated bucket)
```bash
aws s3 cp ${DB_NAME}/manifest.csv s3://${DEDICATED_BUCKET}/migration/manifest.csv
```

### --> All table list.
```bash
aws glue get-tables \
  --database-name ${DB_NAME} \
  --query 'TableList[].{Name:Name, Location:StorageDescriptor.Location}' \
  --output table
```

### --> Save full table definitions
```bash
for TABLE_NAME in $(aws glue get-tables --database-name ${DB_NAME} \
  --query 'TableList[].Name' --output text); do
  aws glue get-table \
    --database-name ${DB_NAME} \
    --name "$TABLE_NAME" \
    --output json > "${DB_NAME}/backup_${TABLE_NAME}.json"
  echo "Backed up: $TABLE_NAME"
done
```

## PHASE 2 — Copy Data with S3 Batch Operations

### --> Create the BOPS Job. NOTE: Copy Job ID from output

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
MANIFEST_ETAG=$(aws s3api head-object --bucket ${DEDICATED_BUCKET} --key migration/manifest.csv --query ETag --output text)

aws s3control create-job \
  --account-id "$ACCOUNT_ID" \
  --operation '{
    "S3PutObjectCopy": {
      "TargetResource": "arn:aws:s3:::'"${DEDICATED_BUCKET}"'",
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
      "ObjectArn": "arn:aws:s3:::'"${DEDICATED_BUCKET}"'/migration/manifest.csv",
      "ETag": '${MANIFEST_ETAG}'
    }
  }' \
  --report '{
    "Bucket": "arn:aws:s3:::'"${DEDICATED_BUCKET}"'",
    "Prefix": "migration/reports/",
    "Format": "Report_CSV_20180820",
    "Enabled": true,
    "ReportScope": "AllTasks"
  }' \
  --priority 1 \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/S3BatchOpsDatalakeMigration" \
  --confirmation-required \
  --description "Iceberg datalake migration: ${SHARED_BUCKET} -> ${DEDICATED_BUCKET}"

```

```bash
JOB_ID="1c5f23d3-fdcb-44c0-8fe3-143a54534190"

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

###--> Monitor Progress. Wait for complete status

```bash
# Poll job status
aws s3control describe-job \
  --account-id "$ACCOUNT_ID" \
  --job-id "$JOB_ID" \
  --query 'Job.{Status:Status, Progress:ProgressSummary}' \
  --output table
```

###--> Retrieve job report, check for failures (if any)
```bash
aws s3 cp s3://${DEDICATED_BUCKET}/migration/reports/ ${DB_NAME}/reports/ --recursive
# Check for failures
grep -c "failed" ${DB_NAME}/reports/job-${JOB_ID}/results/*.csv
```

###--> Validate the Copy. Compare object counts and total size between source and destination:

```bash
echo "=== Source ==="
aws s3 ls s3://${SHARED_BUCKET}/warehouse/ --recursive --summarize | tail -2

echo "=== Destination ==="
aws s3 ls s3://${DEDICATED_BUCKET}/warehouse/ --recursive --summarize | tail -2
```

## PHASE 3 — Update Iceberg Metadata Files

- Upload nb-iceberg-migration.ipynb to Glue
- Use Glue notebook to re-register tables to new location
- validate table counts w/SQL


### --> Batch Script — Update All Tables

```bash
#!/bin/bash
#$(aws glue get-tables --database-name "$DB_NAME" \
#  --query 'TableList[].Name' --output text); do

TABLES=("main_rd_table" "main_sample_table")

for TABLE_NAME in "${TABLES[@]}"; do

  echo "Processing: $TABLE_NAME"

  # Get current table definition
  aws glue get-table --database-name "$DB_NAME" --name "$TABLE_NAME" \
    --query 'Table' --output json > "/tmp/${TABLE_NAME}_def.json"

  # Patch all references from old bucket to new bucket
  jq --arg old "s3://${SHARED_BUCKET}" --arg new "s3://${DEDICATED_BUCKET}" '
    del(.DatabaseName, .CreateTime, .UpdateTime, .CreatedBy,
        .IsRegisteredWithLakeFormation, .CatalogId, .VersionId,
        .IsMultiDialectView, .Status) |
    walk(if type == "string" then gsub($old; $new) else . end)
  ' "/tmp/${TABLE_NAME}_def.json" > "/tmp/${TABLE_NAME}_input.json"

  # Update the table
  aws glue update-table \
    --database-name "$DB_NAME" \
    --table-input "file:///tmp/${TABLE_NAME}_input.json"

  echo "Updated: $TABLE_NAME"
done
```

## PHASE 4 — Validation

### --> Verify Glue Catalog Entries

```bash
aws glue get-tables \
  --database-name ${DB_NAME} \
  --query 'TableList[].{Name:Name, Location:StorageDescriptor.Location, MetadataLocation:Parameters.metadata_location}' \
  --output table
```