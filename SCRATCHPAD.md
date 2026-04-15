# Basic scratchpad for snippets

---
```

SCRATCHPAD-2025-APR-15


## DEPLOY

S3_BUCKET="BUCKET"
SCRIPT_BUCKET="BUCKET"
ROLE_ARN="ROLEARN"

aws s3 cp iceberg_partition_perf_test_v2.py \
  s3://${SCRIPT_BUCKET}/tmp_glue_scripts/iceberg_partition_perf_test_v2.py



### DATA GEN - INPUT
STAGING_PATH="s3://BUCKET/temporary/staging"

aws glue start-job-run \
  --job-name ot-synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--SOURCE_TABLE": "ot_test_simm_sensitivty_stg_100k",
    "--TARGET_TABLE": "ot_pftst_sensi_input_data",
    "--TARGET_PARTITION": " ",
    "--TARGET_ROWS": "100000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'

#### copy data

aws glue start-job-run \
  --job-name ot-iceberg-persist-synthetic-data \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--TARGET_TABLE": "ot_pftst_sensi_input_data",
    "--TARGET_ROWS": "100000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'


## target table - Unpartitioned

aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--SOURCE_TABLE": "ot_test_simm_sensitivty_stg_100k",
    "--TARGET_TABLE": "ot_pftst_sensi_unpartitioned",
    "--TARGET_PARTITION": " ",
    "--TARGET_ROWS": "1000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'


## target table - Date partition

aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--SOURCE_TABLE": "ot_test_simm_sensitivty_stg_100k",
    "--TARGET_TABLE": "ot_pftst_sensi_datepart",
    "--TARGET_PARTITION": "cob_date",
    "--TARGET_ROWS": "1000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'

## target table - bucket partition

aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--SOURCE_TABLE": "ot_test_simm_sensitivty_stg_100k",
    "--TARGET_TABLE": "ot_pftst_sensi_bucket",
    "--TARGET_PARTITION": "bucket(16, primary_key)",
    "--TARGET_ROWS": "1000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'

## target table - truncate partition

aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--SOURCE_TABLE": "ot_test_simm_sensitivty_stg_100k",
    "--TARGET_TABLE": "ot_pftst_sensi_truncate",
    "--TARGET_PARTITION": "truncate(10, primary_key)",
    "--TARGET_ROWS": "1000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'

## target table - composite partition

aws glue start-job-run \
  --job-name synthetic-data-generator \
  --arguments '{
    "--DATABASE_NAME": "market_data",
    "--SOURCE_TABLE": "ot_test_simm_sensitivty_stg_100k",
    "--TARGET_TABLE": "ot_pftst_sensi_truncate",
    "--TARGET_PARTITION": "cob_date,bucket(16, primary_key)",
    "--TARGET_ROWS": "1000",
    "--STAGING_PATH": "'"${STAGING_PATH}"'"
  }'



  ### PERF TEST
  \

aws glue start-job-run \
  --job-name ot-iceberg-partition-perf-test-v2 \
  --arguments '{
    "--TEST_SCOPE": "insert",
    "--DATABASE_NAME": "market_data",
    "--INPUT_TABLE": "glue_catalog.market_data.ot_pftst_sensi_input_data",
    "--JOIN_KEY": "primary_key",
    "--TABLE_UNPARTITIONED": "glue_catalog.market_data.ot_pftst_sensi_unpartitioned",
    "--TABLE_IDENTITY": "glue_catalog.market_data.ot_pftst_sensi_datepart",
    "--PARTITION_COL_IDENTITY": "cob_date",
    "--TABLE_BUCKET": "glue_catalog.market_data.ot_pftst_sensi_bucket",
    "--PARTITION_COL_BUCKET": "bucket(16, primary_key)",    
    "--TABLE_TRUNCATE": "glue_catalog.market_data.ot_pftst_sensi_truncate",
    "--PARTITION_COL_TRUNCATE": "truncate(10, priamry_key)",    
    "--TABLE_COMPOSITE": "glue_catalog.market_data.ot_pftst_sensi_truncate",
    "--PARTITION_COL_COMPOSITE": "month(cob_date), bucket(16, primary_key)",
    "--RESULTS_PATH": "s3://'"${S3_BUCKET}"'/perf_results/"
  }'



```

