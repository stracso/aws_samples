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

AWS Console
```
https://signin.aws.amazon.com/federation?Action=login&Destination=https%3A%2F%2Fconsole.aws.amazon.com%2F&SigninToken=oAdeYzPj1AYlz1gRStgunbfDeLyz416oCCMEwSGtjiGAlS22Exji8anipCp5RUEK8C_pIWrVgBbqKoOjpVRUl1yVsZdnDPEv7xMH10CjRPw32XwHePV_ZZyp-dRuugcRXqTXZIylhzKkLXVTzebZwDrGic-RL40HVIdjMjrWfUjduq4C_-_EEv-8yuqFA4hso28fsEcYSXd0PlBEEB04Oi1VpEM8jCuvQRFhFPTn7qTl1B_u-uS7QBSE-oqLQLYLOOyzGdgLbB9ZJI-S-AR96348qcj_rax9hAsNXPs_HmYUf1pUfJulUhYR709fL6_oHGNJS76bLCExBhWg9k3hIx3_1QsaA5x9cEdSg8r4bqnV_JLB44wM_3nvztyeN_jdpeQ1muMd6yi3CA_pNaaj4dT-qaOTO8vISafRXjHdedtVYegQUmv4W8r9QQqiplfiTIgkVMQhcICMdrhqYIBgfdjBv_rekbQHtPzWNuSqoOExwpVmYK2RmAA5Rsk9qa5mdzvnfw_bgaAA6zjV793-lsI7aL4Qttd6ybfuJNSEzUycRj9VmSQo8BvwO1dmg41a49DF39Cb1CsSciMczNBxzmpOqdAHn9HKr7n_Tu5VJIBnlPk6IL4OMngpFCXpjHuElpGx-edo32UecyoyuoR6Rz854_Yr69jplR0Y5nej5l2cCyA451KHtcgYwn_Fpm_tmccnTQj_fiMBVUUtJmYhiRUgo6e7RR0BjTnQeAL0V8L6T098gqfoNioxNNNJ8lSOFPtl56WqrCXhCH_-oltcV0vhs5JFuvLMrHfiZMjkSnMOP2w7IDgaI_QOkHF6qYl0gWvHNn92j_YdGsCwNgibiexpwPlvXnN4FRTHPd0OSuWVaUP1CY2ysE0tO8tj_9rF83hm66Q-K-ES-e1l2_qLeyEnotihZbg8Ft2PbGk0bmdqbBDHzR7KRrOtbMivJl1YHuAFGWk6rvkQ0P6T19j6BE-vHNBiOSs6tENMcJ677TzKdVpEiYnf3o9N-08ENTfYi7_b_N_g8NdU8KcHs6CNGW1bbALyt6cS40WyxzOZTaWZYRnNFaokyKuUsu04apl1o6MlHs6PXBpdnErR75FvGtJF_77w8nHxg5Y1WoAaOSAY4FzTujE1AiDDwrylcIzFs3lJ9moYl0w1kOwhAMrORR394-1VfczsjwxHDly49jh3-FPM4lUCZsVbrmfjeSHozRrgTb7jVi8wo0NHyjMhhqskZZQ6Zsufsofyu1fmyMJenBCvA2OExy7W2o6AxHzwwwxTiGavMSYkdwnnS187AHAgasr1x7mpD2iVl0hkvkJ76_V3vj2VCdyjeKuy-o_Nha0RhmFGq0dsWOxDEHzVF4twajp4dLMF-T7ubMvk7iiLE73q489YFqezhuVySPaFPaKI5WoIJeYQ-AvlTtwsSZXtq7ky9tmLOI3pjcFTfo1wMfH8-AGIi22G23xPTdnwE4QQbgu5bCyVOYvYB0tBFsj__ffL4docyW8kHgceBMgrkBTAdeZE8q3PQBOz-ns-iNeMKTrzV4EeP7ANwOJiHhZGRdgAU1OwOpypK1LXX84IKG5nSgKJJQlJVUuWqhluWq9ZbC_4TwR-yvbzePIIdjs9GG50qmVonAKMjxZaEpiuj2tR9wKvsGHkdfCTpLzd1a48OCV5ZKIFUVSVk58HylYRUvUbqVnsvmTLupMvUsiAr0PS4CSyWIi6KGdsGEhK
```