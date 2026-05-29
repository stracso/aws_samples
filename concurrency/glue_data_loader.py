"""
AWS Glue Job: Iceberg Data Loader
=================================
Simulates a single data loader that:
1. Generates synthetic data (simulating YAML-driven file loading)
2. Writes to a loader-specific Iceberg staging table
3. Validates the staged data
4. Upserts (MERGE INTO) the validated data into the main Iceberg table

Job Parameters:
  --loader_id        : Unique identifier for this loader instance (e.g., "loader_a")
  --source_system    : Source system partition key (e.g., "system_a")
  --database         : Glue catalog database name
  --main_table       : Target Iceberg table name
  --warehouse_path   : S3 warehouse path for Iceberg tables
  --num_rows         : Number of rows to generate (default: 10000)
  --overlap_mode     : "isolated" (default) or "overlapping" — controls partition targeting
  --commit_retries   : Max commit retry attempts (default: 5)
"""

import sys
import time
import random
import json
from datetime import datetime, timedelta

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, expr, rand, floor, concat,
    date_add, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, DateType, TimestampType
)
from py4j.protocol import Py4JJavaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'loader_id',
    'source_system',
    'database',
    'main_table',
    'warehouse_path',
    'num_rows',
    'overlap_mode',
    'commit_retries'
])

LOADER_ID = args['loader_id']
SOURCE_SYSTEM = args['source_system']
DATABASE = args['database']
MAIN_TABLE = args['main_table']
WAREHOUSE_PATH = args['warehouse_path']
NUM_ROWS = int(args.get('num_rows', '10000'))
OVERLAP_MODE = args.get('overlap_mode', 'isolated')
MAX_RETRIES = int(args.get('commit_retries', '5'))

STAGING_TABLE = f"staging_{LOADER_ID}"
FULL_MAIN_TABLE = f"glue_catalog.{DATABASE}.{MAIN_TABLE}"
FULL_STAGING_TABLE = f"glue_catalog.{DATABASE}.{STAGING_TABLE}"

BASE_DELAY_SECONDS = 2

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

print(f"[{LOADER_ID}] Spark session initialized")
print(f"[{LOADER_ID}] Config: database={DATABASE}, main_table={MAIN_TABLE}, "
      f"num_rows={NUM_ROWS}, overlap_mode={OVERLAP_MODE}")

# ---------------------------------------------------------------------------
# Phase 0: Ensure main table exists
# ---------------------------------------------------------------------------

def ensure_main_table_exists():
    """Create the main Iceberg table if it doesn't exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_MAIN_TABLE} (
            id              BIGINT,
            source_system   STRING,
            record_date     DATE,
            customer_id     STRING,
            amount          DOUBLE,
            status          STRING,
            description     STRING,
            load_timestamp  TIMESTAMP,
            loader_id       STRING
        )
        USING iceberg
        PARTITIONED BY (source_system, days(record_date))
        TBLPROPERTIES (
            'commit.retry.num-retries' = '10',
            'commit.retry.min-wait-ms' = '500',
            'commit.retry.max-wait-ms' = '30000',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print(f"[{LOADER_ID}] Main table ensured: {FULL_MAIN_TABLE}")


ensure_main_table_exists()

# ---------------------------------------------------------------------------
# Phase 1: Generate synthetic data (simulates reading YAML + parsing files)
# ---------------------------------------------------------------------------

def generate_data():
    """
    Generate synthetic data simulating a YAML-driven file load.
    In overlap_mode='isolated', all rows belong to this loader's source_system.
    In overlap_mode='overlapping', rows share a common source_system to force conflicts.
    """
    print(f"[{LOADER_ID}] Generating {NUM_ROWS} rows...")

    # Determine the source_system value based on overlap mode
    if OVERLAP_MODE == "overlapping":
        source_sys_value = "shared_system"  # All loaders write to same partition
    else:
        source_sys_value = SOURCE_SYSTEM  # Each loader owns its partition

    # Generate a base DataFrame with sequential IDs
    # Use loader-specific ID ranges to avoid PK collisions in isolated mode
    # In overlapping mode, use shared ID ranges to test upsert behavior
    loader_hash = abs(hash(LOADER_ID)) % 1000
    id_offset = loader_hash * 1_000_000 if OVERLAP_MODE == "isolated" else 0

    df = (
        spark.range(id_offset, id_offset + NUM_ROWS)
        .withColumnRenamed("id", "id")
        .withColumn("source_system", lit(source_sys_value))
        .withColumn("record_date",
                    date_add(to_date(lit("2024-01-01")), (col("id") % 30).cast("int")))
        .withColumn("customer_id",
                    concat(lit("CUST-"), (col("id") % 500).cast("string")))
        .withColumn("amount", (rand() * 10000).cast("double"))
        .withColumn("status",
                    expr("CASE WHEN rand() > 0.3 THEN 'ACTIVE' "
                         "WHEN rand() > 0.5 THEN 'PENDING' ELSE 'CLOSED' END"))
        .withColumn("description",
                    concat(lit(f"Record from {LOADER_ID} - "), col("id").cast("string")))
        .withColumn("load_timestamp", current_timestamp())
        .withColumn("loader_id", lit(LOADER_ID))
    )

    print(f"[{LOADER_ID}] Generated {df.count()} rows with source_system='{source_sys_value}'")
    return df


# ---------------------------------------------------------------------------
# Phase 2: Write to staging table
# ---------------------------------------------------------------------------

def write_to_staging(df):
    """Write data to a loader-specific Iceberg staging table."""
    print(f"[{LOADER_ID}] Writing to staging table: {FULL_STAGING_TABLE}")

    # Drop and recreate staging table for idempotency
    spark.sql(f"DROP TABLE IF EXISTS {FULL_STAGING_TABLE}")

    df.writeTo(FULL_STAGING_TABLE).using("iceberg").createOrReplace()

    staged_count = spark.read.table(FULL_STAGING_TABLE).count()
    print(f"[{LOADER_ID}] Staging table loaded: {staged_count} rows")
    return staged_count

# ---------------------------------------------------------------------------
# Phase 3: Validate staged data
# ---------------------------------------------------------------------------

def validate_staging():
    """Run validation checks on the staging table."""
    print(f"[{LOADER_ID}] Validating staging data...")

    staged_df = spark.read.table(FULL_STAGING_TABLE)

    # Check 1: Non-empty
    count = staged_df.count()
    assert count > 0, f"[{LOADER_ID}] VALIDATION FAILED: Staging table is empty"

    # Check 2: No NULL primary keys
    null_ids = staged_df.filter("id IS NULL").count()
    assert null_ids == 0, f"[{LOADER_ID}] VALIDATION FAILED: {null_ids} NULL IDs found"

    # Check 3: No NULL required fields
    null_required = staged_df.filter(
        "source_system IS NULL OR record_date IS NULL OR customer_id IS NULL"
    ).count()
    assert null_required == 0, \
        f"[{LOADER_ID}] VALIDATION FAILED: {null_required} rows with NULL required fields"

    # Check 4: Amount is positive
    negative_amounts = staged_df.filter("amount < 0").count()
    assert negative_amounts == 0, \
        f"[{LOADER_ID}] VALIDATION FAILED: {negative_amounts} negative amounts"

    # Check 5: No duplicate IDs within this batch
    dup_count = (
        staged_df.groupBy("id").count().filter("count > 1").count()
    )
    assert dup_count == 0, \
        f"[{LOADER_ID}] VALIDATION FAILED: {dup_count} duplicate IDs in batch"

    print(f"[{LOADER_ID}] ✓ Validation passed: {count} rows, no issues found")
    return True


# ---------------------------------------------------------------------------
# Phase 4: Upsert to main table with retry logic
# ---------------------------------------------------------------------------

def upsert_to_main():
    """
    MERGE staging data into the main table.
    Implements retry with exponential backoff for commit conflicts.
    """
    print(f"[{LOADER_ID}] Starting MERGE INTO {FULL_MAIN_TABLE}...")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()

            spark.sql(f"""
                MERGE INTO {FULL_MAIN_TABLE} t
                USING {FULL_STAGING_TABLE} s
                ON t.id = s.id AND t.source_system = s.source_system
                WHEN MATCHED THEN
                    UPDATE SET
                        t.record_date = s.record_date,
                        t.customer_id = s.customer_id,
                        t.amount = s.amount,
                        t.status = s.status,
                        t.description = s.description,
                        t.load_timestamp = s.load_timestamp,
                        t.loader_id = s.loader_id
                WHEN NOT MATCHED THEN
                    INSERT (id, source_system, record_date, customer_id,
                            amount, status, description, load_timestamp, loader_id)
                    VALUES (s.id, s.source_system, s.record_date, s.customer_id,
                            s.amount, s.status, s.description, s.load_timestamp, s.loader_id)
            """)

            elapsed = time.time() - start_time
            print(f"[{LOADER_ID}] ✓ MERGE succeeded on attempt {attempt} ({elapsed:.1f}s)")
            return {"success": True, "attempts": attempt, "duration": elapsed}

        except Py4JJavaError as e:
            error_msg = str(e.java_exception)

            if "CommitFailedException" in error_msg or "ValidationException" in error_msg:
                if attempt == MAX_RETRIES:
                    print(f"[{LOADER_ID}] ✗ MERGE failed after {MAX_RETRIES} attempts")
                    raise

                delay = BASE_DELAY_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"[{LOADER_ID}] Commit conflict on attempt {attempt}. "
                      f"Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                print(f"[{LOADER_ID}] ✗ Non-retryable error: {error_msg[:200]}")
                raise

    return {"success": False, "attempts": MAX_RETRIES, "duration": 0}

# ---------------------------------------------------------------------------
# Phase 5: Cleanup and reporting
# ---------------------------------------------------------------------------

def cleanup_staging():
    """Drop the staging table after successful merge."""
    spark.sql(f"DROP TABLE IF EXISTS {FULL_STAGING_TABLE}")
    print(f"[{LOADER_ID}] Staging table dropped: {FULL_STAGING_TABLE}")


def report_results(result):
    """Print final results summary."""
    print("=" * 60)
    print(f"[{LOADER_ID}] LOADER EXECUTION SUMMARY")
    print("=" * 60)
    print(f"  Loader ID:      {LOADER_ID}")
    print(f"  Source System:  {SOURCE_SYSTEM}")
    print(f"  Overlap Mode:   {OVERLAP_MODE}")
    print(f"  Rows Loaded:    {NUM_ROWS}")
    print(f"  MERGE Success:  {result['success']}")
    print(f"  MERGE Attempts: {result['attempts']}")
    print(f"  MERGE Duration: {result['duration']:.1f}s")
    print("=" * 60)

    # Query final table state
    total_rows = spark.sql(f"SELECT count(*) as cnt FROM {FULL_MAIN_TABLE}").collect()[0]['cnt']
    print(f"  Main table total rows: {total_rows}")

    partitions = spark.sql(f"""
        SELECT source_system, count(*) as cnt
        FROM {FULL_MAIN_TABLE}
        GROUP BY source_system
        ORDER BY source_system
    """).collect()
    for row in partitions:
        print(f"    {row['source_system']}: {row['cnt']} rows")


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"[{LOADER_ID}] === STARTING DATA LOADER ===")
    job_start = time.time()

    try:
        # Phase 1: Generate data
        df = generate_data()

        # Phase 2: Write to staging
        write_to_staging(df)

        # Phase 3: Validate
        validate_staging()

        # Phase 4: Upsert with retry
        result = upsert_to_main()

        # Phase 5: Cleanup and report
        cleanup_staging()
        report_results(result)

    except Exception as e:
        print(f"[{LOADER_ID}] ✗ LOADER FAILED: {str(e)}")
        raise
    finally:
        total_time = time.time() - job_start
        print(f"[{LOADER_ID}] Total execution time: {total_time:.1f}s")

    print(f"[{LOADER_ID}] === LOADER COMPLETE ===")
