"""
Iceberg Persist Synthetic Data - AWS Glue PySpark Script

Reads a staged Parquet dataset and inserts it into an existing Iceberg
target table in batches.

Usage:
  Run as an AWS Glue job with the following job parameters:
    --DATABASE_NAME       : Glue catalog database (required)
    --TARGET_TABLE        : Target Iceberg table name (required)
    --TARGET_ROWS         : Expected number of rows in the staging data (required)
    --STAGING_PATH        : S3 path for staging Parquet data (required)
"""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext


# ---------------------------------------------------------------------------
# Configuration & Parameters
# ---------------------------------------------------------------------------

BATCH_SIZE = 500_000  # rows per write batch
# G1.X, 10 workers
NUM_EXECUTORS = 10 - 1
SLOTS_PER_EXEC = 4


def get_params():
    """Parse Glue job parameters."""
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "DATABASE_NAME",
        "TARGET_TABLE",
        "TARGET_ROWS",
        "STAGING_PATH",
    ])
    return {
        "job_name":       args["JOB_NAME"],
        "database":       args["DATABASE_NAME"],
        "target_table":   args["TARGET_TABLE"],
        "target_rows":    int(args["TARGET_ROWS"]),
        "staging_path":   args["STAGING_PATH"],
    }


def init_spark():
    """Initialise Spark session with Iceberg + Glue catalog."""
    spark = (
        SparkSession.builder
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )
    glue_context = GlueContext(spark.sparkContext)
    return spark, glue_context


def table_fqn(database, table_name):
    """Return fully-qualified Iceberg table name."""
    return f"glue_catalog.{database}.{table_name}"


def elapsed(start):
    return round(time.time() - start, 3)


# ---------------------------------------------------------------------------
# Data Insertion
# ---------------------------------------------------------------------------

def insert_from_parquet(spark, target_fqn, staging_path, target_rows):
    """Read the staged Parquet data and insert into the Iceberg target table
    in batches."""
    parquet_df = spark.read.parquet(staging_path)
    total_written = 0

    if target_rows <= BATCH_SIZE:
        print(f"  Writing {target_rows:,} rows in a single batch...")
        start = time.time()
        parquet_df = parquet_df.coalesce(NUM_EXECUTORS * SLOTS_PER_EXEC)
        parquet_df.writeTo(target_fqn).append()
        total_written = target_rows
        print(f"  Batch completed in {elapsed(start)}s — {total_written:,} rows written.")
    else:
        parquet_df = parquet_df.withColumn("_batch_id", F.monotonically_increasing_id())
        num_batches = (target_rows + BATCH_SIZE - 1) // BATCH_SIZE

        parquet_df.cache()
        parquet_df.count()  # materialise the cache
        print(f"  Cached {target_rows:,} staged rows across executors.")

        for i in range(num_batches):
            batch_df = parquet_df.filter(
                F.col("_batch_id") % num_batches == i
            ).drop("_batch_id")
            rows_this_batch = min(BATCH_SIZE, target_rows - total_written)

            print(f" Attempting to write batch {i + 1}/{num_batches} ({rows_this_batch:,} rows)...")
            start = time.time()
            print(" ...coalescing source DF...")
            batch_df = batch_df.coalesce(NUM_EXECUTORS * SLOTS_PER_EXEC)
            print(" ...START of append()...")
            batch_df.writeTo(target_fqn).append()
            total_written += rows_this_batch
            print(f"  Batch {i + 1} completed in {elapsed(start)}s — "
                  f"total written: {total_written:,}/{target_rows:,}")

        parquet_df.unpersist()

    return total_written

# ---------------------------------------------------------------------------
# TBL Optimization
# ---------------------------------------------------------------------------
def table_optimization(spark, target_fqn):
    spark.sql(f"""
        ALTER TABLE {target_fqn}
        SET TBLPROPERTIES ('write.distribution-mode' = 'hash')
    """)
    print(" ... table optimized with hash distribution...")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    params = get_params()
    spark, glue_context = init_spark()

    target_fqn = table_fqn(params["database"], params["target_table"])
    target_rows = params["target_rows"]
    staging_path = f"{params['staging_path']}/{params['target_table']}_parquet_staging"

    print("=" * 60)
    print("Iceberg Persist Synthetic Data")
    print("=" * 60)
    print(f"  Target table : {target_fqn}")
    print(f"  Target rows  : {target_rows:,}")
    print(f"  Staging path : {staging_path}")
    print()

    # Prelim: Optimizations
    table_optimization(spark, target_fqn)

    # Step 1: Read Parquet staging data and insert into Iceberg
    print(f"Step 1: Reading staged Parquet and inserting into {target_fqn}...")
    start = time.time()
    rows_written = insert_from_parquet(spark, target_fqn, staging_path, target_rows)
    iceberg_time = elapsed(start)
    print(f"\n  Iceberg insert time: {iceberg_time}s")
    print(f"  Rows written: {rows_written:,}")
    print(f"  Throughput: {round(rows_written / max(iceberg_time, 0.001)):,} rows/sec")

    # Step 2: Verify
    print("\nStep 2: Verifying target table...")
    actual_count = spark.sql(f"SELECT count(*) FROM {target_fqn}").collect()[0][0]
    print(f"  Target row count: {actual_count:,}")
    print(f"  Expected:         {target_rows:,}")

    print("\n  Sample rows from target:")
    spark.sql(f"SELECT * FROM {target_fqn} LIMIT 5").show(truncate=False)

    # Step 3: Cleanup staging data
    print(f"\nStep 3: Cleaning up staging Parquet at {staging_path}...")
    glue_context.purge_s3_path(staging_path, options={"retentionPeriod": 0})
    print("  Staging data removed.")
    print("\nIceberg persist complete.")


if __name__ == "__main__":
    main()
