"""
Iceberg Partition Performance Testing - AWS Glue PySpark Script

Executes insert and query performance tests across different Iceberg partition
strategies as defined in iceberg_partition_perf_guide.md.

Usage:
  Run as an AWS Glue job with the following job parameters:
    --DATABASE_NAME       : Glue catalog database (default: perf_test_db)
    --S3_BUCKET           : S3 bucket for table storage (required)
    --DATASET_SIZE        : small | medium | large (default: small)
    --TEST_SCOPE          : all | insert | query (default: all)
    --RESULTS_PATH        : S3 path for results output (default: s3://<bucket>/perf_results/)
    --WAREHOUSE_PATH      : S3 warehouse path (default: s3://<bucket>/iceberg_warehouse/)
"""

import sys
import time
import json
import random
import string
from datetime import datetime, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType, StringType,
    DecimalType, DateType, ArrayType
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATASET_SIZES = {
    "small":  1_000_000,
    "medium": 100_000_000,
    "large":  1_000_000_000,
}

PARTITION_STRATEGIES = {
    "unpartitioned":    "",
    "identity_date":    "created_date",
    "identity_region":  "region",
    "bucket_16_id":     "bucket(16, id)",
    "truncate_10_id":   "truncate(10, id)",
    "month_ts":         "month(event_ts)",
    "day_ts":           "days(event_ts)",
    "composite":        "month(event_ts), bucket(8, user_id)",
}

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "sa-east-1"]
EVENT_TYPES = ["click", "view", "purchase", "signup", "logout", "error", "search", "share"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_params():
    """Parse Glue job parameters with defaults."""
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "DATABASE_NAME",
        "S3_BUCKET",
        "DATASET_SIZE",
        "TEST_SCOPE",
        "RESULTS_PATH",
        "WAREHOUSE_PATH",
    ])
    bucket = args["S3_BUCKET"]
    return {
        "job_name":       args["JOB_NAME"],
        "database":       args.get("DATABASE_NAME", "perf_test_db"),
        "s3_bucket":      bucket,
        "dataset_size":   args.get("DATASET_SIZE", "small"),
        "test_scope":     args.get("TEST_SCOPE", "all"),
        "results_path":   args.get("RESULTS_PATH", f"s3://{bucket}/perf_results/"),
        "warehouse_path": args.get("WAREHOUSE_PATH", f"s3://{bucket}/iceberg_warehouse/"),
    }



def init_spark(params):
    """Initialise Spark session with Iceberg + Glue catalog."""
    spark = (
        SparkSession.builder
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", params["warehouse_path"])
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{params['database']}")
    return spark


def table_fqn(params, strategy_key):
    """Return fully-qualified Iceberg table name."""
    return f"glue_catalog.{params['database']}.perf_{strategy_key}"


def elapsed(start):
    return round(time.time() - start, 3)


class PerfResult:
    """Accumulates results for a single test run."""

    def __init__(self, strategy, test_id, dataset_size):
        self.strategy = strategy
        self.test_id = test_id
        self.dataset_size = dataset_size
        self.metrics = {}

    def set(self, key, value):
        self.metrics[key] = value

    def to_dict(self):
        return {
            "strategy": self.strategy,
            "test_id": self.test_id,
            "dataset_size": self.dataset_size,
            "timestamp": datetime.utcnow().isoformat(),
            **self.metrics,
        }


# ---------------------------------------------------------------------------
# Data Generation
# ---------------------------------------------------------------------------

def generate_dataset(spark, num_rows, num_partitions=200):
    """Generate a synthetic dataset matching the perf_test schema."""
    base_ts = datetime(2022, 1, 1)
    days_range = 730  # ~2 years of data

    df = (
        spark.range(0, num_rows, numPartitions=num_partitions)
        .withColumn("event_ts",
            (F.lit(base_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
             + F.expr(f"make_interval(0,0,0,0,0, cast(rand() * {days_range * 86400} as int))")))
        .withColumn("user_id",
            F.concat(F.lit("user_"), (F.col("id") % 100000).cast("string")))
        .withColumn("region",
            F.element_at(F.array([F.lit(r) for r in REGIONS]),
                         (F.col("id") % len(REGIONS) + 1).cast("int")))
        .withColumn("event_type",
            F.element_at(F.array([F.lit(e) for e in EVENT_TYPES]),
                         (F.col("id") % len(EVENT_TYPES) + 1).cast("int")))
        .withColumn("payload",
            F.concat(F.lit("payload_"), F.col("id").cast("string")))
        .withColumn("amount",
            F.round(F.rand() * 10000, 2).cast("decimal(18,2)"))
        .withColumn("tags",
            F.array(F.lit("tag_a"), F.lit("tag_b"),
                    F.concat(F.lit("tag_"), (F.col("id") % 50).cast("string"))))
        .withColumn("created_date",
            F.to_date("event_ts"))
    )
    return df


def generate_dimension_table(spark, params):
    """Create a small dimension table for join tests (Q-6)."""
    dim_table = table_fqn(params, "dim_users")
    num_users = 100_000
    df = (
        spark.range(0, num_users)
        .withColumn("user_id",
            F.concat(F.lit("user_"), F.col("id").cast("string")))
        .withColumn("user_name",
            F.concat(F.lit("name_"), F.col("id").cast("string")))
        .withColumn("signup_date",
            F.date_sub(F.current_date(), (F.col("id") % 365).cast("int")))
        .drop("id")
    )
    df.writeTo(dim_table).using("iceberg").createOrReplace()
    return dim_table



# ---------------------------------------------------------------------------
# Table Management
# ---------------------------------------------------------------------------

def create_table(spark, params, strategy_key):
    """Create a fresh Iceberg table for the given partition strategy."""
    fqn = table_fqn(params, strategy_key)
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")

    partition_clause = PARTITION_STRATEGIES[strategy_key]
    partitioned_by = f"PARTITIONED BY ({partition_clause})" if partition_clause else ""

    spark.sql(f"""
        CREATE TABLE {fqn} (
            id              BIGINT,
            event_ts        TIMESTAMP,
            user_id         STRING,
            region          STRING,
            event_type      STRING,
            payload         STRING,
            amount          DECIMAL(18,2),
            tags            ARRAY<STRING>,
            created_date    DATE
        )
        USING iceberg
        {partitioned_by}
        TBLPROPERTIES (
            'write.parquet.row-group-size-bytes' = '134217728',
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read'
        )
    """)
    return fqn


def collect_table_stats(spark, fqn):
    """Gather file / snapshot / manifest statistics for a table."""
    stats = {}
    try:
        files_df = spark.sql(f"SELECT * FROM {fqn}.files")
        stats["file_count"] = files_df.count()
        size_stats = files_df.agg(
            F.avg("file_size_in_bytes").alias("avg_file_size"),
            F.sum("file_size_in_bytes").alias("total_size"),
            F.sum("record_count").alias("total_records"),
        ).collect()[0]
        stats["avg_file_size_mb"] = round((size_stats["avg_file_size"] or 0) / (1024 * 1024), 2)
        stats["total_size_mb"] = round((size_stats["total_size"] or 0) / (1024 * 1024), 2)
        stats["total_records"] = int(size_stats["total_records"] or 0)
    except Exception as e:
        stats["file_stats_error"] = str(e)

    try:
        stats["snapshot_count"] = spark.sql(f"SELECT * FROM {fqn}.snapshots").count()
    except Exception:
        stats["snapshot_count"] = -1

    try:
        stats["manifest_count"] = spark.sql(f"SELECT * FROM {fqn}.manifests").count()
    except Exception:
        stats["manifest_count"] = -1

    try:
        partition_df = spark.sql(
            f"SELECT partition, count(*) as file_cnt FROM {fqn}.files GROUP BY partition"
        )
        stats["partition_count"] = partition_df.count()
        skew = partition_df.agg(
            F.max("file_cnt").alias("max_files"),
            F.min("file_cnt").alias("min_files"),
            F.avg("file_cnt").alias("avg_files"),
        ).collect()[0]
        stats["partition_skew"] = {
            "max_files": int(skew["max_files"] or 0),
            "min_files": int(skew["min_files"] or 0),
            "avg_files": round(float(skew["avg_files"] or 0), 2),
        }
    except Exception:
        stats["partition_count"] = -1

    return stats


# ---------------------------------------------------------------------------
# INSERT Test Cases
# ---------------------------------------------------------------------------

def run_insert_i1_bulk(spark, params, strategy_key, dataset):
    """I-1: Bulk append — insert full dataset in one operation."""
    result = PerfResult(strategy_key, "I-1", params["dataset_size"])
    fqn = create_table(spark, params, strategy_key)

    start = time.time()
    dataset.writeTo(fqn).append()
    duration = elapsed(start)

    row_count = DATASET_SIZES[params["dataset_size"]]
    result.set("duration_s", duration)
    result.set("rows_per_sec", round(row_count / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, fqn))
    return result


def run_insert_i2_incremental(spark, params, strategy_key, dataset):
    """I-2: Incremental append — insert in 100 equal-sized batches."""
    result = PerfResult(strategy_key, "I-2", params["dataset_size"])
    fqn = create_table(spark, params, strategy_key)

    num_batches = 100
    total_rows = dataset.count()
    fraction = 1.0 / num_batches

    start = time.time()
    for i in range(num_batches):
        batch = dataset.sample(withReplacement=False, fraction=fraction, seed=i)
        batch.writeTo(fqn).append()
    duration = elapsed(start)

    result.set("duration_s", duration)
    result.set("num_batches", num_batches)
    result.set("rows_per_sec", round(total_rows / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, fqn))
    return result


def run_insert_i3_out_of_order(spark, params, strategy_key, dataset):
    """I-3: Out-of-order insert — shuffle rows before insert."""
    result = PerfResult(strategy_key, "I-3", params["dataset_size"])
    fqn = create_table(spark, params, strategy_key)

    shuffled = dataset.orderBy(F.rand())

    start = time.time()
    shuffled.writeTo(fqn).append()
    duration = elapsed(start)

    row_count = DATASET_SIZES[params["dataset_size"]]
    result.set("duration_s", duration)
    result.set("rows_per_sec", round(row_count / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, fqn))
    return result



def run_insert_i4_upsert(spark, params, strategy_key, dataset):
    """I-4: Upsert / merge — MERGE INTO with 10% updates, 90% inserts."""
    result = PerfResult(strategy_key, "I-4", params["dataset_size"])
    fqn = create_table(spark, params, strategy_key)

    # First, insert 10% of data as the base
    row_count = DATASET_SIZES[params["dataset_size"]]
    base_fraction = 0.10
    base_data = dataset.sample(withReplacement=False, fraction=base_fraction, seed=42)
    base_data.writeTo(fqn).append()

    # Build the merge source: full dataset (90% new + 10% overlap = updates)
    source_view = "merge_source"
    dataset.createOrReplaceTempView(source_view)

    start = time.time()
    spark.sql(f"""
        MERGE INTO {fqn} AS target
        USING {source_view} AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.event_ts   = source.event_ts,
            target.amount     = source.amount,
            target.payload    = source.payload,
            target.event_type = source.event_type
        WHEN NOT MATCHED THEN INSERT *
    """)
    duration = elapsed(start)

    result.set("duration_s", duration)
    result.set("rows_per_sec", round(row_count / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, fqn))
    return result


def run_insert_i5_concurrent(spark, params, strategy_key, dataset):
    """I-5: Concurrent writers — 4 parallel writers inserting disjoint data."""
    result = PerfResult(strategy_key, "I-5", params["dataset_size"])
    fqn = create_table(spark, params, strategy_key)

    # Split dataset into 4 disjoint partitions by id modulo
    splits = [
        dataset.filter(F.col("id") % 4 == i)
        for i in range(4)
    ]

    durations = []

    def write_split(split_df, idx):
        t = time.time()
        split_df.writeTo(fqn).append()
        return elapsed(t)

    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(write_split, splits[i], i): i
            for i in range(4)
        }
        for future in as_completed(futures):
            durations.append(future.result())
    total_duration = elapsed(start)

    row_count = DATASET_SIZES[params["dataset_size"]]
    result.set("duration_s", total_duration)
    result.set("per_writer_durations", durations)
    result.set("rows_per_sec", round(row_count / max(total_duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, fqn))
    return result


# ---------------------------------------------------------------------------
# QUERY Test Cases
# ---------------------------------------------------------------------------

def _run_query_n_times(spark, sql, n=5):
    """Run a query n times, return cold (first) and warm (avg of rest) timings."""
    timings = []
    for i in range(n):
        start = time.time()
        df = spark.sql(sql)
        df.collect()  # force materialisation
        timings.append(elapsed(start))

    cold = timings[0]
    warm = round(sum(timings[1:]) / max(len(timings) - 1, 1), 3) if len(timings) > 1 else cold
    return {"cold_s": cold, "warm_s": warm, "all_runs": timings}


def _get_scan_metrics(spark, fqn, sql):
    """Attempt to get files scanned / skipped from EXPLAIN."""
    metrics = {}
    try:
        plan = spark.sql(f"EXPLAIN EXTENDED {sql}").collect()[0][0]
        metrics["query_plan_snippet"] = plan[:2000]
    except Exception:
        pass
    return metrics


def run_query_q1_point_lookup(spark, fqn, params):
    """Q-1: Point lookup — WHERE id = ?"""
    row_count = DATASET_SIZES[params["dataset_size"]]
    target_id = row_count // 2
    sql = f"SELECT * FROM {fqn} WHERE id = {target_id}"

    result = PerfResult("", "Q-1", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result


def run_query_q2_time_range(spark, fqn, params):
    """Q-2: Time range scan — WHERE event_ts BETWEEN ? AND ?"""
    sql = f"""
        SELECT * FROM {fqn}
        WHERE event_ts BETWEEN '2022-06-01' AND '2022-06-30'
    """
    result = PerfResult("", "Q-2", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result


def run_query_q3_partition_aligned(spark, fqn, params, strategy_key):
    """Q-3: Partition-aligned filter — filter matches partition column exactly."""
    # Choose a filter that aligns with the partition strategy
    filter_map = {
        "unpartitioned":   "region = 'us-east-1'",
        "identity_date":   "created_date = DATE '2022-07-15'",
        "identity_region": "region = 'us-east-1'",
        "bucket_16_id":    "id = 500000",
        "truncate_10_id":  "id BETWEEN 1000 AND 1009",
        "month_ts":        "event_ts BETWEEN '2022-07-01' AND '2022-07-31'",
        "day_ts":          "event_ts BETWEEN '2022-07-15' AND '2022-07-15 23:59:59'",
        "composite":       "event_ts BETWEEN '2022-07-01' AND '2022-07-31' AND user_id = 'user_42'",
    }
    condition = filter_map.get(strategy_key, "region = 'us-east-1'")
    sql = f"SELECT * FROM {fqn} WHERE {condition}"

    result = PerfResult("", "Q-3", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result



def run_query_q4_cross_partition(spark, fqn, params):
    """Q-4: Cross-partition filter — filter on non-partition column."""
    sql = f"SELECT * FROM {fqn} WHERE event_type = 'purchase' AND amount > 5000"

    result = PerfResult("", "Q-4", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result


def run_query_q5_aggregation(spark, fqn, params):
    """Q-5: Aggregation — GROUP BY region, date_trunc('month', event_ts)."""
    sql = f"""
        SELECT region, date_trunc('month', event_ts) AS month,
               count(*) AS cnt, sum(amount) AS total_amount
        FROM {fqn}
        GROUP BY region, date_trunc('month', event_ts)
    """
    result = PerfResult("", "Q-5", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result


def run_query_q6_join(spark, fqn, dim_table, params):
    """Q-6: Join — join against dimension table on user_id."""
    sql = f"""
        SELECT t.*, d.user_name, d.signup_date
        FROM {fqn} t
        JOIN {dim_table} d ON t.user_id = d.user_id
        WHERE t.region = 'us-east-1'
    """
    result = PerfResult("", "Q-6", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result


def run_query_q7_full_scan(spark, fqn, params):
    """Q-7: Full table scan — SELECT count(*)."""
    sql = f"SELECT count(*) FROM {fqn}"

    result = PerfResult("", "Q-7", params["dataset_size"])
    timings = _run_query_n_times(spark, sql)
    result.metrics.update(timings)
    result.metrics.update(_get_scan_metrics(spark, fqn, sql))
    return result


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_insert_tests(spark, params, strategy_key, dataset):
    """Execute all insert test cases for a given strategy."""
    results = []
    print(f"\n{'='*60}")
    print(f"INSERT TESTS — Strategy: {strategy_key}")
    print(f"{'='*60}")

    tests = [
        ("I-1", run_insert_i1_bulk),
        ("I-2", run_insert_i2_incremental),
        ("I-3", run_insert_i3_out_of_order),
        ("I-4", run_insert_i4_upsert),
        ("I-5", run_insert_i5_concurrent),
    ]

    for test_id, test_fn in tests:
        print(f"\n  Running {test_id} for {strategy_key}...")
        try:
            r = test_fn(spark, params, strategy_key, dataset)
            r.strategy = strategy_key
            results.append(r)
            print(f"  {test_id} completed in {r.metrics.get('duration_s', '?')}s")
        except Exception as e:
            print(f"  {test_id} FAILED: {e}")
            err = PerfResult(strategy_key, test_id, params["dataset_size"])
            err.set("error", str(e))
            results.append(err)

    return results


def run_query_tests(spark, params, strategy_key, dim_table):
    """Execute all query test cases against an existing table."""
    fqn = table_fqn(params, strategy_key)
    results = []

    print(f"\n{'='*60}")
    print(f"QUERY TESTS — Strategy: {strategy_key}")
    print(f"{'='*60}")

    # Collect table statistics before query tests
    try:
        spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{fqn}')")
        print("  Compaction completed before query tests.")
    except Exception:
        print("  Skipping pre-query compaction (not critical).")

    query_tests = [
        ("Q-1", lambda: run_query_q1_point_lookup(spark, fqn, params)),
        ("Q-2", lambda: run_query_q2_time_range(spark, fqn, params)),
        ("Q-3", lambda: run_query_q3_partition_aligned(spark, fqn, params, strategy_key)),
        ("Q-4", lambda: run_query_q4_cross_partition(spark, fqn, params)),
        ("Q-5", lambda: run_query_q5_aggregation(spark, fqn, params)),
        ("Q-6", lambda: run_query_q6_join(spark, fqn, dim_table, params)),
        ("Q-7", lambda: run_query_q7_full_scan(spark, fqn, params)),
    ]

    for test_id, test_fn in query_tests:
        print(f"\n  Running {test_id} for {strategy_key}...")
        try:
            r = test_fn()
            r.strategy = strategy_key
            results.append(r)
            print(f"  {test_id} cold={r.metrics.get('cold_s','?')}s  warm={r.metrics.get('warm_s','?')}s")
        except Exception as e:
            print(f"  {test_id} FAILED: {e}")
            err = PerfResult(strategy_key, test_id, params["dataset_size"])
            err.set("error", str(e))
            results.append(err)

    return results



def save_results(spark, results, params):
    """Persist all results as JSON to S3."""
    results_data = [r.to_dict() for r in results]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = f"{params['results_path']}{params['dataset_size']}/{ts}"

    results_json = json.dumps(results_data, indent=2, default=str)
    rdd = spark.sparkContext.parallelize([results_json])
    rdd.saveAsTextFile(f"{output_path}/results.json")

    # Also save as a Spark-readable table for easier analysis
    results_df = spark.createDataFrame(
        [(json.dumps(r, default=str),) for r in results_data],
        ["result_json"]
    )
    results_df.write.mode("append").json(f"{output_path}/results_table")

    print(f"\nResults saved to {output_path}")
    return output_path


def print_summary(results):
    """Print a human-readable summary to stdout."""
    print("\n" + "=" * 80)
    print("PERFORMANCE TEST SUMMARY")
    print("=" * 80)

    insert_results = [r for r in results if r.test_id.startswith("I-")]
    query_results = [r for r in results if r.test_id.startswith("Q-")]

    if insert_results:
        print("\nINSERT RESULTS")
        print("-" * 70)
        print(f"{'Strategy':<20} {'Test':<6} {'Duration(s)':<14} {'Rows/sec':<14} {'Files':<8} {'Avg Size(MB)':<14}")
        print("-" * 70)
        for r in insert_results:
            m = r.metrics
            stats = m.get("table_stats", {})
            print(f"{r.strategy:<20} {r.test_id:<6} "
                  f"{m.get('duration_s', 'ERR'):<14} "
                  f"{m.get('rows_per_sec', 'ERR'):<14} "
                  f"{stats.get('file_count', '?'):<8} "
                  f"{stats.get('avg_file_size_mb', '?'):<14}")

    if query_results:
        print("\nQUERY RESULTS")
        print("-" * 70)
        print(f"{'Strategy':<20} {'Test':<6} {'Cold(s)':<10} {'Warm(s)':<10}")
        print("-" * 70)
        for r in query_results:
            m = r.metrics
            print(f"{r.strategy:<20} {r.test_id:<6} "
                  f"{m.get('cold_s', 'ERR'):<10} "
                  f"{m.get('warm_s', 'ERR'):<10}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    params = get_params()
    spark = init_spark(params)

    print(f"Iceberg Partition Performance Test")
    print(f"  Dataset size : {params['dataset_size']} ({DATASET_SIZES[params['dataset_size']]:,} rows)")
    print(f"  Test scope   : {params['test_scope']}")
    print(f"  Database     : {params['database']}")
    print(f"  Warehouse    : {params['warehouse_path']}")

    # Record cluster info
    sc = spark.sparkContext
    print(f"  Executors    : {sc._jsc.sc().getExecutorMemoryStatus().size()}")
    print(f"  Spark version: {sc.version}")

    all_results = []

    # Generate dataset once and cache it
    num_rows = DATASET_SIZES[params["dataset_size"]]
    print(f"\nGenerating {num_rows:,} row dataset...")
    dataset = generate_dataset(spark, num_rows)
    dataset.cache()
    dataset.count()  # materialise cache
    print("Dataset cached.")

    # Create dimension table for join queries
    dim_table = generate_dimension_table(spark, params)

    for strategy_key in PARTITION_STRATEGIES:
        print(f"\n{'#'*60}")
        print(f"# STRATEGY: {strategy_key}")
        print(f"{'#'*60}")

        if params["test_scope"] in ("all", "insert"):
            insert_results = run_insert_tests(spark, params, strategy_key, dataset)
            all_results.extend(insert_results)

        if params["test_scope"] in ("all", "query"):
            # For query-only runs, ensure the table exists with data
            fqn = table_fqn(params, strategy_key)
            try:
                count = spark.sql(f"SELECT count(*) FROM {fqn}").collect()[0][0]
                if count == 0:
                    raise Exception("Empty table")
            except Exception:
                print(f"  Table {fqn} not found or empty — inserting data first...")
                create_table(spark, params, strategy_key)
                dataset.writeTo(fqn).append()

            query_results = run_query_tests(spark, params, strategy_key, dim_table)
            all_results.extend(query_results)

    # Save and summarise
    save_results(spark, all_results, params)
    print_summary(all_results)

    # Cleanup cached data
    dataset.unpersist()
    print("\nPerformance test complete.")


if __name__ == "__main__":
    main()
