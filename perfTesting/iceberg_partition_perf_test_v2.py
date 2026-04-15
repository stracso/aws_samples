"""
Iceberg Partition Performance Testing v2 - AWS Glue PySpark Script

Executes INSERT and QUERY performance tests across different Iceberg partition
strategies as defined in iceberg_partition_perf_guide_v2.md.

Table names for each partition strategy are passed as configuration parameters
rather than being auto-generated. For INSERT tests, an INPUT table containing
the test data is also specified.

Usage:
  Run as an AWS Glue job with the following job parameters:

    --DATABASE_NAME         : Glue catalog database (default: perf_test_db)
    --TEST_SCOPE            : insert | query (required — run one at a time)
    --RESULTS_PATH          : S3 path for results output (required)

    # INSERT-specific parameters
    --INPUT_TABLE           : Fully-qualified Iceberg table with source data
                              (required when TEST_SCOPE=insert)

    # Table names per partition strategy (provide one or more):
    --TABLE_UNPARTITIONED   : Table name for unpartitioned strategy
    --TABLE_IDENTITY        : Table name for identity partition strategy
    --TABLE_BUCKET          : Table name for bucket partition strategy
    --TABLE_TRUNCATE        : Table name for truncate partition strategy
    --TABLE_TIME_BASED      : Table name for time-based partition strategy
    --TABLE_COMPOSITE       : Table name for composite partition strategy

    # Partition column overrides (comma-separated for composite):
    --PARTITION_COL_IDENTITY   : Column for identity partition (default: created_date)
    --PARTITION_COL_BUCKET     : e.g. bucket(16, id)
    --PARTITION_COL_TRUNCATE   : e.g. truncate(10, id)
    --PARTITION_COL_TIME_BASED : e.g. month(event_ts)
    --PARTITION_COL_COMPOSITE  : e.g. month(event_ts), bucket(8, user_id)

    # Query-specific parameters
    --QUERY_PARTITION_FILTER   : Filter expression for Q-1 point lookup
    --QUERY_RANGE_FILTER       : Filter expression for Q-2 range scan
    --QUERY_AGG_COLUMN         : Column for Q-4 GROUP BY aggregation

Example:
  aws glue start-job-run --job-name perf-test-v2 --arguments '{
    "--TEST_SCOPE": "insert",
    "--DATABASE_NAME": "perf_test_db",
    "--INPUT_TABLE": "glue_catalog.perf_test_db.source_data",
    "--TABLE_UNPARTITIONED": "glue_catalog.perf_test_db.perf_unpart",
    "--TABLE_IDENTITY": "glue_catalog.perf_test_db.perf_identity",
    "--PARTITION_COL_IDENTITY": "created_date",
    "--RESULTS_PATH": "s3://my-bucket/perf_results/"
  }'
"""

import sys
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions


# ---------------------------------------------------------------------------
# Configuration & Parameter Parsing
# ---------------------------------------------------------------------------

# Map of strategy key -> (table param name, partition col param name, default partition expr)
STRATEGY_CONFIG = {
    "unpartitioned": ("TABLE_UNPARTITIONED", None, ""),
    "identity":      ("TABLE_IDENTITY",      "PARTITION_COL_IDENTITY",   "created_date"),
    "bucket":        ("TABLE_BUCKET",        "PARTITION_COL_BUCKET",     "bucket(16, id)"),
    "truncate":      ("TABLE_TRUNCATE",      "PARTITION_COL_TRUNCATE",   "truncate(10, id)"),
    "time_based":    ("TABLE_TIME_BASED",    "PARTITION_COL_TIME_BASED", "month(event_ts)"),
    "composite":     ("TABLE_COMPOSITE",     "PARTITION_COL_COMPOSITE",  "month(event_ts), bucket(8, user_id)"),
}


def get_params():
    """Parse Glue job parameters. Only required params are resolved; optional
    ones are read from sys.argv manually to avoid Glue's missing-key error."""
    # Resolve only the always-required params
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    # Build a dict from all --key value pairs in sys.argv
    all_args = {}
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        if argv[i].startswith("--"):
            key = argv[i][2:]
            val = argv[i + 1] if i + 1 < len(argv) and not argv[i + 1].startswith("--") else ""
            all_args[key] = val
            i += 2
        else:
            i += 1

    params = {
        "job_name":     args["JOB_NAME"],
        "database":     all_args.get("DATABASE_NAME", "perf_test_db"),
        "test_scope":   all_args.get("TEST_SCOPE", "insert"),
        "results_path": all_args.get("RESULTS_PATH", ""),
        "input_table":  all_args.get("INPUT_TABLE", ""),
    }

    # Query-specific overrides
    params["query_partition_filter"] = all_args.get("QUERY_PARTITION_FILTER", "")
    params["query_range_filter"]     = all_args.get("QUERY_RANGE_FILTER", "")
    params["query_agg_column"]       = all_args.get("QUERY_AGG_COLUMN", "")

    # Discover which strategies the user wants to test based on provided table names
    params["strategies"] = {}
    for strategy_key, (table_param, col_param, default_col) in STRATEGY_CONFIG.items():
        table_name = all_args.get(table_param, "")
        if table_name:
            partition_expr = ""
            if col_param:
                partition_expr = all_args.get(col_param, default_col)
            params["strategies"][strategy_key] = {
                "table": table_name,
                "partition_expr": partition_expr,
            }

    return params


def init_spark(params):
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
    return spark


def elapsed(start):
    return round(time.time() - start, 3)


# ---------------------------------------------------------------------------
# Result Tracking
# ---------------------------------------------------------------------------

class PerfResult:
    """Accumulates results for a single test run."""

    def __init__(self, strategy, test_id):
        self.strategy = strategy
        self.test_id = test_id
        self.metrics = {}

    def set(self, key, value):
        self.metrics[key] = value

    def to_dict(self):
        return {
            "strategy": self.strategy,
            "test_id": self.test_id,
            "timestamp": datetime.utcnow().isoformat(),
            **self.metrics,
        }


# ---------------------------------------------------------------------------
# Table Management
# ---------------------------------------------------------------------------

def clear_table(spark, table_name):
    """Delete all rows from an existing Iceberg table to prepare for a fresh insert test."""
    print(f"  Clearing table {table_name}...")
    spark.sql(f"DELETE FROM {table_name}")
    print(f"  Table {table_name} cleared.")


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
# INSERT Test Cases (I-1 through I-5)
# ---------------------------------------------------------------------------

def run_insert_i1_bulk(spark, dataset, target_table):
    """I-1: Bulk append — insert full dataset in one operation."""
    row_count = dataset.count()
    start = time.time()
    dataset.writeTo(target_table).append()
    duration = elapsed(start)

    result = PerfResult("", "I-1")
    result.set("duration_s", duration)
    result.set("row_count", row_count)
    result.set("rows_per_sec", round(row_count / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, target_table))
    return result


def run_insert_i2_incremental(spark, dataset, target_table):
    """I-2: Incremental append — insert in 100 equal-sized batches."""
    num_batches = 100
    total_rows = dataset.count()
    fraction = 1.0 / num_batches

    start = time.time()
    for i in range(num_batches):
        batch = dataset.sample(withReplacement=False, fraction=fraction, seed=i)
        batch.writeTo(target_table).append()
    duration = elapsed(start)

    result = PerfResult("", "I-2")
    result.set("duration_s", duration)
    result.set("num_batches", num_batches)
    result.set("row_count", total_rows)
    result.set("rows_per_sec", round(total_rows / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, target_table))
    return result


def run_insert_i3_out_of_order(spark, dataset, target_table):
    """I-3: Out-of-order insert — shuffle rows before insert."""
    row_count = dataset.count()
    shuffled = dataset.orderBy(F.rand())

    start = time.time()
    shuffled.writeTo(target_table).append()
    duration = elapsed(start)

    result = PerfResult("", "I-3")
    result.set("duration_s", duration)
    result.set("row_count", row_count)
    result.set("rows_per_sec", round(row_count / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, target_table))
    return result


def run_insert_i4_upsert(spark, dataset, target_table):
    """I-4: Upsert / merge — MERGE INTO with 10% updates, 90% inserts.
    First seeds the table with 10% of the data, then merges the full dataset."""
    row_count = dataset.count()

    # Seed with 10% as the base
    base_data = dataset.sample(withReplacement=False, fraction=0.10, seed=42)
    base_data.writeTo(target_table).append()

    # Use the full dataset as merge source (overlapping 10% = updates, rest = inserts)
    source_view = "merge_source_v2"
    dataset.createOrReplaceTempView(source_view)

    # Build SET clause dynamically from all columns except the join key
    columns = [f.name for f in dataset.schema.fields]
    update_cols = [c for c in columns if c != "id"]
    set_clause = ", ".join(f"target.{c} = source.{c}" for c in update_cols)

    start = time.time()
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT *
    """)
    duration = elapsed(start)

    result = PerfResult("", "I-4")
    result.set("duration_s", duration)
    result.set("row_count", row_count)
    result.set("rows_per_sec", round(row_count / max(duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, target_table))
    return result


def run_insert_i5_concurrent(spark, dataset, target_table):
    """I-5: Concurrent writers — 4 parallel writers inserting disjoint data."""
    row_count = dataset.count()

    # Split dataset into 4 disjoint partitions by id modulo
    splits = [dataset.filter(F.col("id") % 4 == i) for i in range(4)]

    durations = []

    def write_split(split_df, idx):
        t = time.time()
        split_df.writeTo(target_table).append()
        return elapsed(t)

    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(write_split, splits[i], i): i for i in range(4)
        }
        for future in as_completed(futures):
            durations.append(future.result())
    total_duration = elapsed(start)

    result = PerfResult("", "I-5")
    result.set("duration_s", total_duration)
    result.set("per_writer_durations", durations)
    result.set("row_count", row_count)
    result.set("rows_per_sec", round(row_count / max(total_duration, 0.001)))
    result.set("table_stats", collect_table_stats(spark, target_table))
    return result


# ---------------------------------------------------------------------------
# QUERY Test Cases (Q-1 through Q-5)
# ---------------------------------------------------------------------------

def _run_query_n_times(spark, sql, n=5):
    """Run a query n times. Return cold (first run) and warm (avg of remaining)."""
    timings = []
    for _ in range(n):
        start = time.time()
        df = spark.sql(sql)
        df.collect()  # force materialisation
        timings.append(elapsed(start))

    cold = timings[0]
    warm = round(sum(timings[1:]) / max(len(timings) - 1, 1), 3) if len(timings) > 1 else cold
    return {"cold_s": cold, "warm_s": warm, "all_runs": timings}


def _get_scan_metrics(spark, sql):
    """Capture EXPLAIN output for pruning analysis."""
    metrics = {}
    try:
        plan = spark.sql(f"EXPLAIN EXTENDED {sql}").collect()[0][0]
        metrics["query_plan_snippet"] = plan[:2000]
    except Exception:
        pass
    return metrics


def run_query_q1_point_lookup(spark, fqn, params, partition_expr):
    """Q-1: Point lookup — WHERE <PARTITION_COLUMN> = ?
    Uses QUERY_PARTITION_FILTER if provided, otherwise derives from partition expr."""
    filter_expr = params.get("query_partition_filter", "")
    if not filter_expr:
        # Derive a sensible default from the partition expression
        filter_expr = _derive_point_filter(partition_expr)
    sql = f"SELECT * FROM {fqn} WHERE {filter_expr}"

    result = PerfResult("", "Q-1")
    result.set("sql", sql)
    result.metrics.update(_run_query_n_times(spark, sql))
    result.metrics.update(_get_scan_metrics(spark, sql))
    return result


def run_query_q2_range_scan(spark, fqn, params, partition_expr):
    """Q-2: Time range scan — WHERE <PARTITION_COLUMN> BETWEEN ? AND ?
    Uses QUERY_RANGE_FILTER if provided, otherwise derives from partition expr."""
    filter_expr = params.get("query_range_filter", "")
    if not filter_expr:
        filter_expr = _derive_range_filter(partition_expr)
    sql = f"SELECT * FROM {fqn} WHERE {filter_expr}"

    result = PerfResult("", "Q-2")
    result.set("sql", sql)
    result.metrics.update(_run_query_n_times(spark, sql))
    result.metrics.update(_get_scan_metrics(spark, sql))
    return result


def run_query_q3_cross_partition(spark, fqn):
    """Q-3: Cross-partition filter — filter on non-partition column."""
    sql = f"SELECT * FROM {fqn} WHERE event_type = 'purchase' AND amount > 5000"

    result = PerfResult("", "Q-3")
    result.set("sql", sql)
    result.metrics.update(_run_query_n_times(spark, sql))
    result.metrics.update(_get_scan_metrics(spark, sql))
    return result


def run_query_q4_aggregation(spark, fqn, params, partition_expr):
    """Q-4: Aggregation — GROUP BY <PARTITION_COLUMN>."""
    agg_col = params.get("query_agg_column", "")
    if not agg_col:
        agg_col = _extract_column_from_expr(partition_expr) or "region"
    sql = f"""
        SELECT {agg_col}, count(*) AS cnt, sum(amount) AS total_amount
        FROM {fqn}
        GROUP BY {agg_col}
    """

    result = PerfResult("", "Q-4")
    result.set("sql", sql)
    result.metrics.update(_run_query_n_times(spark, sql))
    result.metrics.update(_get_scan_metrics(spark, sql))
    return result


def run_query_q5_full_scan(spark, fqn):
    """Q-5: Full table scan — SELECT count(*) FROM table."""
    sql = f"SELECT count(*) FROM {fqn}"

    result = PerfResult("", "Q-5")
    result.set("sql", sql)
    result.metrics.update(_run_query_n_times(spark, sql))
    result.metrics.update(_get_scan_metrics(spark, sql))
    return result


# ---------------------------------------------------------------------------
# Filter derivation helpers
# ---------------------------------------------------------------------------

def _extract_column_from_expr(partition_expr):
    """Extract the raw column name from a partition expression.
    E.g. 'month(event_ts)' -> 'event_ts', 'bucket(16, id)' -> 'id',
         'created_date' -> 'created_date'."""
    if not partition_expr:
        return ""
    expr = partition_expr.split(",")[0].strip()  # take first for composite
    if "(" in expr:
        # e.g. month(event_ts) or bucket(16, id)
        inner = expr.split("(", 1)[1].rstrip(")")
        parts = [p.strip() for p in inner.split(",")]
        # The column is the last argument (bucket(N, col), truncate(N, col), month(col))
        return parts[-1]
    return expr


def _derive_point_filter(partition_expr):
    """Build a point-lookup WHERE clause from the partition expression."""
    col = _extract_column_from_expr(partition_expr)
    if not col:
        return "1=1"  # unpartitioned — no meaningful point filter
    # Heuristic: pick a reasonable literal based on common column names
    if "date" in col.lower():
        return f"{col} = DATE '2022-07-15'"
    if "ts" in col.lower() or "time" in col.lower():
        return f"{col} = TIMESTAMP '2022-07-15 12:00:00'"
    if col == "region":
        return f"{col} = 'us-east-1'"
    # Numeric / id columns
    return f"{col} = 500000"


def _derive_range_filter(partition_expr):
    """Build a range-scan WHERE clause from the partition expression."""
    col = _extract_column_from_expr(partition_expr)
    if not col:
        return "1=1"
    if "date" in col.lower():
        return f"{col} BETWEEN DATE '2022-06-01' AND DATE '2022-06-30'"
    if "ts" in col.lower() or "time" in col.lower():
        return f"{col} BETWEEN TIMESTAMP '2022-06-01 00:00:00' AND TIMESTAMP '2022-06-30 23:59:59'"
    if col == "region":
        return f"{col} IN ('us-east-1', 'us-west-2')"
    return f"{col} BETWEEN 100000 AND 200000"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_insert_tests(spark, params, strategy_key, strategy_cfg, dataset):
    """Execute all INSERT test cases (I-1 to I-5) for a given strategy.
    The target table must already exist; it is cleared before each test."""
    target_table = strategy_cfg["table"]
    partition_expr = strategy_cfg["partition_expr"]
    results = []

    print(f"\n{'='*60}")
    print(f"INSERT TESTS — Strategy: {strategy_key}  Table: {target_table}")
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
            # Clear the existing table before each test case
            clear_table(spark, target_table)
            r = test_fn(spark, dataset, target_table)
            r.strategy = strategy_key
            results.append(r)
            print(f"  {test_id} completed in {r.metrics.get('duration_s', '?')}s")
        except Exception as e:
            print(f"  {test_id} FAILED: {e}")
            import traceback; traceback.print_exc()
            err = PerfResult(strategy_key, test_id)
            err.set("error", str(e))
            results.append(err)

    return results


def run_query_tests(spark, params, strategy_key, strategy_cfg):
    """Execute all QUERY test cases (Q-1 to Q-5) against an existing table."""
    fqn = strategy_cfg["table"]
    partition_expr = strategy_cfg["partition_expr"]
    results = []

    print(f"\n{'='*60}")
    print(f"QUERY TESTS — Strategy: {strategy_key}  Table: {fqn}")
    print(f"{'='*60}")

    # Verify the table exists and has data
    try:
        count = spark.sql(f"SELECT count(*) FROM {fqn}").collect()[0][0]
        print(f"  Table row count: {count:,}")
        if count == 0:
            print(f"  WARNING: Table {fqn} is empty — query results will be trivial.")
    except Exception as e:
        print(f"  ERROR: Cannot read table {fqn}: {e}")
        err = PerfResult(strategy_key, "Q-ALL")
        err.set("error", f"Table not accessible: {e}")
        return [err]

    # Optional: compact before querying for fair comparison
    try:
        spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{fqn}')")
        print("  Compaction completed before query tests.")
    except Exception:
        print("  Skipping pre-query compaction (not critical).")

    query_tests = [
        ("Q-1", lambda: run_query_q1_point_lookup(spark, fqn, params, partition_expr)),
        ("Q-2", lambda: run_query_q2_range_scan(spark, fqn, params, partition_expr)),
        ("Q-3", lambda: run_query_q3_cross_partition(spark, fqn)),
        ("Q-4", lambda: run_query_q4_aggregation(spark, fqn, params, partition_expr)),
        ("Q-5", lambda: run_query_q5_full_scan(spark, fqn)),
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
            import traceback; traceback.print_exc()
            err = PerfResult(strategy_key, test_id)
            err.set("error", str(e))
            results.append(err)

    return results


# ---------------------------------------------------------------------------
# Results Persistence & Reporting
# ---------------------------------------------------------------------------

def save_results(spark, results, params):
    """Persist all results as JSON to S3."""
    results_data = [r.to_dict() for r in results]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = f"{params['results_path'].rstrip('/')}/{params['test_scope']}/{ts}"

    results_json = json.dumps(results_data, indent=2, default=str)
    rdd = spark.sparkContext.parallelize([results_json])
    rdd.saveAsTextFile(f"{output_path}/results.json")

    # Also save as Spark-readable JSON for easier analysis
    results_df = spark.createDataFrame(
        [(json.dumps(r, default=str),) for r in results_data],
        ["result_json"]
    )
    results_df.write.mode("overwrite").json(f"{output_path}/results_table")

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
        print("-" * 80)
        print(f"{'Strategy':<16} {'Test':<6} {'Duration(s)':<14} {'Rows/sec':<14} {'Files':<8} {'AvgSize(MB)':<12} {'Partitions':<10}")
        print("-" * 80)
        for r in insert_results:
            m = r.metrics
            stats = m.get("table_stats", {})
            print(
                f"{r.strategy:<16} {r.test_id:<6} "
                f"{m.get('duration_s', 'ERR'):<14} "
                f"{m.get('rows_per_sec', 'ERR'):<14} "
                f"{stats.get('file_count', '?'):<8} "
                f"{stats.get('avg_file_size_mb', '?'):<12} "
                f"{stats.get('partition_count', '?'):<10}"
            )

    if query_results:
        print("\nQUERY RESULTS")
        print("-" * 80)
        print(f"{'Strategy':<16} {'Test':<6} {'Cold(s)':<10} {'Warm(s)':<10}")
        print("-" * 80)
        for r in query_results:
            m = r.metrics
            print(
                f"{r.strategy:<16} {r.test_id:<6} "
                f"{m.get('cold_s', 'ERR'):<10} "
                f"{m.get('warm_s', 'ERR'):<10}"
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    params = get_params()
    spark = init_spark(params)

    scope = params["test_scope"]
    strategies = params["strategies"]

    print("Iceberg Partition Performance Test v2")
    print(f"  Test scope : {scope}")
    print(f"  Database   : {params['database']}")
    print(f"  Strategies : {list(strategies.keys())}")
    if scope == "insert":
        print(f"  Input table: {params['input_table']}")

    sc = spark.sparkContext
    print(f"  Executors  : {sc._jsc.sc().getExecutorMemoryStatus().size()}")
    print(f"  Spark ver  : {sc.version}")

    if not strategies:
        raise ValueError(
            "No strategies configured. Provide at least one --TABLE_<STRATEGY> parameter."
        )

    # For INSERT scope, load and cache the input dataset
    dataset = None
    if scope == "insert":
        if not params["input_table"]:
            raise ValueError("--INPUT_TABLE is required when TEST_SCOPE=insert")
        print(f"\nLoading input data from {params['input_table']}...")
        dataset = spark.table(params["input_table"])
        dataset.cache()
        row_count = dataset.count()
        print(f"  Input dataset: {row_count:,} rows cached.")

    all_results = []

    for strategy_key, strategy_cfg in strategies.items():
        print(f"\n{'#'*60}")
        print(f"# STRATEGY: {strategy_key}  ->  {strategy_cfg['table']}")
        print(f"#   Partition: {strategy_cfg['partition_expr'] or '(none)'}")
        print(f"{'#'*60}")

        if scope == "insert":
            results = run_insert_tests(spark, params, strategy_key, strategy_cfg, dataset)
            all_results.extend(results)
        elif scope == "query":
            results = run_query_tests(spark, params, strategy_key, strategy_cfg)
            all_results.extend(results)
        else:
            raise ValueError(f"Unknown TEST_SCOPE: {scope}. Use 'insert' or 'query'.")

    # Save and summarise
    if params["results_path"]:
        save_results(spark, all_results, params)
    print_summary(all_results)

    # Cleanup
    if dataset:
        dataset.unpersist()
    print("\nPerformance test v2 complete.")


if __name__ == "__main__":
    main()
