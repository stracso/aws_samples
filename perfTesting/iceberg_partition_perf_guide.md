# Performance Testing Guide: Iceberg Tables with Partition Strategies

## Objective

Evaluate insert and query performance across different Apache Iceberg partition strategies to determine optimal configurations for various workload patterns.

## Test Matrix

### Partition Strategies to Test

| Strategy | Description | Best For |
|---|---|---|
| Unpartitioned | No partitioning applied | Baseline comparison |
| Identity partition | Partition by raw column value (e.g., `date`, `region`) | Low-cardinality columns |
| Bucket partition | Hash-based bucketing (e.g., `bucket(16, id)`) | High-cardinality columns |
| Truncate partition | Truncate values (e.g., `truncate(10, id)`) | Range-like grouping on strings/ints |
| Time-based partition | `year()`, `month()`, `day()`, `hour()` transforms | Time-series data |
| Composite partition | Combination of transforms (e.g., `month(ts), bucket(8, user_id)`) | Multi-dimensional access patterns |

### Dataset Specifications

| Parameter | Small | Medium | Large |
|---|---|---|---|
| Row count | 1M | 100M | 1B |
| File format | Parquet | Parquet | Parquet |
| Row group size | 128 MB | 128 MB | 128 MB |
| Columns | 15–20 | 15–20 | 15–20 |

Use a consistent schema across all tests:

```sql
CREATE TABLE perf_test (
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
PARTITIONED BY (<strategy_under_test>);
```

## Test Scenarios

### 1. Insert Performance

#### Metrics to Capture

- Wall-clock time for batch insert
- Throughput (rows/sec, MB/sec)
- Number of data files created
- Number of partitions created
- Average file size
- Commit duration (catalog overhead)

#### Test Cases

| ID | Test | Details |
|---|---|---|
| I-1 | Bulk append | Insert full dataset in one operation |
| I-2 | Incremental append | Insert in 100 equal-sized batches |
| I-3 | Out-of-order insert | Shuffle rows before insert (stress partition sorting) |
| I-4 | Upsert / merge | MERGE INTO with 10% updates, 90% inserts |
| I-5 | Concurrent writers | 4 parallel writers inserting disjoint data |

#### Procedure

1. Create a fresh table for each partition strategy.
2. Record pre-insert catalog state (snapshot count, manifest count).
3. Execute the insert workload, capturing wall time and Spark/Trino metrics.
4. Record post-insert file statistics via `SELECT * FROM <table>.files`.
5. Record manifest and snapshot counts via `<table>.snapshots` and `<table>.manifests`.

### 2. Query Performance

#### Metrics to Capture

- Wall-clock query time (cold and warm cache)
- Files scanned vs. files skipped (partition pruning effectiveness)
- Data scanned (bytes)
- Planning time vs. execution time
- Peak memory usage

#### Test Cases

| ID | Test | Query Pattern |
|---|---|---|
| Q-1 | Point lookup | `WHERE id = ?` |
| Q-2 | Time range scan | `WHERE event_ts BETWEEN ? AND ?` |
| Q-3 | Partition-aligned filter | Filter matches partition column exactly |
| Q-4 | Cross-partition filter | Filter on non-partition column |
| Q-5 | Aggregation | `GROUP BY region, date_trunc('month', event_ts)` |
| Q-6 | Join | Join against a dimension table on `user_id` |
| Q-7 | Full table scan | `SELECT count(*) FROM table` |

#### Procedure

1. Flush OS and engine caches before cold-run tests.
2. Run each query 5 times; discard first run for warm-cache metrics.
3. Collect query plans (`EXPLAIN` / `EXPLAIN ANALYZE`) to verify pruning.
4. Record files scanned from Iceberg scan metrics or engine UI.

## Environment Setup

### Engine Options

Test on at least one of:

- Apache Spark 3.x + `iceberg-spark-runtime`
- Trino + Iceberg connector
- (Optional) Flink for streaming insert tests

### Catalog

- Use a consistent catalog (Glue, Hive Metastore, or REST catalog).
- Document catalog latency separately if it becomes a bottleneck.

### Infrastructure

- Pin instance types and cluster size across all runs.
- Disable autoscaling during tests.
- Record instance type, executor count, memory, and core count.

### Reproducibility Checklist

- [ ] Same dataset and schema for every partition strategy
- [ ] Same cluster configuration for every run
- [ ] Caches cleared between cold-run measurements
- [ ] Compaction disabled during insert tests (run separately if needed)
- [ ] Table statistics collected (`ANALYZE TABLE`) before query tests

## Reporting Template

For each partition strategy, record results in this format:

```
Strategy:       <name>
Dataset size:   <rows / GB>
Engine:         <Spark / Trino / Flink>
Cluster:        <instance type x count>

INSERT RESULTS
-------------------------------------------------
Test ID | Duration (s) | Rows/sec | Files Created | Avg File Size
I-1     |              |          |               |
I-2     |              |          |               |
...

QUERY RESULTS
-------------------------------------------------
Test ID | Cold (s) | Warm (s) | Files Scanned | Files Skipped | Data Scanned (MB)
Q-1     |          |          |               |               |
Q-2     |          |          |               |               |
...
```

## Analysis Guidelines

When comparing results across strategies, focus on:

1. Pruning ratio — `files_skipped / total_files`. Higher is better for selective queries.
2. Small file problem — strategies that produce too many small files hurt scan performance and increase catalog pressure.
3. Insert overhead — complex partition transforms add sorting cost at write time.
4. Skew — check if any partitions are disproportionately large; use `SELECT partition, count(*) FROM <table>.files GROUP BY partition`.
5. Maintenance cost — factor in compaction frequency needed to keep file sizes healthy.

## Recommended Comparison Summary

After all runs, produce a heatmap or table like:

| Strategy | Insert Throughput | Point Lookup | Range Scan | Aggregation | File Count | Avg File Size |
|---|---|---|---|---|---|---|
| Unpartitioned | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ |
| Identity (date) | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ |
| Bucket (16, id) | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ |
| Month (ts) | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ |
| Composite | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ | ⬛ |

Rate each cell as: 🟢 Good / 🟡 Acceptable / 🔴 Poor relative to the best result in that column.
