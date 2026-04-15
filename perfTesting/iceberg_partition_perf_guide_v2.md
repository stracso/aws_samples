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


- For each partition type, we will specify a tablename to use and the coresponding partition column(s)


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
3. Execute the insert workload, capturing wall time and Spark metrics.
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
| Q-1 | Point lookup | `WHERE <PARTITION_COLUMN> = ?` |
| Q-2 | Time range scan | `WHERE <PARTITION_COLUMN> BETWEEN ? AND ?` |
| Q-3 | Cross-partition filter | Filter on non-partition column |
| Q-4 | Aggregation | `GROUP BY <PARTITION_COLUMN1>` |
| Q-5 | Full table scan | `SELECT count(*) FROM table` |

#### Procedure

1. Flush OS and engine caches before cold-run tests.
2. Run each query 5 times; discard first run for warm-cache metrics.
3. Collect query plans (`EXPLAIN` / `EXPLAIN ANALYZE`) to verify pruning.
4. Record files scanned from Iceberg scan metrics or engine UI.

## Environment Setup

### Engine Options

Test on at least one of:

- Apache Spark 3.x + `iceberg-spark-runtime`
- DuckDB + Iceberg extension

### Catalog

- Use a consistent catalog (Glue Icebergcatalog).

### Infrastructure

- Record Worker type and NUm of workers for AWS Glue.


## Reporting Template

For each partition strategy, record results in this format:

```
Strategy:       <name>
Dataset size:   <rows / GB>
Engine:         <Spark / DuckDB>

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


