# Iceberg Table Optimization Testing Guide

## Objective

Evaluate the impact of Apache Iceberg maintenance operations on query performance, storage efficiency, and metadata overhead. Each scenario sets up a "degraded" table state, runs the optimization, and measures before/after metrics.

## Common Setup

### Prerequisites

- Apache Spark 3.x + `iceberg-spark-runtime` or AWS Glue with Iceberg support
- Catalog: Glue Iceberg catalog (or Hive/REST catalog)
- A pre-populated test table with enough history to exercise each optimization (see **Baseline Table Preparation** below)

### Baseline Table Preparation

Create a table and simulate realistic wear by running many small appends, updates, and deletes over time:

```sql
CREATE TABLE optim_test_db.events (
    id              BIGINT,
    event_ts        TIMESTAMP,
    user_id         STRING,
    region          STRING,
    event_type      STRING,
    payload         STRING,
    amount          DECIMAL(18,2),
    created_date    DATE
)
USING iceberg
PARTITIONED BY (days(event_ts));
```

Then run 200+ small batch inserts (1 000–10 000 rows each) to create a large number of small files, followed by a handful of `DELETE` and `UPDATE` statements to generate delete files and orphaned data.

### Metrics to Capture (All Scenarios)

| Metric | How to Collect |
|---|---|
| Total data files | `SELECT count(*) FROM optim_test_db.events.files` |
| Total delete files | `SELECT count(*) FROM optim_test_db.events.delete_files` |
| Snapshot count | `SELECT count(*) FROM optim_test_db.events.snapshots` |
| Manifest count | `SELECT count(*) FROM optim_test_db.events.manifests` |
| Total table size (bytes) | Sum `file_size_in_bytes` from `.files` |
| Avg file size | `SELECT avg(file_size_in_bytes) FROM optim_test_db.events.files` |
| Benchmark query time | Run a standard set of queries (point lookup, range scan, full scan) 5× each |

Record all metrics **before** and **after** each optimization.

---

## Scenario 1 — Compaction (Rewrite Data Files)

### Goal

Merge many small data files into fewer, optimally-sized files to improve scan performance and reduce planning overhead.

### Degraded State Setup

Run 500 micro-batch appends of ~2 000 rows each so the table contains hundreds of small files (avg < 5 MB).

### Procedure

1. Record baseline metrics.
2. Run compaction:

```sql
CALL spark_catalog.system.rewrite_data_files(
    table => 'optim_test_db.events',
    options => map('target-file-size-bytes', '134217728')  -- 128 MB target
);
```

3. Record post-compaction metrics.
4. Re-run benchmark queries and compare.

### Variations

| ID | Variation | Details |
|---|---|---|
| C-1 | Default target size (128 MB) | Baseline compaction run |
| C-2 | Smaller target (32 MB) | Useful for partition-heavy tables |
| C-3 | Bin-pack strategy | `strategy => 'binpack'` (default) |
| C-4 | Sort strategy | `strategy => 'sort'`, `sort-order => 'event_ts ASC'` |
| C-5 | Partial compaction | `where => 'created_date >= current_date - 7'` — only recent partitions |
| C-6 | Z-order compaction | `strategy => 'sort'`, `sort-order => 'zorder(region, event_ts)'` |

### What to Look For

- Reduction in file count and increase in avg file size
- Query speedup from fewer file opens and better predicate pushdown (sort/z-order)
- Write amplification cost (time and bytes rewritten)

---

## Scenario 2 — Expire Snapshots

### Goal

Remove old snapshots and their associated metadata to reduce catalog size and planning time.

### Degraded State Setup

Accumulate 200+ snapshots by running many small commits (appends, deletes, compactions).

### Procedure

1. Record snapshot and manifest counts.
2. Expire snapshots:

```sql
CALL spark_catalog.system.expire_snapshots(
    table => 'optim_test_db.events',
    older_than => TIMESTAMP '2026-04-01 00:00:00',
    retain_last => 5
);
```

3. Record post-expiry snapshot/manifest counts.
4. Measure planning time improvement on benchmark queries.

### Variations

| ID | Variation | Details |
|---|---|---|
| ES-1 | Retain last 5 | Keep only recent snapshots |
| ES-2 | Retain last 1 | Aggressive cleanup |
| ES-3 | Age-based only | `older_than` without `retain_last` |
| ES-4 | Dry run | Count snapshots that *would* be expired without actually removing them |

### What to Look For

- Reduction in snapshot and manifest count
- Decrease in metadata file sizes on storage
- Faster query planning (fewer manifests to read)
- Confirm time-travel queries to retained snapshots still work

---

## Scenario 3 — Remove Orphan Files

### Goal

Delete data files on storage that are no longer referenced by any snapshot, reclaiming wasted space.

### Degraded State Setup

1. Run compaction (which rewrites files but doesn't delete originals until snapshots expire).
2. Expire snapshots so old file references are dropped.
3. At this point, unreferenced files remain on S3/storage.

### Procedure

1. List orphan files (dry run):

```sql
CALL spark_catalog.system.remove_orphan_files(
    table => 'optim_test_db.events',
    dry_run => true
);
```

2. Record count and total size of orphan files.
3. Execute removal:

```sql
CALL spark_catalog.system.remove_orphan_files(
    table => 'optim_test_db.events',
    older_than => TIMESTAMP '2026-04-08 00:00:00'
);
```

4. Verify storage size reduction (e.g., `aws s3 ls --summarize --recursive`).

### Variations

| ID | Variation | Details |
|---|---|---|
| OF-1 | Default age threshold | Only files older than 3 days |
| OF-2 | Aggressive threshold | Files older than 1 hour (use with caution) |
| OF-3 | Specific location | Provide `location` param to target a subfolder |

### What to Look For

- Storage bytes reclaimed
- No impact on query correctness (all referenced files must remain)
- Time taken to scan storage vs. metadata for orphan detection

---

## Scenario 4 — Rewrite Manifests

### Goal

Consolidate manifest files to speed up query planning by reducing the number of manifests the engine must read.

### Degraded State Setup

Many small commits produce one manifest each. After 500 appends the table may have 500+ manifests.

### Procedure

1. Record manifest count.
2. Rewrite manifests:

```sql
CALL spark_catalog.system.rewrite_manifests(
    table => 'optim_test_db.events'
);
```

3. Record new manifest count.
4. Measure query planning time before and after.

### Variations

| ID | Variation | Details |
|---|---|---|
| RM-1 | Default | Let Iceberg decide manifest sizing |
| RM-2 | With spec ID filter | Rewrite only manifests for a specific partition spec |

### What to Look For

- Manifest count reduction (often 10×–100× fewer)
- Planning time improvement on complex queries
- No change in query results

---

## Scenario 5 — Delete File Compaction (Rewrite Position Deletes)

### Goal

Merge position-delete files into the base data files so reads no longer need to apply deletes at query time.

### Degraded State Setup

Run row-level `DELETE` or `UPDATE` statements against a v2 (merge-on-read) table to generate position-delete files:

```sql
DELETE FROM optim_test_db.events WHERE event_type = 'deprecated';
UPDATE optim_test_db.events SET amount = 0 WHERE region = 'test-region';
```

### Procedure

1. Record delete file count and benchmark query time.
2. Compact delete files:

```sql
CALL spark_catalog.system.rewrite_data_files(
    table => 'optim_test_db.events',
    options => map('delete-file-threshold', '1')
);
```

3. Record post-compaction delete file count and re-run benchmarks.

### Variations

| ID | Variation | Details |
|---|---|---|
| DC-1 | Low delete ratio (~1%) | Few deletes spread across many files |
| DC-2 | High delete ratio (~30%) | Heavy deletes concentrated in some partitions |
| DC-3 | Mixed updates + deletes | Combination of UPDATE and DELETE generating both delete files and new data files |

### What to Look For

- Elimination (or reduction) of delete files
- Read performance improvement (no merge-on-read overhead)
- Write amplification from rewriting data files that had deletes applied

---

## Scenario 6 — Full Maintenance Pipeline (Combined)

### Goal

Run all optimizations in the recommended order and measure cumulative impact.

### Recommended Order

1. **Compact data files** — merge small files
2. **Rewrite manifests** — consolidate metadata
3. **Expire snapshots** — drop old snapshots
4. **Remove orphan files** — reclaim storage

### Procedure

1. Record all baseline metrics.
2. Execute each step in order, recording metrics after each.
3. Run full benchmark suite at the end.

### Reporting Template

```
Step                  | Files | Delete Files | Snapshots | Manifests | Table Size (GB) | Avg Query Time (s)
----------------------|-------|--------------|-----------|-----------|-----------------|--------------------
Baseline              |       |              |           |           |                 |
After Compaction      |       |              |           |           |                 |
After Rewrite Manifests|      |              |           |           |                 |
After Expire Snapshots|       |              |           |           |                 |
After Remove Orphans  |       |              |           |           |                 |
```

---

## Analysis Guidelines

When comparing results across scenarios:

1. **Query speedup** — primary success metric. Compare cold and warm query times.
2. **Storage savings** — bytes reclaimed after orphan removal and compaction.
3. **Metadata overhead** — manifest and snapshot count reduction directly impacts planning time.
4. **Write amplification** — compaction rewrites data; measure bytes written vs. bytes saved.
5. **Safety** — always verify query correctness after each optimization (row counts, checksums).
6. **Scheduling cadence** — based on results, recommend how often each maintenance task should run (e.g., compact daily, expire snapshots weekly, remove orphans monthly).
