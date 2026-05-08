# Iceberg Schema Evolution Scenarios

This document covers common scenarios for Apache Iceberg schema evolution — adding columns, removing columns, and inserting data that conforms to an older schema into a table with a newer schema.

---

## Background

Iceberg tracks schema changes using unique column IDs rather than column names or positions. This means:

- Adding or removing columns never breaks existing data files.
- Old data files are read using the schema at the time they were written; missing columns resolve to `null`.
- Writes always target the **current** (latest) table schema.

---

## Scenario 1: Adding New Columns

### Setup

Starting schema (schema version 0):

| Column       | Type    |
|--------------|---------|
| id           | long    |
| name         | string  |
| created_at   | timestamp |

### Evolution

Add two columns:

```sql
ALTER TABLE catalog.db.events ADD COLUMNS (
  region string,
  event_type string
);
```

Schema version 1:

| Column       | Type      |
|--------------|-----------|
| id           | long      |
| name         | string    |
| created_at   | timestamp |
| region       | string    |
| event_type   | string    |

### Behavior

- Existing data files written under schema v0 still only contain `id`, `name`, `created_at`.
- When those files are read, `region` and `event_type` return `null`.
- New inserts must provide values (or nulls) for all current columns.

---

## Scenario 2: Removing Columns

### Evolution

Drop a column that is no longer needed:

```sql
ALTER TABLE catalog.db.events DROP COLUMN event_type;
```

Schema version 2:

| Column       | Type      |
|--------------|-----------|
| id           | long      |
| name         | string    |
| created_at   | timestamp |
| region       | string    |

### Behavior

- Data files written under schema v1 still physically contain `event_type`, but it is no longer projected during reads.
- No data rewrite is required — the column is simply hidden from the current schema.
- The column ID is retired and cannot be reused, preventing accidental type mismatches.

---

## Scenario 3: Inserting Old-Schema Data into the Latest Schema

This is the most common real-world challenge: you have a data pipeline or backfill job producing records that match an **older** schema, and you need to write them into a table whose schema has since evolved.

### Approach A: Let Iceberg Handle Nulls (Recommended)

If the new columns are **nullable**, simply insert the old data without the new columns. Iceberg (and the engine) will fill missing columns with `null`.

```sql
-- Table currently has: id, name, created_at, region
-- Old data only has: id, name, created_at

INSERT INTO catalog.db.events (id, name, created_at)
VALUES (1001, 'legacy_event', timestamp '2024-01-15 08:30:00');
```

`region` will be stored as `null` for this row.

#### Spark DataFrame example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

# Old data — missing 'region'
old_df = spark.read.parquet("s3://bucket/backfill/events_v0/")

# Add missing columns as null to match current schema
current_schema_columns = ["id", "name", "created_at", "region"]
for col_name in current_schema_columns:
    if col_name not in old_df.columns:
        old_df = old_df.withColumn(col_name, lit(None))

# Select in the correct order and write
old_df.select(current_schema_columns).writeTo("catalog.db.events").append()
```

### Approach B: Provide Default Values

If business logic requires non-null defaults for new columns, enrich the old data before inserting:

```python
from pyspark.sql.functions import lit

old_df = spark.read.parquet("s3://bucket/backfill/events_v0/")

enriched_df = old_df \
    .withColumn("region", lit("UNKNOWN"))

enriched_df.writeTo("catalog.db.events").append()
```

### Approach C: Schema Reconciliation with MERGE

Use `MERGE INTO` when you want upsert semantics and the source data may overlap with existing records:

```sql
MERGE INTO catalog.db.events AS target
USING old_events_staging AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    created_at = source.created_at
    -- region stays unchanged
WHEN NOT MATCHED THEN INSERT (id, name, created_at, region)
    VALUES (source.id, source.name, source.created_at, NULL);
```

---

## Scenario 4: Combined Evolution — Add and Remove in Sequence

A realistic lifecycle where a table goes through multiple schema changes:

```
v0: id, name, created_at
v1: id, name, created_at, region, event_type        (added region, event_type)
v2: id, name, created_at, region                     (dropped event_type)
v3: id, name, created_at, region, priority (int)     (added priority)
```

### Reading across versions

| Data written at | Columns physically stored              | Columns projected at v3                     |
|-----------------|----------------------------------------|---------------------------------------------|
| v0              | id, name, created_at                   | id, name, created_at, null, null            |
| v1              | id, name, created_at, region, event_type | id, name, created_at, region, null        |
| v2              | id, name, created_at, region           | id, name, created_at, region, null          |
| v3              | id, name, created_at, region, priority | id, name, created_at, region, priority      |

### Inserting v0-era data into v3 table

```python
old_df = spark.read.parquet("s3://bucket/archive/events_v0/")

# Align to current schema v3
aligned_df = old_df \
    .withColumn("region", lit(None).cast("string")) \
    .withColumn("priority", lit(None).cast("int"))

aligned_df.writeTo("catalog.db.events").append()
```

---

## Scenario 5: Using Branching for Safe Schema Evolution

Iceberg **branches** (part of the table's ref/tag system) let you evolve the schema on an isolated branch, validate the change, and then fast-forward the main branch — all without affecting production readers until you're ready.

### Why Use Branching?

- **Safe experimentation** — test schema changes and backfill data without impacting the `main` snapshot.
- **Rollback** — if something goes wrong, simply drop the branch; `main` is untouched.
- **Parallel pipelines** — one team can write old-schema data to `main` while another prepares the new schema on a branch.
- **Validation window** — QA or downstream consumers can read from the branch to verify before promotion.

### Step 1: Create a Branch

```sql
ALTER TABLE catalog.db.events
CREATE BRANCH schema_v4
RETAIN 7 DAYS;
```

This creates a named branch pointing at the current snapshot of `main`. The `RETAIN` clause auto-expires the branch if you forget to clean up.

### Step 2: Evolve the Schema on the Branch

Write to the branch by setting the write reference:

```python
spark.conf.set("spark.wap.branch", "schema_v4")

# Or per-statement in Spark 3.4+:
spark.sql("""
    ALTER TABLE catalog.db.events
    ADD COLUMNS (priority int, source_system string)
""")
```

> **Note:** In Spark, schema DDL applies to the table globally (schema is shared across branches). The branch isolates *data* snapshots, not the schema definition itself. So the typical pattern is:
> 1. Create the branch.
> 2. Evolve the schema (this affects all branches).
> 3. Write new-schema data **only** to the branch.
> 4. Validate reads on the branch.
> 5. Fast-forward `main` once satisfied.

### Step 3: Write Old-Schema Data to the Branch

Backfill historical data that conforms to the old schema, filling new columns with nulls or defaults:

```python
from pyspark.sql.functions import lit

spark.conf.set("spark.wap.branch", "schema_v4")

old_df = spark.read.parquet("s3://bucket/backfill/events_v0/")

aligned_df = old_df \
    .withColumn("region", lit(None).cast("string")) \
    .withColumn("priority", lit(None).cast("int")) \
    .withColumn("source_system", lit("legacy"))

aligned_df.writeTo("catalog.db.events").append()
```

### Step 4: Validate on the Branch

Read from the branch to confirm data integrity:

```python
branch_df = spark.read \
    .option("branch", "schema_v4") \
    .table("catalog.db.events")

branch_df.printSchema()
branch_df.filter("priority IS NULL").count()  # Verify expected nulls
```

Meanwhile, production readers on `main` see the table as it was before — no disruption.

### Step 5: Fast-Forward Main

Once validated, promote the branch snapshot to `main`:

```sql
-- Spark SQL (Spark 3.4+ with Iceberg 1.4+)
ALTER TABLE catalog.db.events
EXECUTE fast-forward('main', 'schema_v4');
```

Or via the Iceberg Java/Python API:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("glue", **{"type": "glue"})
table = catalog.load_table("db.events")

# Fast-forward main to the branch snapshot
table.manage_snapshots().fast_forward("main", "schema_v4").commit()
```

### Step 6: Clean Up

```sql
ALTER TABLE catalog.db.events DROP BRANCH schema_v4;
```

### Branching + Schema Evolution Flow Diagram

```
main:       S0 ──── S0 (unchanged, production reads here)
                \
schema_v4:       S0 ── [ADD COLUMNS] ── S1 (backfill) ── S2 (validated)
                                                            │
main:       S0 ─────────────────────────────────────────── S2 (fast-forward)
```

### When to Prefer Branching Over Direct Evolution

| Scenario | Direct ALTER | Branch first |
|----------|:-----------:|:------------:|
| Adding a nullable column to a low-traffic table | ✅ | — |
| Multi-column change with large backfill | — | ✅ |
| Schema change that requires downstream validation | — | ✅ |
| Breaking change (drop + re-add with different type) | — | ✅ |
| Coordinated release across multiple teams | — | ✅ |

### Caveats

- **Schema is global** — DDL like `ADD COLUMNS` applies to the table metadata, not just the branch. The branch isolates *data snapshots*. Plan accordingly: add columns first, then branch for data backfill.
- **Conflict on fast-forward** — if `main` has advanced (new commits since the branch point), fast-forward will fail. You'll need to cherry-pick or rebase the branch.
- **Catalog support** — branching requires Iceberg 1.2+ and catalog support. AWS Glue catalog supports branches as of Iceberg 1.4 with Spark 3.4+.
- **Retention** — set `RETAIN` or `MAX_SNAPSHOT_AGE` on branches to avoid unbounded metadata growth.

---

## Scenario 6: Loading Old Data That Contains Dropped Columns

### The Problem

Your table has evolved and **dropped** columns that no longer serve the current schema. But you have historical data (backfill, late-arriving events, replayed archives) that still contains those dropped columns. Iceberg will reject writes that reference columns not in the current schema.

Example timeline:

```
v0: id, name, created_at, region, event_type, source_ip
v1: id, name, created_at, region, event_type              (dropped source_ip)
v2: id, name, created_at, region                           (dropped event_type)
```

You now need to load a Parquet file written at v0 that contains all six columns, but the table only accepts `id, name, created_at, region`.

### Key Insight

Iceberg **will not** let you write data for columns that don't exist in the current schema. The dropped column IDs are retired. You have two choices:

1. **Drop the extra columns** from the incoming data before writing (data loss for those columns).
2. **Re-add the columns** to the schema, load the data, then optionally drop them again.

---

### Approach A: Drop Extra Columns Before Insert (Simple, Lossy)

If the dropped columns are truly no longer needed, just select the current schema columns:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Old data has: id, name, created_at, region, event_type, source_ip
old_df = spark.read.parquet("s3://bucket/archive/events_v0/")

# Current table schema: id, name, created_at, region
current_columns = ["id", "name", "created_at", "region"]
trimmed_df = old_df.select(current_columns)

trimmed_df.writeTo("catalog.db.events").append()
```

The `event_type` and `source_ip` values are discarded. This is fine if those columns were dropped intentionally and the data is not needed.

---

### Approach B: Re-Add Columns, Load, Then Drop Again (Lossless)

If you need to **preserve** the old column data in the table (even if current consumers don't use it), temporarily re-add the columns:

```sql
-- Re-add the dropped columns
ALTER TABLE catalog.db.events ADD COLUMNS (
  event_type string,
  source_ip string
);
```

```python
# Now the table accepts all six columns
old_df = spark.read.parquet("s3://bucket/archive/events_v0/")
old_df.writeTo("catalog.db.events").append()
```

```sql
-- Optionally drop them again after the load
ALTER TABLE catalog.db.events DROP COLUMN event_type;
ALTER TABLE catalog.db.events DROP COLUMN source_ip;
```

**Important:** The re-added columns get **new** column IDs (Iceberg never reuses IDs). This means:

- The data you just wrote is stored with the new IDs.
- After dropping again, those values are hidden from reads — but still physically present in the data files.
- If you ever re-add the columns *again*, they'll get yet another new ID and won't see the previously written data.

This approach is useful when you want the data physically preserved for compliance/audit but hidden from day-to-day queries.

---

### Approach C: Use a Staging Table (Recommended for Large Backfills)

Avoid schema churn on the production table by using a staging table with the old schema:

```sql
-- Create a staging table with the full old schema
CREATE TABLE catalog.db.events_staging (
  id long,
  name string,
  created_at timestamp,
  region string,
  event_type string,
  source_ip string
) USING iceberg;
```

```python
# Load old data into staging
old_df = spark.read.parquet("s3://bucket/archive/events_v0/")
old_df.writeTo("catalog.db.events_staging").append()
```

```sql
-- Insert into production, selecting only current columns
INSERT INTO catalog.db.events (id, name, created_at, region)
SELECT id, name, created_at, region
FROM catalog.db.events_staging;
```

The staging table retains the full historical data if you ever need it, and the production table stays clean.

---

### Approach D: Branch + Temporary Schema Expansion

Combine branching with temporary schema re-addition for a safe, reversible workflow:

```sql
-- 1. Create a branch for the backfill work
ALTER TABLE catalog.db.events CREATE BRANCH backfill_v0 RETAIN 7 DAYS;

-- 2. Re-add dropped columns to the table schema
ALTER TABLE catalog.db.events ADD COLUMNS (
  event_type string,
  source_ip string
);
```

```python
# 3. Write old data to the branch
spark.conf.set("spark.wap.branch", "backfill_v0")

old_df = spark.read.parquet("s3://bucket/archive/events_v0/")
old_df.writeTo("catalog.db.events").append()
```

```sql
-- 4. Drop the columns again (hides them from reads going forward)
ALTER TABLE catalog.db.events DROP COLUMN event_type;
ALTER TABLE catalog.db.events DROP COLUMN source_ip;

-- 5. Validate on the branch
SELECT id, name, created_at, region FROM catalog.db.events VERSION AS OF 'backfill_v0';

-- 6. Fast-forward main
ALTER TABLE catalog.db.events EXECUTE fast-forward('main', 'backfill_v0');

-- 7. Clean up
ALTER TABLE catalog.db.events DROP BRANCH backfill_v0;
```

This way:
- Production readers on `main` are never disrupted.
- The old data is physically written (preserving all columns in the data files).
- After dropping the columns again, current-schema reads only project `id, name, created_at, region`.

---

### Decision Matrix: Which Approach to Use

| Situation | Approach |
|-----------|----------|
| Dropped columns are truly unneeded, small backfill | A (drop columns from data) |
| Must preserve all historical data for compliance | B (re-add, load, drop) or D (branch variant) |
| Large backfill, don't want schema churn on prod table | C (staging table) |
| Need zero-downtime + full preservation + validation | D (branch + temp schema) |
| Multiple teams reading the table during backfill | C or D |

---

## Best Practices

1. **Always make new columns nullable** (or provide defaults) so that old data can be inserted without errors.
2. **Never reuse column names** after dropping them — Iceberg prevents ID reuse, but using the same name with a different type can confuse consumers.
3. **Use explicit column lists** in INSERT statements to avoid positional mismatches.
4. **Validate schema alignment** before bulk writes — compare your DataFrame schema against the table's current schema and add/cast columns as needed.
5. **Prefer `writeTo().append()`** over `insertInto()` in Spark — `writeTo` uses column names (not positions) and handles schema evolution more gracefully.
6. **Track schema versions** in your pipeline metadata so you know which version each data batch conforms to.
7. **Test with time-travel** — use `SELECT * FROM table VERSION AS OF <snapshot>` to verify that old data reads correctly under the new schema.

---

## Glue Catalog Considerations (AWS)

When using AWS Glue as the Iceberg catalog:

- Schema changes via `ALTER TABLE` automatically update the Glue table metadata.
- If you update the schema outside of Iceberg (e.g., directly in Glue), the Iceberg metadata and Glue metadata can drift — always evolve schema through Iceberg APIs.
- Glue has a 400-column limit per table; plan schema evolution with this ceiling in mind.
